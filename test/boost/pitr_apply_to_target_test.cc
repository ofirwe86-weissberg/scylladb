/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for applying PITR's strict replay to a differently named target (issue #17): the schema conversion, the T
// filter, the shard the write lands on, and what the target cannot hold.
//
// Every case writes real commitlog segments for a real source table and reads them back with the real reader, so the
// entries, their mappings, the chunking and the splitting of an oversized write are the real thing. What landed is read
// back through CQL, which routes to the shard that owns the key, so a write applied to the wrong shard is invisible
// there and the test fails.

// Defines the test entry point; must come before anything that pulls in seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <algorithm>
#include <vector>

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>

#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/closeable.hh>

#include "cql3/result_set.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/pitr/apply_to_target.hh"
#include "db/pitr/replay_reader.hh"
#include "dht/i_partitioner.hh"
#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"
#include "keys/keys.hh"
#include "locator/abstract_replication_strategy.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "replica/database.hh"
#include "schema/schema.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "test/lib/tmpdir.hh"
#include "transport/messages/result_message.hh"
#include "utils/lister.hh"

using namespace db;
using namespace db::pitr;

namespace {

// The timestamp every case restores to. Fixed, so T, T - 1 and T + 1 are the same in every run.
constexpr api::timestamp_type T = 1700000000000000;

// A function, not a variable: FILENAME_PREFIX is defined in another translation unit and is still empty when this one's
// statics are built.
sstring prefix() {
    return db::commitlog::descriptor::FILENAME_PREFIX;
}

// A commitlog in `dir`, left on disk when `f` returns: release() keeps the segments so that the reader can be pointed at
// them afterwards. Its own metrics category, since the test environment already runs a commitlog of its own.
future<> with_log(const sstring& dir, noncopyable_function<future<>(commitlog&)> f, bool allow_fragmented_entries = false) {
    commitlog::config cfg;
    cfg.allow_fragmented_entries = allow_fragmented_entries;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.commitlog_total_space_in_mb = 8 * this_smp_shard_count();
    cfg.allow_going_over_size_limit = true;
    cfg.commit_log_location = dir;
    cfg.metrics_category_name = "pitr_apply_to_target_test";
    auto log = co_await commitlog::create_commitlog(std::move(cfg));
    std::exception_ptr ep;
    try {
        co_await f(log);
        co_await log.sync_all_segments();
    } catch (...) {
        ep = std::current_exception();
    }
    co_await log.release();
    co_await log.shutdown();
    if (ep) {
        std::rethrow_exception(ep);
    }
}

future<> write_mutation(commitlog& log, schema_ptr s, const mutation& m) {
    auto fm = freeze(m);
    commitlog_mutation_entry_writer w(s, fm, commitlog::force_sync::no);
    auto h = co_await log.add_entry(s->id(), w, db::no_timeout);
    // Releasing the handle keeps the entry: a handle that goes out of scope tells the commitlog the data is no longer
    // needed, and the segment is then deleted instead of shipped.
    h.release();
}

// A small entry and an oversized one in one batch: the commitlog refuses a single record over its limit, but splits an
// oversized one in a batch across chunks, which is the split write the reader reassembles.
future<> write_small_and_split_batch(commitlog& log, schema_ptr s, const mutation& small, const mutation& big) {
    auto small_fm = freeze(small);
    auto big_fm = freeze(big);
    utils::chunked_vector<commitlog_mutation_entry_writer> writers;
    writers.emplace_back(s, small_fm, commitlog::force_sync::no);
    writers.emplace_back(s, big_fm, commitlog::force_sync::no);
    auto handles = co_await log.add_entries(std::move(writers), db::no_timeout);
    for (auto& h : handles) {
        h.release();
    }
}

future<std::vector<sstring>> segment_names(const sstring& dir) {
    std::vector<sstring> names;
    co_await lister::scan_dir(std::filesystem::path(dir), lister::dir_entry_types::of<directory_entry_type::regular>(),
            [&](std::filesystem::path parent, directory_entry de) -> future<> {
                if (sstring(de.name).find(prefix()) == 0) {
                    names.push_back(sstring((parent / de.name).string()));
                }
                return make_ready_future<>();
            });
    co_return names;
}

// Stands in for the chain check the reader's caller will run: every one of this shard's segments in `dir`, by ascending
// id, each with the size it has on disk, which is the size a shipped segment arrives with.
future<utils::chunked_vector<shipped_segment>> verified_list(const sstring& dir) {
    std::vector<std::pair<segment_id_type, sstring>> found;
    for (auto& path : co_await segment_names(dir)) {
        const commitlog::descriptor d(std::string(path), prefix());
        if (replay_position(d).shard_id() == this_shard_id()) {
            found.emplace_back(d.id, path);
        }
    }
    std::ranges::sort(found);
    utils::chunked_vector<shipped_segment> segments;
    for (auto& [id, path] : found) {
        segments.push_back(shipped_segment{.path = path, .used_size = co_await file_size(path)});
    }
    co_return segments;
}

struct run_result {
    replay_reader_stats reader;
    apply_stats applier;
};

// Replays `dir` into table `target`, as the restore will: the reader's entries go straight to a target_applier.
run_result replay_into(cql_test_env& e, const sstring& dir, const sstring& source, const sstring& target, api::timestamp_type t = T) {
    const auto source_id = e.local_db().find_schema("ks", source)->id();
    const auto target_id = e.local_db().find_schema("ks", target)->id();
    sharded<shard_mapping_cache> mappings;
    mappings.start().get();
    auto stop_mappings = deferred_stop(mappings);
    target_applier applier(e.db(), mappings, target_id, t);
    const shard_replay_params params{
            .segments = verified_list(dir).get(),
            .filename_prefix = prefix(),
            .shard = this_shard_id(),
            .source_table = source_id,
            .bound = {},
            .truncate_floor = {},
    };
    const auto reader_stats = replay_shard_segments(params, [&applier](const replay_entry& entry) {
        return applier(entry);
    }).get();
    return run_result{.reader = reader_stats, .applier = applier.stats()};
}

// The answer the test compares against is built from CQL, not from the mutations the test wrote: one line per row, cells
// rendered by their target type, sorted, so a failure prints both lists.
std::vector<sstring> select_lines(cql_test_env& e, const sstring& cql) {
    auto msg = e.execute_cql(cql).get();
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    BOOST_REQUIRE(rows);
    const auto& rs = rows->rs().result_set();
    std::vector<sstring> lines;
    for (const auto& row : rs.rows()) {
        std::vector<sstring> cells;
        for (size_t i = 0; i < row.size(); ++i) {
            const auto& type = rs.get_metadata().get_names()[i]->type;
            cells.push_back(row[i] ? type->to_string(to_bytes(*row[i])) : sstring("null"));
        }
        lines.push_back(fmt::format("{}", fmt::join(cells, "|")));
    }
    std::ranges::sort(lines);
    return lines;
}

void create_tables(cql_test_env& e) {
    e.execute_cql("CREATE TABLE src (pk text, ck1 text, ck2 text, v text, s text static, m map<text, text>, PRIMARY KEY (pk, ck1, ck2))").get();
    // The target the restore would create from the backup's schema: every column of every replayed version, under
    // another name.
    e.execute_cql("CREATE TABLE tgt (pk text, ck1 text, ck2 text, v text, w text, s text static, m map<text, text>, PRIMARY KEY (pk, ck1, ck2))").get();
}

schema_ptr src_schema(cql_test_env& e) {
    return e.local_db().find_schema("ks", "src");
}

mutation new_mutation(schema_ptr s, const sstring& pk) {
    return mutation(s, partition_key::from_single_value(*s, to_bytes(pk)));
}

clustering_key ck(const schema& s, const sstring& ck1, const sstring& ck2) {
    return clustering_key::from_exploded(s, {to_bytes(ck1), to_bytes(ck2)});
}

// An INSERT: a row marker and a cell, both at `ts`, as CQL writes them.
mutation insert_row(schema_ptr s, const sstring& pk, const sstring& ck1, const sstring& ck2, const sstring& v, api::timestamp_type ts) {
    auto m = new_mutation(s, pk);
    auto key = ck(*s, ck1, ck2);
    m.partition().clustered_row(*s, key).apply(row_marker(ts));
    m.set_clustered_cell(key, to_bytes("v"), data_value(v), ts);
    return m;
}

// `count` keys the target owns on one and the same shard, so entries that must share a shard's mapping cache do.
std::vector<sstring> keys_on_one_shard(cql_test_env& e, size_t count) {
    auto& tgt = e.local_db().find_column_family("ks", "tgt");
    auto erm = tgt.get_effective_replication_map();
    auto s = tgt.schema();
    std::vector<std::vector<sstring>> per_shard(this_smp_shard_count());
    for (int i = 0; i < 10000; ++i) {
        const auto key = fmt::format("k{}", i);
        const auto token = dht::get_token(*s, partition_key::from_single_value(*s, to_bytes(key)));
        const auto shards = erm->shard_for_writes(*s, token);
        BOOST_REQUIRE_EQUAL(shards.size(), 1u);
        auto& keys = per_shard[shards[0]];
        keys.push_back(key);
        if (keys.size() == count) {
            return keys;
        }
    }
    BOOST_FAIL("no shard got enough keys");
    return {};
}

} // namespace

SEASTAR_TEST_CASE(test_insert_at_or_below_t_lands_and_above_t_does_not) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto s = src_schema(e);
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            co_await write_mutation(log, s, insert_row(s, "below", "c1", "c2", "old", T - 1));
            co_await write_mutation(log, s, insert_row(s, "at", "c1", "c2", "exactly", T));
            co_await write_mutation(log, s, insert_row(s, "above", "c1", "c2", "future", T + 1));
        }).get();

        const auto r = replay_into(e, dir.path().native(), "src", "tgt");

        BOOST_REQUIRE_EQUAL(r.reader.entries_replayed, 3u);
        BOOST_REQUIRE_EQUAL(r.applier.entries_applied, 2u);
        BOOST_REQUIRE_EQUAL(r.applier.entries_empty_after_filter, 1u);
        const std::vector<sstring> expected{"at|c1|c2|exactly", "below|c1|c2|old"};
        BOOST_REQUIRE_EQUAL(
                fmt::format("{}", fmt::join(select_lines(e, "SELECT pk, ck1, ck2, v FROM tgt"), " / ")), fmt::format("{}", fmt::join(expected, " / ")));
    });
}

SEASTAR_TEST_CASE(test_update_of_a_cell_lands_without_a_row_marker) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto s = src_schema(e);
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            auto m = new_mutation(s, "p");
            m.set_clustered_cell(ck(*s, "c1", "c2"), to_bytes("v"), data_value(sstring("updated")), T);
            co_await write_mutation(log, s, m);
        }).get();

        const auto r = replay_into(e, dir.path().native(), "src", "tgt");

        BOOST_REQUIRE_EQUAL(r.applier.entries_applied, 1u);
        BOOST_REQUIRE_EQUAL(r.applier.cells_applied, 1u);
        BOOST_REQUIRE_EQUAL(select_lines(e, "SELECT v FROM tgt").size(), 1u);
        BOOST_REQUIRE_EQUAL(select_lines(e, "SELECT v FROM tgt")[0], "updated");
    });
}

SEASTAR_TEST_CASE(test_row_delete_at_t_lands) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto s = src_schema(e);
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            co_await write_mutation(log, s, insert_row(s, "p", "c1", "c2", "v", T - 2));
            auto del = new_mutation(s, "p");
            del.partition().apply_delete(*s, ck(*s, "c1", "c2"), tombstone(T, gc_clock::now()));
            co_await write_mutation(log, s, del);
        }).get();

        replay_into(e, dir.path().native(), "src", "tgt");

        BOOST_REQUIRE(select_lines(e, "SELECT pk FROM tgt").empty());
    });
}

SEASTAR_TEST_CASE(test_range_delete_at_t_lands) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto s = src_schema(e);
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            co_await write_mutation(log, s, insert_row(s, "p", "gone", "c2", "v", T - 2));
            co_await write_mutation(log, s, insert_row(s, "p", "kept", "c2", "v", T - 2));
            // A prefix of the clustering key: one range tombstone over every row under "gone".
            auto del = new_mutation(s, "p");
            del.partition().apply_delete(*s, clustering_key_prefix::from_exploded(*s, {to_bytes("gone")}), tombstone(T, gc_clock::now()));
            co_await write_mutation(log, s, del);
        }).get();

        replay_into(e, dir.path().native(), "src", "tgt");

        const auto lines = select_lines(e, "SELECT ck1 FROM tgt");
        BOOST_REQUIRE_EQUAL(lines.size(), 1u);
        BOOST_REQUIRE_EQUAL(lines[0], "kept");
    });
}

SEASTAR_TEST_CASE(test_partition_delete_at_t_lands) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto s = src_schema(e);
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            co_await write_mutation(log, s, insert_row(s, "p", "c1", "c2", "v", T - 2));
            auto del = new_mutation(s, "p");
            del.partition().apply(tombstone(T, gc_clock::now()));
            co_await write_mutation(log, s, del);
        }).get();

        replay_into(e, dir.path().native(), "src", "tgt");

        BOOST_REQUIRE(select_lines(e, "SELECT pk FROM tgt").empty());
    });
}

SEASTAR_TEST_CASE(test_a_partition_delete_above_t_does_not_land) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto s = src_schema(e);
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            co_await write_mutation(log, s, insert_row(s, "p", "c1", "c2", "survives", T - 2));
            auto del = new_mutation(s, "p");
            del.partition().apply(tombstone(T + 1, gc_clock::now()));
            co_await write_mutation(log, s, del);
        }).get();

        const auto r = replay_into(e, dir.path().native(), "src", "tgt");

        BOOST_REQUIRE_EQUAL(r.applier.entries_empty_after_filter, 1u);
        const auto lines = select_lines(e, "SELECT v FROM tgt");
        BOOST_REQUIRE_EQUAL(lines.size(), 1u);
        BOOST_REQUIRE_EQUAL(lines[0], "survives");
    });
}

SEASTAR_TEST_CASE(test_static_column_and_collection_land) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto s = src_schema(e);
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            auto m = new_mutation(s, "p");
            m.set_static_cell(to_bytes("s"), data_value(sstring("static value")), T);
            const auto& mdef = *s->get_column_definition(to_bytes("m"));
            collection_mutation_writer w({});
            w.push_back(bytes_view(to_bytes("k1")), atomic_cell_view(atomic_cell::make_live(*utf8_type, T, to_bytes("v1"))));
            // One member above T, which the filter must drop while the rest lands.
            w.push_back(bytes_view(to_bytes("k2")), atomic_cell_view(atomic_cell::make_live(*utf8_type, T + 1, to_bytes("v2"))));
            m.set_clustered_cell(ck(*s, "c1", "c2"), mdef, atomic_cell_or_collection(std::move(w).finish()));
            m.partition().clustered_row(*s, ck(*s, "c1", "c2")).apply(row_marker(T));
            co_await write_mutation(log, s, m);
        }).get();

        replay_into(e, dir.path().native(), "src", "tgt");

        const auto lines = select_lines(e, "SELECT s, m FROM tgt");
        BOOST_REQUIRE_EQUAL(lines.size(), 1u);
        BOOST_REQUIRE_EQUAL(lines[0], "static value|{k1 : v1}");
    });
}

SEASTAR_TEST_CASE(test_two_schema_versions_both_convert_to_the_target) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto v1 = src_schema(e);
        tmpdir dir;
        e.execute_cql("ALTER TABLE src ADD w text").get();
        auto v2 = src_schema(e);
        BOOST_REQUIRE(v1->version() != v2->version());
        // Both keys on one shard, so one shard's mapping cache holds both versions: a cache that keyed them together
        // would convert the second entry with the first version's mapping.
        const auto keys = keys_on_one_shard(e, 2);
        testlog.info("both versions on one shard, keys {}", keys);
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            co_await write_mutation(log, v1, insert_row(v1, keys[0], "c1", "c2", "from v1", T));
            auto m = insert_row(v2, keys[1], "c1", "c2", "from v2", T);
            m.set_clustered_cell(ck(*v2, "c1", "c2"), to_bytes("w"), data_value(sstring("only in v2")), T);
            co_await write_mutation(log, v2, m);
        }).get();

        const auto r = replay_into(e, dir.path().native(), "src", "tgt");

        BOOST_REQUIRE_EQUAL(r.applier.entries_applied, 2u);
        std::vector<sstring> expected{fmt::format("{}|from v1|null", keys[0]), fmt::format("{}|from v2|only in v2", keys[1])};
        std::ranges::sort(expected);
        BOOST_REQUIRE_EQUAL(fmt::format("{}", fmt::join(select_lines(e, "SELECT pk, v, w FROM tgt"), " / ")), fmt::format("{}", fmt::join(expected, " / ")));
    });
}

SEASTAR_TEST_CASE(test_a_split_write_lands_whole) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto s = src_schema(e);
        tmpdir dir;
        size_t big_size = 0;
        with_log(
                dir.path().native(),
                [&](commitlog& log) -> future<> {
                    big_size = 2 * log.max_record_size();
                    co_await write_small_and_split_batch(
                            log, s, insert_row(s, "small", "c1", "c2", "small", T), insert_row(s, "big", "c1", "c2", sstring(big_size, 'x'), T));
                },
                true)
                .get();

        const auto r = replay_into(e, dir.path().native(), "src", "tgt");

        BOOST_REQUIRE_EQUAL(r.applier.entries_applied, 2u);
        auto msg = e.execute_cql("SELECT v FROM tgt WHERE pk = 'big' AND ck1 = 'c1' AND ck2 = 'c2'").get();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE_EQUAL(rows->rs().result_set().rows().size(), 1u);
        BOOST_REQUIRE_EQUAL(rows->rs().result_set().rows()[0][0]->size(), big_size);
    });
}

SEASTAR_TEST_CASE(test_entries_of_another_table_never_reach_the_target) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        e.execute_cql("CREATE TABLE other (pk text, ck1 text, ck2 text, v text, PRIMARY KEY (pk, ck1, ck2))").get();
        auto s = src_schema(e);
        auto other = e.local_db().find_schema("ks", "other");
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            co_await write_mutation(log, other, insert_row(other, "p", "c1", "c2", "other table", T));
            co_await write_mutation(log, s, insert_row(s, "p", "c1", "c2", "source table", T));
            co_await write_mutation(log, other, insert_row(other, "q", "c1", "c2", "other table", T));
        }).get();

        const auto r = replay_into(e, dir.path().native(), "src", "tgt");

        BOOST_REQUIRE_EQUAL(r.reader.dropped_other_table, 2u);
        BOOST_REQUIRE_EQUAL(r.applier.entries_applied, 1u);
        const auto lines = select_lines(e, "SELECT v FROM tgt");
        BOOST_REQUIRE_EQUAL(lines.size(), 1u);
        BOOST_REQUIRE_EQUAL(lines[0], "source table");
        // The other table itself is untouched: the replay writes only to the target.
        BOOST_REQUIRE(select_lines(e, "SELECT pk FROM other").empty());
    });
}

SEASTAR_TEST_CASE(test_a_column_the_target_lacks_is_refused_and_stops_the_read) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        // A target as the restore would not create it: one column short of the source's schema.
        e.execute_cql("CREATE TABLE narrow (pk text, ck1 text, ck2 text, s text static, PRIMARY KEY (pk, ck1, ck2))").get();
        auto s = src_schema(e);
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            co_await write_mutation(log, s, insert_row(s, "first", "c1", "c2", "has v", T));
            co_await write_mutation(log, s, insert_row(s, "second", "c1", "c2", "has v", T));
        }).get();

        BOOST_REQUIRE_THROW(replay_into(e, dir.path().native(), "src", "narrow"), target_schema_mismatch);
        // Nothing of the entry that was refused, and nothing after it: the read stopped.
        BOOST_REQUIRE(select_lines(e, "SELECT pk FROM narrow").empty());
    });
}

SEASTAR_TEST_CASE(test_an_incompatible_column_type_is_refused) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        e.execute_cql("CREATE TABLE wrong_type (pk text, ck1 text, ck2 text, v int, PRIMARY KEY (pk, ck1, ck2))").get();
        auto s = src_schema(e);
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            co_await write_mutation(log, s, insert_row(s, "p", "c1", "c2", "text into int", T));
        }).get();

        BOOST_REQUIRE_THROW(replay_into(e, dir.path().native(), "src", "wrong_type"), target_schema_mismatch);
    });
}

SEASTAR_TEST_CASE(test_stats_count_cells_applied_dropped_and_not_verified) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto s = src_schema(e);
        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            // Kept: a row marker and a cell. Dropped: the same two, one microsecond above T.
            co_await write_mutation(log, s, insert_row(s, "kept", "c1", "c2", "v", T));
            co_await write_mutation(log, s, insert_row(s, "dropped", "c1", "c2", "v", T + 1));
            // A collection the filter cannot resolve: its tombstone sits exactly at T.
            auto m = new_mutation(s, "ambiguous");
            const auto& mdef = *s->get_column_definition(to_bytes("m"));
            collection_mutation_writer w(tombstone(T, gc_clock::now()));
            w.push_back(bytes_view(to_bytes("k1")), atomic_cell_view(atomic_cell::make_live(*utf8_type, T + 1, to_bytes("v1"))));
            m.set_clustered_cell(ck(*s, "c1", "c2"), mdef, atomic_cell_or_collection(std::move(w).finish()));
            co_await write_mutation(log, s, m);
        }).get();

        const auto r = replay_into(e, dir.path().native(), "src", "tgt");

        testlog.info("stats: applied={} empty={} cells_applied={} cells_dropped={} not_verified={}", r.applier.entries_applied,
                r.applier.entries_empty_after_filter, r.applier.cells_applied, r.applier.cells_dropped_after_t, r.applier.cells_not_verified);
        // "kept" lands with its row marker and its cell. "dropped" loses both, and the collection whose tombstone sits at
        // T goes with the member one microsecond above it, so both of those entries end up empty.
        BOOST_REQUIRE_EQUAL(r.applier.entries_applied, 1u);
        BOOST_REQUIRE_EQUAL(r.applier.entries_empty_after_filter, 2u);
        BOOST_REQUIRE_EQUAL(r.applier.cells_applied, 2u);
        BOOST_REQUIRE_EQUAL(r.applier.cells_dropped_after_t, 4u);
        BOOST_REQUIRE_EQUAL(r.applier.cells_not_verified, 1u);
    });
}

SEASTAR_TEST_CASE(test_keys_land_on_the_shard_that_owns_them) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_tables(e);
        auto s = src_schema(e);
        auto& tgt = e.local_db().find_column_family("ks", "tgt");
        auto erm = tgt.get_effective_replication_map();
        auto tgt_schema = tgt.schema();

        // One key per shard, found through the target's own sharder, so the test knows where each must land without
        // repeating how the applier gets there.
        std::vector<sstring> keys(this_smp_shard_count());
        size_t found = 0;
        for (int i = 0; found < this_smp_shard_count() && i < 10000; ++i) {
            const auto key = fmt::format("k{}", i);
            const auto token = dht::get_token(*tgt_schema, partition_key::from_single_value(*tgt_schema, to_bytes(key)));
            const auto shards = erm->shard_for_writes(*tgt_schema, token);
            BOOST_REQUIRE_EQUAL(shards.size(), 1u);
            if (keys[shards[0]].empty()) {
                keys[shards[0]] = key;
                ++found;
            }
        }
        BOOST_REQUIRE_EQUAL(found, this_smp_shard_count());
        testlog.info("one key per shard: {}", keys);

        tmpdir dir;
        with_log(dir.path().native(), [&](commitlog& log) -> future<> {
            for (const auto& key : keys) {
                co_await write_mutation(log, s, insert_row(s, key, "c1", "c2", key, T));
            }
        }).get();

        const auto r = replay_into(e, dir.path().native(), "src", "tgt");

        BOOST_REQUIRE_EQUAL(r.applier.entries_applied, this_smp_shard_count());
        // A read goes to the shard that owns the key, so a row applied on any other shard is missing here.
        auto expected = keys;
        std::ranges::sort(expected);
        BOOST_REQUIRE_EQUAL(fmt::format("{}", fmt::join(select_lines(e, "SELECT pk FROM tgt"), " / ")), fmt::format("{}", fmt::join(expected, " / ")));
    });
}
