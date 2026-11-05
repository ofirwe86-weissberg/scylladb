/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the PITR strict replay reader (issue #17): the segment reader and
// its per-entry filters.
//
// Every case here writes its segments with a real commitlog and reads them back
// with a fake consumer, so the entries, their positions, the chunking and the
// splitting of an oversized write are the real thing. Nothing below applies a
// mutation anywhere.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <algorithm>
#include <random>
#include <vector>

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>

#include <seastar/core/align.hh>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/pitr/replay_reader.hh"
#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"
#include "keys/keys.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "raft/raft.hh"
#include "schema/schema_builder.hh"
#include "test/lib/log.hh"
#include "test/lib/tmpdir.hh"
#include "utils/UUID_gen.hh"
#include "utils/lister.hh"

using namespace db;
using namespace db::pitr;

namespace {

// A function, not a variable: FILENAME_PREFIX is defined in another
// translation unit and is still empty when this one's statics are built.
static sstring prefix() {
    return db::commitlog::descriptor::FILENAME_PREFIX;
}

static schema_ptr make_schema(const sstring& cf, std::optional<table_id> id = {}) {
    return schema_builder(this_smp_shard_count(), "ks", cf, id).with_column("pk", utf8_type, column_kind::partition_key).with_column("v", bytes_type).build();
}

static mutation make_mutation(schema_ptr s, const sstring& pk, size_t value_size) {
    mutation m(s, partition_key::from_single_value(*s, to_bytes(pk)));
    m.set_clustered_cell(clustering_key::make_empty(), to_bytes("v"), data_value(bytes(value_size, 'x')), api::new_timestamp());
    return m;
}

// A commitlog in `dir`, left on disk when `f` returns: release() keeps the
// segments so that the reader can be pointed at them afterwards.
static future<> with_log(const sstring& dir, commitlog::config cfg, noncopyable_function<future<>(commitlog&)> f) {
    cfg.commit_log_location = dir;
    cfg.metrics_category_name = "commitlog";
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

static commitlog::config small_segments() {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.commitlog_total_space_in_mb = 8 * this_smp_shard_count();
    cfg.allow_going_over_size_limit = true;
    return cfg;
}

static future<replay_position> write_mutation(commitlog& log, schema_ptr s, const mutation& m) {
    auto fm = freeze(m);
    commitlog_mutation_entry_writer w(s, fm, commitlog::force_sync::no);
    auto h = co_await log.add_entry(s->id(), w, db::no_timeout);
    co_return h.release();
}

static future<replay_position> write_raft_batch(commitlog& log, const table_id& tid, raft::group_id gid) {
    const std::vector<raft::log_entry_ptr> entries{
            make_lw_shared<raft::log_entry>(raft::log_entry{.term = raft::term_t(1), .idx = raft::index_t(1), .data = raft::log_entry::dummy{}})};
    commitlog_raft_batch_writer sizer(gid, raft::index_t{0}, entries);
    auto h = co_await log.add(tid, sizer.size(), db::no_timeout, commitlog::force_sync::no, [&](auto& out) {
        commitlog_raft_batch_writer w(gid, raft::index_t{0}, entries);
        w.write(out);
    });
    co_return h.release();
}

// What the reader handed over, in the order it did.
struct capture {
    std::vector<replay_position> positions;
    std::vector<table_id> tables;
    std::vector<size_t> sizes;
    size_t fail_at = std::numeric_limits<size_t>::max();

    replay_consumer consumer() {
        return [this](const replay_entry& e) -> future<> {
            if (positions.size() == fail_at) {
                throw std::runtime_error("consumer failed on purpose");
            }
            positions.push_back(e.position);
            tables.push_back(e.mutation.column_family_id());
            sizes.push_back(e.mutation.representation().size());
            return make_ready_future<>();
        };
    }
};

// Boost cannot print a vector of positions, so compare the rendered lists: a
// failure then shows both of them.
static sstring to_str(const std::vector<replay_position>& v) {
    return fmt::format("[{}]", fmt::join(v, ", "));
}

static shard_replay_params params_for(const sstring& dir, const table_id& table, replay_position bound = {}, replay_position floor = {}) {
    return shard_replay_params{
            .directory = dir,
            .filename_prefix = prefix(),
            .shard = this_shard_id(),
            .source_table = table,
            .bound = bound,
            .truncate_floor = floor,
    };
}

// Flips four bytes at `off`, as commitlog_test's own corruption cases do.
static future<> corrupt_segment(const sstring& seg, uint64_t off, uint32_t value) {
    auto f = co_await open_file_dma(seg, open_flags::rw);
    std::exception_ptr ep;
    try {
        const size_t size = align_up<size_t>(off + sizeof(value), 4096);
        auto buf = co_await f.dma_read_exactly<char>(0, size);
        std::copy_n(reinterpret_cast<const char*>(&value), sizeof(value), buf.get_write() + off);
        co_await f.dma_write(0, buf.get(), buf.size());
    } catch (...) {
        ep = std::current_exception();
    }
    co_await f.close();
    if (ep) {
        std::rethrow_exception(ep);
    }
}

static future<std::vector<sstring>> segment_names(const sstring& dir) {
    std::vector<sstring> names;
    co_await lister::scan_dir(std::filesystem::path(dir), lister::dir_entry_types::of<directory_entry_type::regular>(),
            [&](std::filesystem::path parent, directory_entry de) -> future<> {
                if (sstring(de.name).find(prefix()) == 0) {
                    names.push_back(sstring((parent / de.name).string()));
                }
                return make_ready_future<>();
            });
    std::ranges::sort(names);
    co_return names;
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE(pitr_replay_reader_test)

// The bound is inclusive: the entry sitting exactly on it replays, the one
// below it does not, and the one above it does.
SEASTAR_TEST_CASE(test_entry_exactly_at_the_bound_is_replayed) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });
    BOOST_REQUIRE_EQUAL(rps.size(), 3u);

    capture c;
    auto stats = co_await replay_shard_segments(params_for(dir, s->id(), rps[1]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[1], rps[2]}));
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, 2u);
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 1u);
    BOOST_REQUIRE_EQUAL(stats.entries_read, 3u);
}

// A bound that falls between two entries of the same chunk must drop only the
// entries below it. Passing it to the reader as a start offset instead would
// select whole chunks, and all twenty entries here share one chunk.
SEASTAR_TEST_CASE(test_bound_inside_a_chunk_drops_only_the_entries_below_it) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 20; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });
    // One chunk: nothing synced in between, and the entries are tiny.
    BOOST_REQUIRE_EQUAL(rps.front().id, rps.back().id);

    // Between entry 9 and entry 10, and above the chunk's own start.
    auto bound = replay_position(rps[10].id, rps[10].pos - 1);
    BOOST_REQUIRE_GT(bound.pos, rps[0].pos);

    capture c;
    auto stats = co_await replay_shard_segments(params_for(dir, s->id(), bound), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>(rps.begin() + 10, rps.end())));
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 10u);
}

// An oversized write is split across segments. One reassembly state shared by
// the shard's segments must put it back together and deliver it once, whole, at
// the position its first fragment took.
SEASTAR_TEST_CASE(test_write_split_across_segments_arrives_whole) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position rp;
    size_t frozen_size = 0;

    auto cfg = small_segments();
    cfg.allow_fragmented_entries = true;
    cfg.allow_going_over_size_limit = true;

    co_await with_log(dir, cfg, [&](commitlog& log) -> future<> {
        // Twice the largest entry a segment can hold, so the write cannot land
        // in one segment however the commitlog packs it.
        auto m = make_mutation(s, "big", 2 * log.max_record_size());
        frozen_size = freeze(m).representation().size();
        BOOST_REQUIRE_GT(frozen_size, log.max_record_size());
        rp = co_await write_mutation(log, s, m);
    });

    auto segments = co_await segment_names(dir);
    BOOST_REQUIRE_GT(segments.size(), 1u);

    capture c;
    // The bound sits exactly on the split write, which must still survive.
    auto stats = co_await replay_shard_segments(params_for(dir, s->id(), rp), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rp}));
    BOOST_REQUIRE_EQUAL(c.sizes.at(0), frozen_size);
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, 1u);
}

// System tables share the commitlog, so everything but the source table is
// dropped on the table id alone.
SEASTAR_TEST_CASE(test_only_the_source_table_is_replayed) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto src = make_schema("src");
    auto other = make_schema("other");
    std::vector<replay_position> src_rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 4; ++i) {
            src_rps.push_back(co_await write_mutation(log, src, make_mutation(src, format("pk{}", i), 16)));
            co_await write_mutation(log, other, make_mutation(other, format("pk{}", i), 16));
        }
    });

    capture c;
    auto stats = co_await replay_shard_segments(params_for(dir, src->id()), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(src_rps));
    BOOST_REQUIRE(std::ranges::all_of(c.tables, [&](const table_id& t) {
        return t == src->id();
    }));
    BOOST_REQUIRE_EQUAL(stats.dropped_other_table, 4u);
}

// A raft batch carries no table data to restore, and is counted and skipped
// rather than mistaken for a mutation.
SEASTAR_TEST_CASE(test_raft_batches_are_skipped) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    auto gid = raft::group_id{utils::UUID_gen::get_time_UUID()};
    std::vector<replay_position> rps;

    auto cfg = small_segments();
    cfg.descriptor_tag = "variant";

    co_await with_log(dir, cfg, [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            co_await write_raft_batch(log, s->id(), gid);
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });

    capture c;
    auto stats = co_await replay_shard_segments(params_for(dir, s->id()), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(rps));
    BOOST_REQUIRE_EQUAL(stats.raft_batches_skipped, 3u);
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, 3u);
}

// A TRUNCATE above the bound raises the floor: what the source truncated away
// must not come back. The floor is exclusive, so the entry at it is dropped
// where the entry at the bound is kept.
SEASTAR_TEST_CASE(test_truncate_floor_above_the_bound_drops_up_to_the_floor) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 5; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });

    capture c;
    auto stats = co_await replay_shard_segments(params_for(dir, s->id(), rps[1], rps[3]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[4]}));
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 1u);
    BOOST_REQUIRE_EQUAL(stats.dropped_at_or_below_truncate_floor, 3u);
}

// A TRUNCATE below the bound is already covered by the snapshot and changes
// nothing.
SEASTAR_TEST_CASE(test_truncate_floor_below_the_bound_drops_nothing_extra) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 5; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });

    capture c;
    auto stats = co_await replay_shard_segments(params_for(dir, s->id(), rps[3], rps[1]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[3], rps[4]}));
    BOOST_REQUIRE_EQUAL(stats.dropped_at_or_below_truncate_floor, 0u);
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 3u);
}

// Segments of another shard sit in the same directory and belong to another
// reader, with its own bound, floor and reassembly state.
SEASTAR_TEST_CASE(test_segments_of_another_shard_are_not_read) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16));
        }
    });

    auto params = params_for(dir, s->id());
    params.shard = this_shard_id() + 1;

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.segments_read, 0u);
    BOOST_REQUIRE_EQUAL(stats.entries_read, 0u);
}

// Two shards' segments in one directory, each read with its own floor. A floor
// is one shard's truncation position and says nothing about the other's, so
// neither reader may drop the other's entries.
SEASTAR_TEST_CASE(test_truncate_floor_applies_per_shard) {
    if (this_smp_shard_count() < 2) {
        testlog.warn("test_truncate_floor_applies_per_shard needs at least two shards; skipped");
        co_return;
    }
    tmpdir tmp;
    tmpdir tmp_other;
    auto dir = sstring(tmp.path().string());
    auto other_dir = sstring(tmp_other.path().string());
    const auto tid = table_id(utils::UUID_gen::get_time_UUID());
    auto s = make_schema("cf", tid);
    std::vector<replay_position> local_rps;
    std::vector<replay_position> remote_rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 4; ++i) {
            local_rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });
    // The other shard's segments, written by its own commitlog so that they
    // carry its shard id, then moved in beside this shard's.
    co_await smp::submit_to(this_shard_id() + 1, [&]() -> future<> {
        auto remote_s = make_schema("cf", tid);
        co_await with_log(other_dir, small_segments(), [&](commitlog& log) -> future<> {
            for (int i = 0; i < 4; ++i) {
                remote_rps.push_back(co_await write_mutation(log, remote_s, make_mutation(remote_s, format("pk{}", i), 16)));
            }
        });
    });
    for (auto& seg : co_await segment_names(other_dir)) {
        co_await commitlog::rename_segment(seg, dir + "/" + sstring(std::filesystem::path(seg).filename().string()));
    }
    BOOST_REQUIRE_NE(local_rps.front().shard_id(), remote_rps.front().shard_id());

    // This shard truncated at its third entry; the other shard did not truncate.
    capture local;
    auto local_stats = co_await replay_shard_segments(params_for(dir, tid, {}, local_rps[2]), local.consumer());
    BOOST_REQUIRE_EQUAL(to_str(local.positions), to_str(std::vector<replay_position>{local_rps[3]}));
    BOOST_REQUIRE_EQUAL(local_stats.dropped_at_or_below_truncate_floor, 3u);

    auto other_params = params_for(dir, tid);
    other_params.shard = remote_rps.front().shard_id();
    capture remote;
    auto remote_stats = co_await replay_shard_segments(other_params, remote.consumer());
    BOOST_REQUIRE_EQUAL(to_str(remote.positions), to_str(remote_rps));
    BOOST_REQUIRE_EQUAL(remote_stats.dropped_at_or_below_truncate_floor, 0u);
}

// A segment holding nothing but its header is read without complaint.
SEASTAR_TEST_CASE(test_an_empty_segment_contributes_no_entries) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;
    capture c;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        rps.push_back(co_await write_mutation(log, s, make_mutation(s, "pk", 16)));
        co_await log.sync_all_segments();
        // Leaves the written segment behind and starts an empty one. The read
        // happens here, while the log still owns the empty segment: an unused
        // segment does not survive its shutdown.
        co_await log.force_new_active_segment();
        // The written one, the new active one, and whatever the commitlog keeps
        // in reserve: every one of them must be read, and only one has entries.
        const auto count = (co_await segment_names(dir)).size();
        BOOST_REQUIRE_GE(count, 2u);

        auto stats = co_await replay_shard_segments(params_for(dir, s->id()), c.consumer());
        BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(rps));
        BOOST_REQUIRE_EQUAL(stats.segments_read, count);
    });
}

// Nothing to read is not an error: a shard that took no writes in the window
// ships no segments.
SEASTAR_TEST_CASE(test_no_segments_yields_no_entries) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    // Neither of these is a segment: one does not carry the prefix, the other
    // carries it but is not a descriptor. Both are ignored, not parsed.
    for (auto name : {"/manifest.json", "/CommitLog-not-a-descriptor.log"}) {
        auto f = co_await open_file_dma(dir + name, open_flags::create | open_flags::wo);
        co_await f.close();
    }

    capture c;
    auto stats = co_await replay_shard_segments(params_for(dir, make_schema("cf")->id()), c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.segments_read, 0u);
    BOOST_REQUIRE_EQUAL(stats.entries_read, 0u);
}

// Normal replay counts a corrupt stretch and carries on. Restore cannot: the
// entries behind the damage are the ones the user asked for.
SEASTAR_TEST_CASE(test_corrupted_segment_fails_the_operation) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });

    auto segments = co_await segment_names(dir);
    BOOST_REQUIRE_EQUAL(segments.size(), 1u);
    // The checksum half of the second entry's header.
    co_await corrupt_segment(segments[0], rps[1].pos + 4, 0x451234ab);

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params_for(dir, s->id()), c.consumer()), commitlog::segment_data_corruption_error);
}

// A segment cut short by a crash at the source is not a complete log, so it
// fails rather than replaying what is left.
SEASTAR_TEST_CASE(test_truncated_segment_fails_the_operation) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    uint64_t last_pos = 0;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 200; ++i) {
            last_pos = (co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 1024))).pos;
        }
    });

    auto segments = co_await segment_names(dir);
    BOOST_REQUIRE_EQUAL(segments.size(), 1u);
    {
        // Cut the file inside the written data, as a crash mid-chunk would.
        auto f = co_await open_file_dma(segments[0], open_flags::rw);
        co_await f.truncate(align_down<uint64_t>(last_pos / 2, 4096));
        co_await f.close();
    }

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params_for(dir, s->id()), c.consumer()), commitlog::segment_error);
}

// A consumer failure is the operation's failure: nothing past it is read.
SEASTAR_TEST_CASE(test_consumer_failure_fails_the_operation) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 5; ++i) {
            co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16));
        }
    });

    capture c;
    c.fail_at = 2;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params_for(dir, s->id()), c.consumer()), std::runtime_error);
    BOOST_REQUIRE_EQUAL(c.positions.size(), 2u);
}

// Random entry sizes, so the reader meets chunk and segment boundaries it was
// not written against, checked against the positions the commitlog handed back.
SEASTAR_TEST_CASE(test_random_sized_entries_match_the_recorded_positions) {
    constexpr uint32_t seed = 0x17ead;
    testlog.info("test_random_sized_entries_match_the_recorded_positions seed: {}", seed);
    std::mt19937 rnd(seed);
    std::uniform_int_distribution<size_t> size_dist(16, 200 * 1024);

    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto src = make_schema("src");
    auto other = make_schema("other");
    std::vector<replay_position> src_rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 40; ++i) {
            auto& s = (rnd() % 4 == 0) ? other : src;
            auto rp = co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), size_dist(rnd)));
            if (s == src) {
                src_rps.push_back(rp);
            }
        }
    });
    BOOST_REQUIRE_GT((co_await segment_names(dir)).size(), 1u);
    BOOST_REQUIRE_GT(src_rps.size(), 2u);

    auto bound = src_rps[src_rps.size() / 2];
    std::vector<replay_position> expected;
    std::ranges::copy_if(src_rps, std::back_inserter(expected), [&](const replay_position& rp) {
        return rp >= bound;
    });

    capture c;
    auto stats = co_await replay_shard_segments(params_for(dir, src->id(), bound), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(expected));
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, expected.size());
}

BOOST_AUTO_TEST_SUITE_END()
