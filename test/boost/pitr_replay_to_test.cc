/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests the checks a point-in-time replay makes before it loads anything, and
// the durable mark it writes (issue #18, ep.1). Every one of them stands
// between a user and a silently wrong restore: a replay into a table that holds
// data, or whose schema is not the backup's, or that was already replayed into,
// cannot be told apart from a correct one afterwards. Each refusal therefore
// gets a test of its own, and so does each way the mark can be read.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <filesystem>
#include <unordered_set>

#include <boost/test/unit_test.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>

#include "compaction/compaction_manager.hh"
#include "cql3/description.hh"
#include "db/config.hh"
#include "sstables/sstables_manager.hh"
#include "utils/rjson.hh"
#include "db/pitr/replay_to.hh"
#include "replica/database.hh"
#include "replica/schema_describe_helper.hh"
#include "schema/schema.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/tmpdir.hh"

using namespace db::pitr;

BOOST_AUTO_TEST_SUITE(pitr_replay_to_test)

namespace {

// A loopback address and not a name: a check that is skipped by mistake must
// fail on the refusal-code assertion, not 50 s later on a DNS lookup.
constexpr const char* test_endpoint = "https://127.0.0.1:1";
constexpr const char* ks_name = "ks";

const ::table_id test_target{utils::UUID("55556666-0000-4000-8000-00000000000c")};

replay_state make_state() {
    return replay_state{
            .phase = replay_phase::started,
            .snapshot = "backup-1",
            .t = "2026-11-04T14:05:00.000000Z",
            .source_keyspace = "ks",
            .source_table = "source",
            .started_at_us = 1762400000000000LL,
    };
}

// A file written as it is, so that a test can put damage where a mark belongs.
void write_raw(const std::filesystem::path& path, std::string_view body) {
    auto f = seastar::open_file_dma(path.native(), seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate).get();
    auto out = seastar::make_file_output_stream(std::move(f)).get();
    out.write(body.data(), body.size()).get();
    out.close().get();
}

refusal refusal_of(cql_test_env& e, const tmpdir& state_dir, const replay_to_params& params, replay_in_flight& in_flight) {
    abort_source as;
    try {
        auto result = start_replay_to(e.db(), e.get_sstorage_manager().local(), in_flight, state_dir.path(), params, as).get();
        BOOST_REQUIRE_MESSAGE(!result, "the replay was not refused");
        return std::move(result.error());
    } catch (const std::exception& ex) {
        // A check that is skipped by mistake runs on to the store, which these
        // tests do not have: report that as the failed assertion it is, rather
        // than as an I/O error from an endpoint nobody meant to reach.
        BOOST_FAIL(fmt::format("the replay reached the store instead of refusing: {}", ex.what()));
        throw;
    }
}

// The statement a snapshot writes to schema.cql, from this node's own describe.
sstring schema_cql_of(cql_test_env& e, const sstring& table) {
    auto s = e.local_db().find_schema(ks_name, table);
    auto helper = replica::make_schema_describe_helper(s, e.local_db().as_data_dictionary());
    return s->describe(helper, cql3::describe_option::STMTS_AND_INTERNALS).create_statement->linearize();
}

// Nothing talks to the endpoint: every check these tests reach runs before the
// first read, and building a client does no I/O.
cql_test_config replay_config() {
    auto cfg = cql_test_config();
    ::setenv("AWS_ACCESS_KEY_ID", "pitr-test-key", 0);
    ::setenv("AWS_SECRET_ACCESS_KEY", "pitr-test-secret", 0);
    ::setenv("AWS_DEFAULT_REGION", "local", 0);
    cfg.db_config->object_storage_endpoints({db::object_storage_endpoint_param(db::object_storage_endpoint_param::s3_storage{
            .endpoint = test_endpoint,
            .region = "local",
            .iam_role_arn = "",
            .legacy_format = false,
    })});
    return cfg;
}

replay_to_params params_for(const sstring& table, const sstring& t = "2026-11-04T14:05:00.000000Z") {
    return replay_to_params{
            .endpoint = test_endpoint,
            .bucket = "pitr-bucket",
            .prefix = "cf1/backup-1",
            .keyspace = ks_name,
            .table = table,
            .snapshot = "backup-1",
            .t = t,
    };
}

refusal refusal_of(cql_test_env& e, const tmpdir& state_dir, const replay_to_params& params) {
    replay_in_flight in_flight;
    return refusal_of(e, state_dir, params, in_flight);
}

} // namespace

// The codes leave the node in a restore's result and are matched by tests and
// tooling, so two reasons sharing one code would make a refusal unreadable.
BOOST_AUTO_TEST_CASE(every_refusal_reason_has_a_code_of_its_own) {
    std::unordered_set<std::string_view> codes;
    for (auto reason : all_refusal_reasons) {
        auto code = reason_code(reason);
        BOOST_REQUIRE(code != "unknown");
        BOOST_REQUIRE_MESSAGE(codes.insert(code).second, fmt::format("the code {} is used twice", code));
    }
}

BOOST_AUTO_TEST_CASE(a_mark_survives_a_round_trip_through_its_codec) {
    auto state = make_state();
    BOOST_REQUIRE(decode_replay_state(encode(state)) == state);
    state.phase = replay_phase::done;
    BOOST_REQUIRE(decode_replay_state(encode(state)) == state);
}

// The mark is also read by a human after a crash, and a newline would make the
// next line read as a field.
BOOST_AUTO_TEST_CASE(encoding_a_mark_refuses_a_value_holding_a_newline) {
    auto state = make_state();
    state.snapshot = "backup\nphase=done";
    BOOST_REQUIRE_THROW(encode(state), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(decoding_a_mark_refuses_damage) {
    auto whole = encode(make_state());
    // Truncated, a field renamed, a bad number, and bytes after the last line:
    // each is damage and none may read as a mark of its own.
    BOOST_REQUIRE_THROW(decode_replay_state(whole.substr(0, whole.size() / 2)), std::runtime_error);
    BOOST_REQUIRE_THROW(decode_replay_state(""), std::runtime_error);
    BOOST_REQUIRE_THROW(
            decode_replay_state("format=pitr-replay\nversion=2\nphase=started\nsnapshot=s\nt=t\nsource_keyspace=k\nsource_table=c\nstarted_at_us=1\n"),
            std::runtime_error);
    BOOST_REQUIRE_THROW(decode_replay_state("format=pitr-replay\nversion=1\nphase=half\nsnapshot=s\nt=t\nsource_keyspace=k\nsource_table=c\nstarted_at_us=1\n"),
            std::runtime_error);
    BOOST_REQUIRE_THROW(
            decode_replay_state("format=pitr-replay\nversion=1\nphase=started\nsnapshot=s\nt=t\nsource_keyspace=k\nsource_table=c\nstarted_at_us=-1\n"),
            std::runtime_error);
    // A mark written before the source was recorded is damage, not a mark with
    // two fields missing: the keys are the format.
    BOOST_REQUIRE_THROW(decode_replay_state("format=pitr-replay\nversion=1\nphase=started\nsnapshot=s\nt=t\nstarted_at_us=1\n"), std::runtime_error);
    BOOST_REQUIRE_THROW(decode_replay_state(whole + "phase=done\n"), std::runtime_error);
}

SEASTAR_TEST_CASE(a_mark_is_read_back_from_disk_as_it_was_written) {
    return seastar::async([] {
        tmpdir state_dir;
        auto state = make_state();
        write_replay_state(state_dir.path(), test_target, state).get();
        auto read = read_replay_state(state_dir.path(), test_target).get();
        BOOST_REQUIRE(read);
        BOOST_REQUIRE(*read == state);
        // The rename is over the final name, so nothing is left beside it.
        BOOST_REQUIRE(!seastar::file_exists(replay_state_path(state_dir.path(), test_target).native() + ".tmp").get());
    });
}

SEASTAR_TEST_CASE(no_mark_reads_as_a_target_no_replay_touched) {
    return seastar::async([] {
        tmpdir state_dir;
        BOOST_REQUIRE(!read_replay_state(state_dir.path(), test_target).get());
    });
}

// A target whose mark is damaged is not a target that was never replayed into,
// so the read says so rather than answering `nullopt`.
SEASTAR_TEST_CASE(a_damaged_mark_on_disk_does_not_read_as_no_mark) {
    return seastar::async([] {
        tmpdir state_dir;
        seastar::recursive_touch_directory(state_dir.path().native()).get();
        write_raw(replay_state_path(state_dir.path(), test_target), "format=pitr-replay\nversion=1\n");
        BOOST_REQUIRE_THROW(read_replay_state(state_dir.path(), test_target).get(), std::runtime_error);
    });
}

// What a replay into another table is allowed to differ in: the name the
// statements carry, the ID option and the compaction options (P52).
SEASTAR_TEST_CASE(a_schema_differing_only_in_name_id_and_compaction_matches) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.source (pk text PRIMARY KEY, value int) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}").get();
        e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int) WITH compaction = {'class': 'LeveledCompactionStrategy'}").get();
        BOOST_REQUIRE(!check_schema_match(schema_cql_of(e, "target"), schema_cql_of(e, "source")).get());
    });
}

SEASTAR_TEST_CASE(a_target_with_a_column_the_backup_has_not_is_refused) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.source (pk text PRIMARY KEY, value int)").get();
        e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int, extra int)").get();
        auto refused = check_schema_match(schema_cql_of(e, "target"), schema_cql_of(e, "source")).get();
        BOOST_REQUIRE(refused);
        BOOST_REQUIRE(refused->code() == "target_schema_differs");
    });
}

// The target must carry the backup's dropped columns too: exactness at T for a
// dropped and re-added column relies on the target's `dropped_at` (#18, the
// comment of 2026-11-05).
SEASTAR_TEST_CASE(a_target_missing_the_backups_dropped_column_is_refused) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.source (pk text PRIMARY KEY, value int, gone int)").get();
        e.execute_cql("ALTER TABLE ks.source DROP gone").get();
        e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int)").get();
        auto refused = check_schema_match(schema_cql_of(e, "target"), schema_cql_of(e, "source")).get();
        BOOST_REQUIRE(refused);
        BOOST_REQUIRE(refused->code() == "target_schema_differs");
    });
}


// Nothing in a WITH clause may differ, only the three above: a replay into a
// target whose options are not the backup's does not restore the backup's
// behaviour. Each option gets a case of its own, because a normaliser that
// drops one line too many drops them all silently.
SEASTAR_TEST_CASE(a_target_whose_clustering_order_differs_is_refused) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.source (pk text, ck int, value int, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck ASC)").get();
        e.execute_cql("CREATE TABLE ks.target (pk text, ck int, value int, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)").get();
        auto refused = check_schema_match(schema_cql_of(e, "target"), schema_cql_of(e, "source")).get();
        BOOST_REQUIRE(refused);
        BOOST_REQUIRE(refused->code() == "target_schema_differs");
    });
}

SEASTAR_TEST_CASE(a_target_whose_default_time_to_live_differs_is_refused) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.source (pk text PRIMARY KEY, value int) WITH default_time_to_live = 0").get();
        e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int) WITH default_time_to_live = 3600").get();
        auto refused = check_schema_match(schema_cql_of(e, "target"), schema_cql_of(e, "source")).get();
        BOOST_REQUIRE(refused);
        BOOST_REQUIRE(refused->code() == "target_schema_differs");
    });
}

SEASTAR_TEST_CASE(a_target_whose_gc_grace_seconds_differs_is_refused) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.source (pk text PRIMARY KEY, value int) WITH gc_grace_seconds = 864000").get();
        e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int) WITH gc_grace_seconds = 1234").get();
        auto refused = check_schema_match(schema_cql_of(e, "target"), schema_cql_of(e, "source")).get();
        BOOST_REQUIRE(refused);
        BOOST_REQUIRE(refused->code() == "target_schema_differs");
    });
}

// The other side of the same rule: the id is the one thing in the WITH clause
// a replay must ignore, since the target is a table of its own.
SEASTAR_TEST_CASE(two_statements_differing_only_in_their_id_match) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.source (pk text PRIMARY KEY, value int)").get();
        auto backup = schema_cql_of(e, "source");
        auto target = backup;
        auto id = e.local_db().find_schema(ks_name, "source")->id().to_sstring();
        auto at = target.find(id);
        BOOST_REQUIRE(at != sstring::npos);
        target = target.substr(0, at) + "11112222-3333-4444-5555-666677778888" + target.substr(at + id.size());
        BOOST_REQUIRE(target != backup);
        BOOST_REQUIRE(!check_schema_match(target, backup).get());
    });
}

namespace {

// The manifest a snapshot writes, cut down to the fields a replay reads.
sstring manifest_json(const char* snapshot, const char* keyspace, const char* table, const char* sstables) {
    return seastar::format(R"({{"manifest":{{"version":"1.0","scope":"node"}},"snapshot":{{"name":"{}","created_at":1}},)"
                           R"("table":{{"keyspace_name":"{}","table_name":"{}","table_id":"x","tablets_type":"none","tablet_count":1}},)"
                           R"("sstables":[{}]}})",
            snapshot, keyspace, table, sstables);
}

const char* two_sstables = R"({"id":"a","toc_name":"me-1-big-TOC.txt","data_size":1,"index_size":1,"first_token":1,"last_token":2,"tablet_id":0},)"
                           R"({"id":"b","toc_name":"me-2-big-TOC.txt","data_size":1,"index_size":1,"first_token":3,"last_token":4,"tablet_id":0})";

rjson::chunked_content content_of(const sstring& text) {
    rjson::chunked_content content;
    content.push_back(temporary_buffer<char>(text.data(), text.size()));
    return content;
}

std::expected<backup_manifest, refusal> manifest_of(const sstring& text, const char* expected_snapshot = "backup-1") {
    return parse_backup_manifest(content_of(text), expected_snapshot).get();
}

// A manifest of `count` sstables, big enough that the parse cannot finish
// inside one task quota.
sstring manifest_with_many_sstables(size_t count) {
    sstring entries;
    for (size_t i = 0; i < count; ++i) {
        entries += seastar::format(R"({}{{"id":"{}","toc_name":"me-{}-big-TOC.txt","data_size":1,"index_size":1,"first_token":{},"last_token":{},)"
                                   R"("tablet_id":0}})",
                i ? "," : "", i, i, i, i + 1);
    }
    return manifest_json("backup-1", "ks", "source", entries.c_str());
}

} // namespace

// One prefix is one backup, so a prefix naming another backup is a user error
// and must not be restored under the name the call gave.
SEASTAR_TEST_CASE(a_manifest_naming_another_snapshot_is_refused) {
    return seastar::async([] {
        auto refused = manifest_of(manifest_json("another-backup", "ks", "source", two_sstables));
        BOOST_REQUIRE(!refused);
        BOOST_REQUIRE(refused.error().code() == "backup_names_another_table");
        // The reason names what the manifest said, so the user can find the
        // prefix they meant.
        BOOST_REQUIRE(refused.error().detail.find("another-backup") != sstring::npos);
    });
}

SEASTAR_TEST_CASE(a_manifest_gives_the_backups_sstables_in_order) {
    return seastar::async([] {
        auto manifest = manifest_of(manifest_json("backup-1", "ks", "source", two_sstables));
        BOOST_REQUIRE(manifest);
        BOOST_REQUIRE_EQUAL(manifest->snapshot, "backup-1");
        BOOST_REQUIRE_EQUAL(manifest->keyspace, "ks");
        BOOST_REQUIRE_EQUAL(manifest->table, "source");
        BOOST_REQUIRE_EQUAL(manifest->toc_names.size(), 2u);
        BOOST_REQUIRE_EQUAL(manifest->toc_names[0], "me-1-big-TOC.txt");
        BOOST_REQUIRE_EQUAL(manifest->toc_names[1], "me-2-big-TOC.txt");
    });
}

// A backup of a table that held nothing is a backup like any other: the replay
// loads no sstable and goes on to the history.
SEASTAR_TEST_CASE(a_manifest_with_no_sstables_is_not_refused) {
    return seastar::async([] {
        auto manifest = manifest_of(manifest_json("backup-1", "ks", "source", ""));
        BOOST_REQUIRE(manifest);
        BOOST_REQUIRE(manifest->toc_names.empty());
    });
}

SEASTAR_TEST_CASE(a_damaged_manifest_is_refused) {
    return seastar::async([] {
        // Not JSON, no snapshot object, and an sstable entry with no toc_name:
        // none may read as a backup a replay can use.
        for (const auto& text : {sstring("{nonsense"), sstring(R"({"table":{}})"),
                     manifest_json("backup-1", "ks", "source", R"({"id":"a","first_token":1,"last_token":2,"tablet_id":0})")}) {
            auto refused = manifest_of(text);
            BOOST_REQUIRE_MESSAGE(!refused, fmt::format("'{}' was not refused", text));
            BOOST_REQUIRE(refused.error().code() == "backup_manifest_damaged");
        }
    });
}

// Two calls on one target would write over each other's work, and the second
// one cannot tell its own writes from the first one's (P62).
BOOST_AUTO_TEST_CASE(one_target_takes_one_replay_at_a_time) {
    replay_in_flight in_flight;
    auto first = in_flight.try_take(test_target);
    BOOST_REQUIRE(first);
    BOOST_REQUIRE(in_flight.holds(test_target));
    BOOST_REQUIRE(!in_flight.try_take(test_target));
}

BOOST_AUTO_TEST_CASE(another_target_is_not_blocked_by_a_running_replay) {
    replay_in_flight in_flight;
    auto first = in_flight.try_take(test_target);
    BOOST_REQUIRE(in_flight.try_take(::table_id(utils::UUID("77778888-0000-4000-8000-00000000000d"))));
}

// The target is free again when the run ends, however it ended.
BOOST_AUTO_TEST_CASE(a_target_is_released_when_its_replay_ends) {
    replay_in_flight in_flight;
    {
        auto held = in_flight.try_take(test_target);
        BOOST_REQUIRE(held);
    }
    BOOST_REQUIRE(!in_flight.holds(test_target));
    BOOST_REQUIRE(in_flight.try_take(test_target));
}

SEASTAR_TEST_CASE(a_malformed_t_is_refused_before_anything_else) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                tmpdir state_dir;
                // No target and no store location either: T is answered first, so a
                // user fixing one thing at a time is told about T.
                auto params = params_for("nothing", "2026-11-04 14:05:00");
                params.endpoint = "";
                BOOST_REQUIRE(refusal_of(e, state_dir, params).code() == "t_invalid");
            },
            replay_config());
}

SEASTAR_TEST_CASE(a_store_location_this_node_does_not_know_is_refused) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                tmpdir state_dir;
                auto params = params_for("nothing");
                params.endpoint = "https://not-configured.test";
                BOOST_REQUIRE(refusal_of(e, state_dir, params).code() == "store_location_missing");
            },
            replay_config());
}

SEASTAR_TEST_CASE(a_target_that_does_not_exist_is_refused) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                tmpdir state_dir;
                BOOST_REQUIRE(refusal_of(e, state_dir, params_for("nothing")).code() == "target_missing");
            },
            replay_config());
}

// A compaction during the replay purges a tombstone before the writes it
// covers are replayed, so a target with auto-compaction on is refused before
// anything is loaded.
SEASTAR_TEST_CASE(a_target_with_auto_compaction_enabled_is_refused) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                tmpdir state_dir;
                e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int)").get();
                BOOST_REQUIRE(refusal_of(e, state_dir, params_for("target")).code() == "target_auto_compaction_enabled");
            },
            replay_config());
}

SEASTAR_TEST_CASE(a_target_holding_data_is_refused) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                tmpdir state_dir;
                e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int)").get();
                e.db().invoke_on_all([](replica::database& db) {
                          return db.find_column_family(ks_name, "target").disable_auto_compaction();
                      }).get();
                e.execute_cql("INSERT INTO ks.target (pk, value) VALUES ('a', 1)").get();
                BOOST_REQUIRE(refusal_of(e, state_dir, params_for("target")).code() == "target_not_empty");
            },
            replay_config());
}

// A second call on a table a replay already ran into is refused, not answered
// from the first run's result (P62).
SEASTAR_TEST_CASE(a_target_already_replayed_into_is_refused) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                tmpdir state_dir;
                e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int)").get();
                e.db().invoke_on_all([](replica::database& db) {
                          return db.find_column_family(ks_name, "target").disable_auto_compaction();
                      }).get();
                auto target = e.local_db().find_schema(ks_name, "target")->id();
                write_replay_state(state_dir.path(), target, make_state()).get();
                auto refused = refusal_of(e, state_dir, params_for("target"));
                BOOST_REQUIRE(refused.code() == "target_already_replayed");
                // The reason names the mark, so an operator can read what ran before.
                BOOST_REQUIRE(refused.detail.find("backup-1") != sstring::npos);
            },
            replay_config());
}

// A mark that cannot be read means a replay ran and left damage behind, which
// is not the same as a target no replay ever touched.
SEASTAR_TEST_CASE(a_target_whose_mark_is_unreadable_is_refused) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                tmpdir state_dir;
                e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int)").get();
                e.db().invoke_on_all([](replica::database& db) {
                          return db.find_column_family(ks_name, "target").disable_auto_compaction();
                      }).get();
                auto target = e.local_db().find_schema(ks_name, "target")->id();
                write_raw(replay_state_path(state_dir.path(), target), "not a mark\n");
                BOOST_REQUIRE(refusal_of(e, state_dir, params_for("target")).code() == "target_already_replayed");
            },
            replay_config());
}


// A second call while the first run holds the target is refused, rather than
// writing the backup over the first run's work (P62).
SEASTAR_TEST_CASE(a_second_call_on_a_target_a_replay_is_running_into_is_refused) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                tmpdir state_dir;
                e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int)").get();
                e.db().invoke_on_all([](replica::database& db) {
                          return db.find_column_family(ks_name, "target").disable_auto_compaction();
                      }).get();
                // What the first run holds while it is loading, taken by hand:
                // the first call's own guard lives inside its result.
                replay_in_flight in_flight;
                auto first = in_flight.try_take(e.local_db().find_schema(ks_name, "target")->id());
                BOOST_REQUIRE(first);
                BOOST_REQUIRE(refusal_of(e, state_dir, params_for("target"), in_flight).code() == "replay_already_running");
            },
            replay_config());
}


// A replay restores one table of one node from that node's own commitlog, which
// vnodes give and tablets do not (REF-1). Refused before anything touches the
// table, which also keeps the run's compaction hold away from tablet splits.
SEASTAR_TEST_CASE(a_target_in_a_tablets_keyspace_is_refused) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                tmpdir state_dir;
                e.execute_cql("CREATE KEYSPACE tks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = "
                              "{'enabled': true}")
                        .get();
                e.execute_cql("CREATE TABLE tks.target (pk text PRIMARY KEY, value int)").get();
                auto params = params_for("target");
                params.keyspace = "tks";
                BOOST_REQUIRE(refusal_of(e, state_dir, params).code() == "target_uses_tablets");
            },
            replay_config());
}

// The refusal asks the user to create the target with compaction disabled, so
// a target created that way must pass the check that raises it.
SEASTAR_TEST_CASE(a_target_created_with_compaction_disabled_passes_the_check) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                tmpdir state_dir;
                e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int) WITH compaction = {'class': 'SizeTieredCompactionStrategy', "
                              "'enabled': 'false'}")
                        .get();
                // Nothing disabled it by hand, so only the schema says so. One
                // row makes the next check refuse, which is how this one proves
                // it passed without a store to read.
                e.execute_cql("INSERT INTO ks.target (pk, value) VALUES ('a', 1)").get();
                BOOST_REQUIRE(refusal_of(e, state_dir, params_for("target")).code() == "target_not_empty");
            },
            replay_config());
}

namespace {

replica::table& target_table(cql_test_env& e, const char* table) {
    return e.local_db().find_column_family(ks_name, table);
}

} // namespace

// A compaction in the middle of a replay purges a tombstone before the writes
// it covers are replayed, so the run holds every compaction of the target off,
// a major included, and says so at the end.
SEASTAR_TEST_CASE(a_held_table_reports_every_compaction_disabled) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int)").get();
        e.db().invoke_on_all([](replica::database& db) {
                  return db.find_column_family(ks_name, "target").disable_auto_compaction();
              }).get();
        auto& t = target_table(e, "target");
        auto hold = t.hold_compaction_off("point-in-time replay test").get();
        BOOST_REQUIRE(!hold.cres.empty());
        BOOST_REQUIRE(t.compaction_still_held(hold));
    });
}

// Released, the table compacts again: the hold may not outlive the run.
SEASTAR_TEST_CASE(a_released_table_no_longer_reports_compaction_disabled) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int)").get();
        e.db().invoke_on_all([](replica::database& db) {
                  return db.find_column_family(ks_name, "target").disable_auto_compaction();
              }).get();
        auto& t = target_table(e, "target");
        auto hold = t.hold_compaction_off("point-in-time replay test").get();
        auto released = std::move(hold.cres);
        released.clear();
        BOOST_REQUIRE(!t.compaction_still_held(hold));
    });
}

// Auto-compaction turned back on while the run holds the table is exactly the
// case the hold cannot stop, so the run has to fail instead.
SEASTAR_TEST_CASE(auto_compaction_enabled_under_a_hold_is_reported) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int)").get();
        e.db().invoke_on_all([](replica::database& db) {
                  return db.find_column_family(ks_name, "target").disable_auto_compaction();
              }).get();
        auto& t = target_table(e, "target");
        auto hold = t.hold_compaction_off("point-in-time replay test").get();
        BOOST_REQUIRE(t.compaction_still_held(hold));
        t.enable_auto_compaction();
        BOOST_REQUIRE(!t.compaction_still_held(hold));
    });
}


// The hold is what stops a compaction asked for by hand: auto-compaction being
// off does not. A major asked for while a replay holds the target is skipped,
// so the sstables the backup landed are still there when the history is
// replayed over them. It is the outcome that is asserted, not the timing: a
// major on a disabled view returns at once without compacting anything.
SEASTAR_TEST_CASE(a_major_compaction_asked_for_under_a_hold_does_not_run) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.target (pk text PRIMARY KEY, value int)").get();
        e.db().invoke_on_all([](replica::database& db) {
                  return db.find_column_family(ks_name, "target").disable_auto_compaction();
              }).get();
        // Two sstables on this shard, so a major has something to do. Enough
        // keys that some of them land here, whatever the shard count.
        for (int round = 0; round < 2; ++round) {
            for (int key = 0; key < 32; ++key) {
                e.execute_cql(seastar::format("INSERT INTO ks.target (pk, value) VALUES ('{}-{}', {})", round, key, key)).get();
            }
            e.db().invoke_on_all([](replica::database& db) {
                      return db.flush_all_memtables();
                  }).get();
        }
        auto& t = target_table(e, "target");
        auto before = t.sstables_count();
        BOOST_REQUIRE_GE(before, 2u);
        auto hold = t.hold_compaction_off("point-in-time replay test").get();
        t.compact_all_sstables(tasks::make_empty_task_info()).get();
        // Nothing was compacted: the two sstables the backup would have landed
        // are still the two the replay will replay over.
        BOOST_REQUIRE_EQUAL(t.sstables_count(), before);
        BOOST_REQUIRE(t.compaction_still_held(hold));
        hold.cres.clear();
        // Released, the same request does compact them.
        t.compact_all_sstables(tasks::make_empty_task_info()).get();
        BOOST_REQUIRE_LT(t.sstables_count(), before);
    });
}


// Two replays into two tables run at once, so one hold may not stand for the
// other: releasing B leaves A held, and A's run still vouches for its target.
SEASTAR_TEST_CASE(releasing_one_targets_hold_leaves_another_held) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        for (const char* table : {"target_a", "target_b"}) {
            e.execute_cql(seastar::format("CREATE TABLE ks.{} (pk text PRIMARY KEY, value int)", table)).get();
            e.db().invoke_on_all([table](replica::database& db) {
                      return db.find_column_family(ks_name, table).disable_auto_compaction();
                  }).get();
        }
        auto& a = target_table(e, "target_a");
        auto& b = target_table(e, "target_b");
        auto hold_a = a.hold_compaction_off("point-in-time replay test A").get();
        auto hold_b = b.hold_compaction_off("point-in-time replay test B").get();
        hold_b.cres.clear();
        BOOST_REQUIRE(a.compaction_still_held(hold_a));
        BOOST_REQUIRE(!b.compaction_still_held(hold_b));
    });
}


// Two replays into two tables run at once, so one slot per shard would have
// one replacing the other's hold and the first to finish releasing what the
// other still needs.
BOOST_AUTO_TEST_CASE(a_hold_is_kept_and_released_per_target) {
    const ::table_id other{utils::UUID("77778888-0000-4000-8000-00000000000d")};
    db::pitr::per_target<int> holds;
    holds.add(test_target, 1);
    holds.add(other, 2);
    BOOST_REQUIRE_EQUAL(holds.size(), 2u);
    holds.erase(other);
    BOOST_REQUIRE(!holds.find(other));
    BOOST_REQUIRE(holds.find(test_target));
    BOOST_REQUIRE_EQUAL(*holds.find(test_target), 1);
}

// The replay applies the entries of the table the backup was taken from and no
// other, matched on the id its commitlog entries carry; only the backup's own
// schema.cql records it.
SEASTAR_TEST_CASE(the_source_tables_id_is_read_from_the_backups_schema) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.source (pk text PRIMARY KEY, value int)").get();
        const auto parsed = parse_source_table_id(schema_cql_of(e, "source"));
        BOOST_REQUIRE(parsed);
        BOOST_REQUIRE_EQUAL(*parsed, e.local_db().find_schema(ks_name, "source")->id());
    });
}

// A statement describe wrote without its internals, or one an unsupported
// version wrote: there is nothing to match the shipped entries against, and a
// guess would apply another table's writes.
SEASTAR_TEST_CASE(a_backup_schema_without_an_id_has_no_source_table_id) {
    BOOST_REQUIRE(!parse_source_table_id("CREATE TABLE ks.source (\n    pk text PRIMARY KEY\n) WITH comment = ''\n"));
    BOOST_REQUIRE(!parse_source_table_id(""));
    // The words are the option only where describe writes it; a value holding
    // them is not an id.
    BOOST_REQUIRE(!parse_source_table_id("CREATE TABLE ks.source (\n    pk text PRIMARY KEY\n) WITH comment = ') WITH ID = 1'\n"));
    // Damage where the id belongs is not an id either.
    BOOST_REQUIRE(!parse_source_table_id(") WITH ID = not-a-uuid\n"));
    BOOST_REQUIRE(!parse_source_table_id(") WITH ID = 00000000-0000-0000-0000-000000000000\n"));
    return make_ready_future<>();
}

// `rjson::parse_yieldable` yields through `seastar::thread::maybe_yield()`, so
// it must run in a thread: called from a plain coroutine, the yield has no
// thread to switch to and the node crashes. Every other test here goes through
// `seastar::async`, which is a thread context and hides the bug, so this one
// deliberately does not: it is a coroutine, like the endpoint's own caller, and
// the manifest is large enough that the parse is preempted.
SEASTAR_TEST_CASE(a_large_manifest_parses_from_a_coroutine) {
    constexpr size_t sstables = 20000;
    auto manifest = co_await parse_backup_manifest(content_of(manifest_with_many_sstables(sstables)), "backup-1");
    BOOST_REQUIRE(manifest);
    BOOST_REQUIRE_EQUAL(manifest->toc_names.size(), sstables);
    BOOST_REQUIRE_EQUAL(manifest->toc_names.back(), seastar::format("me-{}-big-TOC.txt", sstables - 1));
}

BOOST_AUTO_TEST_SUITE_END()
