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

#include "cql3/description.hh"
#include "db/config.hh"
#include "sstables/sstables_manager.hh"
#include "db/pitr/replay_to.hh"
#include "replica/database.hh"
#include "replica/schema_describe_helper.hh"
#include "schema/schema.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/tmpdir.hh"

using namespace db::pitr;

BOOST_AUTO_TEST_SUITE(pitr_replay_to_test)

namespace {

constexpr const char* test_endpoint = "https://pitr-replay.test";
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
    auto result = start_replay_to(e.db(), e.get_sstorage_manager().local(), in_flight, state_dir.path(), params, as).get();
    BOOST_REQUIRE_MESSAGE(!result, "the replay was not refused");
    return result.error();
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

BOOST_AUTO_TEST_SUITE_END()
