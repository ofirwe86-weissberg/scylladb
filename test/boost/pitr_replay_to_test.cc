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

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>

#include "cql3/description.hh"
#include "db/pitr/replay_to.hh"
#include "replica/database.hh"
#include "replica/schema_describe_helper.hh"
#include "schema/schema.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/tmpdir.hh"

using namespace db::pitr;

BOOST_AUTO_TEST_SUITE(pitr_replay_to_test)

namespace {

constexpr const char* ks_name = "ks";

const ::table_id test_target{utils::UUID("55556666-0000-4000-8000-00000000000c")};

replay_state make_state() {
    return replay_state{
            .phase = replay_phase::started,
            .snapshot = "backup-1",
            .t = "2026-11-04T14:05:00.000000Z",
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

// The statement a snapshot writes to schema.cql, from this node's own describe.
sstring schema_cql_of(cql_test_env& e, const sstring& table) {
    auto s = e.local_db().find_schema(ks_name, table);
    auto helper = replica::make_schema_describe_helper(s, e.local_db().as_data_dictionary());
    return s->describe(helper, cql3::describe_option::STMTS_AND_INTERNALS).create_statement->linearize();
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
    BOOST_REQUIRE_THROW(decode_replay_state("format=pitr-replay\nversion=2\nphase=started\nsnapshot=s\nt=t\nstarted_at_us=1\n"), std::runtime_error);
    BOOST_REQUIRE_THROW(decode_replay_state("format=pitr-replay\nversion=1\nphase=half\nsnapshot=s\nt=t\nstarted_at_us=1\n"), std::runtime_error);
    BOOST_REQUIRE_THROW(decode_replay_state("format=pitr-replay\nversion=1\nphase=started\nsnapshot=s\nt=t\nstarted_at_us=-1\n"), std::runtime_error);
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

BOOST_AUTO_TEST_SUITE_END()
