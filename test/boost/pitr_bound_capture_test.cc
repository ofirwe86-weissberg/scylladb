/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the commitlog bound captured with a snapshot's sstable list
// (issue #12). Point-in-time recovery replays the commitlog from the bound, so
// the bound must sit at or below every write the snapshot's sstables do not
// hold: otherwise such a write is neither in the backup nor replayed, and the
// restored table silently loses it.
//
// The tests therefore check one property, from three starting states: the
// captured bound is at or below the replay position of a write that was only in
// the memtable when the capture ran.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <algorithm>

#include <boost/test/unit_test.hpp>

#include <seastar/core/thread.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/replay_position.hh"
#include "replica/database.hh"
#include "test/lib/cql_test_env.hh"

namespace {

constexpr auto ks_name = "ks";

replica::table& create_test_table(cql_test_env& e, sstring cf_name = "cf") {
    e.execute_cql(seastar::format("create table {}.{} (k text primary key, v int);", ks_name, cf_name)).get();
    return e.local_db().find_column_family(ks_name, cf_name);
}

// Writes `count` rows, which stay in the memtable until something flushes them.
void insert_rows(cql_test_env& e, int first, int count, sstring cf_name = "cf") {
    for (int i = first; i < first + count; ++i) {
        e.execute_cql(seastar::format("insert into {}.{} (k, v) values ('k{}', {});", ks_name, cf_name, i, i)).get();
    }
}

// The bound, with the permit released before returning: a flush cannot proceed
// while the capture's permit is held. A flush may produce more than one sstable
// when compaction groups are on, so the count is only checked for emptiness.
db::replay_position capture_bound(replica::table& t, bool expect_sstables) {
    auto capture = t.snapshot_sstables().get();
    BOOST_REQUIRE_EQUAL(!capture.sstables.empty(), expect_sstables);
    return capture.commitlog_bound;
}

} // namespace

// The write below is in no sstable of the snapshot, so replay from the bound
// must still reach it.
SEASTAR_TEST_CASE(test_bound_covers_a_write_missing_from_the_snapshot) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& t = create_test_table(e);
        insert_rows(e, 0, 2);
        t.flush().get();

        // Rotating the segment moves the bound off the first write's segment, so
        // that a bound taken too late would be visibly above the writes below.
        t.commitlog()->force_new_active_segment().get();
        // Where the writes the snapshot will not hold begin. The bound must not
        // pass it, or replay starts after them.
        auto before_the_writes = t.commitlog()->current_position();
        insert_rows(e, 100, 2);

        auto bound = capture_bound(t, true);

        t.flush().get();
        BOOST_REQUIRE(bound <= before_the_writes);
        BOOST_REQUIRE(bound <= t.highest_flushed_replay_position());
    });
}

// A flush and a major compaction both take the sstable-list permit, so neither
// can land between the bound and the list a capture returns. The flush is of
// another table, so that the captured table's own write stays in the memtable
// throughout the loop: its segment is dirty, and the bound cannot pass it.
SEASTAR_TEST_CASE(test_bound_covers_a_write_during_concurrent_flush_and_compaction) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& t = create_test_table(e);
        auto& other = create_test_table(e, "cf2");
        // Two sstables, so that the concurrent major compaction has work to do.
        insert_rows(e, 0, 2);
        t.flush().get();
        insert_rows(e, 2, 2);
        t.flush().get();

        t.commitlog()->force_new_active_segment().get();
        auto before_the_writes = t.commitlog()->current_position();
        insert_rows(e, 100, 2);
        insert_rows(e, 0, 2, "cf2");

        auto compaction = t.compact_all_sstables(tasks::make_empty_task_info(), replica::table::do_flush::no);
        auto flush = other.flush();

        // The worst (highest) bound any capture returned while both ran.
        db::replay_position bound;
        for (int i = 0; i < 20; ++i) {
            auto capture = t.snapshot_sstables().get();
            bound = std::max(bound, capture.commitlog_bound);
        }
        compaction.get();
        flush.get();

        t.flush().get();
        BOOST_REQUIRE(bound <= before_the_writes);
        BOOST_REQUIRE(bound <= t.highest_flushed_replay_position());
    });
}

// Nothing to flush: the bound is still the commitlog's own floor, never a
// default-constructed position, which replay would refuse.
SEASTAR_TEST_CASE(test_bound_is_the_commitlog_floor_with_an_empty_memtable) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& t = create_test_table(e);

        auto bound = capture_bound(t, false);

        BOOST_REQUIRE_NE(bound.id, 0u);
        BOOST_REQUIRE(bound == t.commitlog()->min_position());
    });
}
