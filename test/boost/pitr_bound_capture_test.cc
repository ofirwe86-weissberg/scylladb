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
// The reference each test compares the bound against is taken from the write
// itself, through the table's flushed position, and never from the commitlog's
// own front position, which stays below a late bound and would hide the bug.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <algorithm>
#include <unordered_set>

#include <boost/test/unit_test.hpp>

#include <seastar/core/thread.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/replay_position.hh"
#include "replica/database.hh"
#include "sstables/generation_type.hh"
#include "sstables/sstables.hh"
#include "test/lib/cql_test_env.hh"

namespace {

constexpr auto ks_name = "ks";
constexpr auto cf_name = "cf";

replica::table& create_test_table(cql_test_env& e) {
    e.execute_cql(seastar::format("create table {}.{} (k text primary key, v int);", ks_name, cf_name)).get();
    auto& t = e.local_db().find_column_family(ks_name, cf_name);
    // Compaction must not rewrite the lists these tests compare.
    t.disable_auto_compaction().get();
    return t;
}

// Writes `count` rows, which stay in the memtable until something flushes them.
void insert_rows(cql_test_env& e, int first, int count) {
    for (int i = first; i < first + count; ++i) {
        e.execute_cql(seastar::format("insert into {}.{} (k, v) values ('k{}', {});", ks_name, cf_name, i, i)).get();
    }
}

std::unordered_set<sstables::generation_type> generations(const std::vector<sstables::shared_sstable>& ssts) {
    std::unordered_set<sstables::generation_type> gens;
    for (const auto& sst : ssts) {
        gens.insert(sst->generation());
    }
    return gens;
}

} // namespace

// The write below is in no sstable of the snapshot, so replay from the bound
// must still reach it. Everything is flushed and the segment rotated first, so
// that the write's own segment is the commitlog's only live one: a bound taken
// anywhere above the write is then visible.
SEASTAR_TEST_CASE(test_bound_covers_a_write_missing_from_the_snapshot) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& t = create_test_table(e);
        insert_rows(e, 0, 2);
        e.local_db().flush_all_memtables().get();
        t.commitlog()->force_new_active_segment().get();

        insert_rows(e, 100, 2);
        auto bound = t.snapshot_sstables().get().commitlog_bound;

        // The reference comes from the write itself: after flushing this table,
        // the flushed position is the position of the write the capture missed.
        t.flush().get();
        BOOST_REQUIRE(bound <= t.highest_flushed_replay_position());
        BOOST_REQUIRE_NE(bound.id, 0u);
    });
}

// A flush that completes while the capture runs must not leave a write out of
// both the snapshot and the replay. The capture's permit is what rules that
// out, so this drives a real concurrent flush against repeated captures: if a
// capture's list misses the flush's sstable, its bound must still be at or
// below the flushed write.
SEASTAR_TEST_CASE(test_bound_is_never_above_a_write_a_concurrent_flush_left_out) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& t = create_test_table(e);

        for (int round = 0; round < 20; ++round) {
            // Each round starts with a clean commitlog and a sealed segment, so
            // that the round's own write is the lowest live position.
            e.local_db().flush_all_memtables().get();
            t.commitlog()->force_new_active_segment().get();

            insert_rows(e, round * 2, 2);
            t.commitlog()->force_new_active_segment().get();

            auto before = generations(t.snapshot_sstables().get().sstables);

            auto flush = t.flush();
            db::replay_position bound;
            std::unordered_set<sstables::generation_type> captured;
            {
                // The permit has to go before the flush is awaited, or they
                // deadlock, so the capture does not outlive this scope.
                auto capture = t.snapshot_sstables().get();
                bound = capture.commitlog_bound;
                captured = generations(capture.sstables);
            }
            flush.get();

            if (captured == before) {
                // The round's write is in no sstable this capture lists, so the
                // bound must leave it to the replay.
                BOOST_REQUIRE(bound <= t.highest_flushed_replay_position());
            }
        }
    });
}

// Nothing was ever written: the bound is still a real position, never the
// default one, which replay refuses.
SEASTAR_TEST_CASE(test_bound_is_a_real_position_with_an_empty_memtable) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& t = create_test_table(e);

        auto capture = t.snapshot_sstables().get();

        BOOST_REQUIRE(capture.sstables.empty());
        BOOST_REQUIRE_NE(capture.commitlog_bound.id, 0u);
    });
}
