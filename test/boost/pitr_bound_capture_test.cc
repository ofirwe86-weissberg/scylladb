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

        // Rounds whose capture ran before the flush's sstable joined the list:
        // only those exercise the property, so the test insists on having some.
        int rounds_that_missed_the_flush = 0;
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
                ++rounds_that_missed_the_flush;
                BOOST_REQUIRE(bound <= t.highest_flushed_replay_position());
            }
        }
        BOOST_REQUIRE_GT(rounds_that_missed_the_flush, 0);
    });
}

// Nothing was ever written, so no sstable records a position and the capture
// reports the default one. The snapshot replaces it with the lowest bound the
// format accepts (lowest_safe_bound(), table.cc), because a table with nothing
// flushed on this shard has no write that could sit below any bound. Before the
// capture read the sstables this was min_position(), which answered a different
// question (#103).
SEASTAR_TEST_CASE(test_bound_is_the_default_position_with_nothing_flushed) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& t = create_test_table(e);

        auto capture = t.snapshot_sstables().get();

        BOOST_REQUIRE(capture.sstables.empty());
        // No operator<< for a position, so the comparison, not _EQUAL.
        BOOST_REQUIRE(capture.commitlog_bound == db::replay_position());
    });
}

// The rule itself: the bound is the highest commitlog position the snapshot's
// own sstables record, and never a position read off the commitlog's segment
// bookkeeping. Two flushes give two sstables, so the max over the list is what
// must come back, and a write made after the capture must still be above it.
SEASTAR_TEST_CASE(test_bound_is_the_highest_position_the_snapshots_sstables_record) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& t = create_test_table(e);
        insert_rows(e, 0, 2);
        t.flush().get();
        insert_rows(e, 10, 2);
        t.flush().get();

        db::replay_position bound, highest, floor;
        {
            // In a scope of its own: the capture holds the sstable-list permit,
            // which the flush below takes too, so keeping it would deadlock.
            auto capture = t.snapshot_sstables().get();
            BOOST_REQUIRE_GE(capture.sstables.size(), 2u);
            bound = capture.commitlog_bound;
            floor = capture.commitlog_min_position;
            for (const auto& sst : capture.sstables) {
                const auto& recorded = sst->get_stats_metadata().position;
                if (sst->originated_on_this_node().value_or(false) && recorded.shard_id() == this_shard_id()) {
                    highest = std::max(highest, recorded);
                }
            }
        }
        // The capture reports the sstables' own value; the snapshot records the
        // lower of it and the floor, which this checks is a real position too.
        BOOST_REQUIRE(bound == highest);
        BOOST_REQUIRE(floor != db::replay_position());
        // The invariant the bound exists for: a write the snapshot does not
        // hold is strictly above the bound, so the replay still reaches it.
        // Strictly, because the write is later than every position the
        // snapshot's sstables recorded: a bound taken off the commitlog's
        // segment bookkeeping instead could name a newer, empty segment and
        // sit above this write (#103).
        insert_rows(e, 100, 2);
        t.flush().get();
        BOOST_REQUIRE(bound < t.highest_flushed_replay_position());
    });
}
