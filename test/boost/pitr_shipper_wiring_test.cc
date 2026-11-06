/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests the point-in-time-recovery wiring as replica::database really builds
// it: that the PITR options turn commitlog::config::hold_segments on together
// with the close notification, that a node without them gets the commitlog it
// had, that a held segment can be opened by the hooks the shipper ships
// through, and that the flush the wiring owes after shipping starts empties a
// memtable. The unit tests in pitr_shipper_test.cc drive the slot and the hooks
// directly, so they cannot see a wiring that holds segments and reports
// nothing, or a path no real segment has.
// See https://github.com/ofirwe86-weissberg/scylladb/issues/8.

#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <tuple>
#include <vector>

#include <seastar/core/format.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "db/pitr/pitr_shipper.hh"
#include "replica/database.hh"
#include "replica/memtable.hh"
#include "test/lib/cql_test_env.hh"

BOOST_AUTO_TEST_SUITE(pitr_shipper_wiring_test)

namespace {

cql_test_config pitr_config() {
    auto cfg = cql_test_config();
    cfg.db_config->pitr_endpoint.set("http://localhost:9000");
    cfg.db_config->pitr_bucket.set("pitr-bucket");
    return cfg;
}

size_t partitions_in_memtables(replica::table& t) {
    size_t partitions = 0;
    t.for_each_active_memtable([&partitions](replica::memtable& m) {
        partitions += m.partition_count();
    });
    return partitions;
}

// Runs the body with one segment that has closed and is still held, which is
// what the shipper ships: the slot is filled with a handler that releases
// nothing, so the hold lasts until this returns. The holds go on the way out,
// as the shipper's would.
template <typename Func>
void with_a_held_closed_segment(cql_test_env& e, Func&& f) {
    auto& db = e.local_db();
    auto* slot = db.pitr_segment_closed_slot();
    BOOST_REQUIRE(slot);
    auto* log = db.commitlog();
    BOOST_REQUIRE(log);

    std::vector<std::tuple<db::segment_id_type, uint64_t, bool>> closed;
    slot->fill([&closed](db::segment_id_type id, uint64_t used_size, bool failed) {
        closed.emplace_back(id, used_size, failed);
    });
    auto release = seastar::defer([&]() noexcept {
        slot->clear();
        for (const auto& [id, used_size, failed] : closed) {
            log->release_segment(id);
        }
    });

    e.execute_cql("create table ks.cf (pk int primary key, v int)").get();
    e.execute_cql("insert into ks.cf (pk, v) values (1, 1)").get();
    log->force_new_active_segment().get();

    BOOST_REQUIRE_EQUAL(closed.size(), 1u);
    const auto [id, used_size, failed] = closed.at(0);
    BOOST_REQUIRE(!failed);
    BOOST_REQUIRE_GT(used_size, 0u);
    f(*log, id, used_size);
}

} // namespace

// With a usable store configured the hold is on, and until something fills the
// slot every segment that closes is released again, so nothing accumulates.
SEASTAR_TEST_CASE(test_a_configured_node_holds_segments_and_releases_them_before_shipping) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                auto& db = e.local_db();
                auto* slot = db.pitr_segment_closed_slot();
                BOOST_REQUIRE(slot);
                // Only the wiring in main.cc fills it, and this environment
                // starts no shipper: the releasing is the slot's own.
                BOOST_REQUIRE(!slot->filled());
                auto* log = db.commitlog();
                BOOST_REQUIRE(log);
                BOOST_REQUIRE_GT(log->segment_hold_limit(), 0u);

                e.execute_cql("create table ks.cf (pk int primary key, v int)").get();
                std::vector<db::segment_id_type> sealed;
                for (int i = 0; i < 3; ++i) {
                    e.execute_cql(seastar::format("insert into ks.cf (pk, v) values ({}, {})", i, i)).get();
                    const auto held = log->held_segments();
                    BOOST_REQUIRE(!held.empty());
                    // The newest held id is the segment being written, which
                    // this seal closes.
                    sealed.push_back(held.back());
                    log->force_new_active_segment().get();
                }
                // A sealed segment can only go once its writes are in an
                // sstable, so the hold is the one thing left keeping it.
                db.flush_all_tables().get();
                log->wait_for_pending_deletes().get();

                BOOST_REQUIRE_EQUAL(log->held_bytes(), 0u);
                // Every segment that was sealed is gone; the one still being
                // written is listed as held and is not shippable yet.
                const auto held = log->held_segments();
                for (auto id : sealed) {
                    BOOST_REQUIRE_MESSAGE(std::ranges::find(held, id) == held.end(), seastar::format("segment {} is still held", id));
                }
            },
            pitr_config());
}

SEASTAR_TEST_CASE(test_a_node_without_pitr_holds_nothing) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& db = e.local_db();
        BOOST_REQUIRE(!db.pitr_segment_closed_slot());
        BOOST_REQUIRE(db.commitlog());
        BOOST_REQUIRE_EQUAL(db.commitlog()->segment_hold_limit(), 0u);
    });
}

// What open_segment() hands an upload has to be that segment's own file, read
// through a handle of the shipper's own.
SEASTAR_TEST_CASE(test_the_hooks_open_the_held_segments_own_file) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                db::pitr::pitr_shipper shipper(e.local_db().get_config());
                shipper.start().get();
                auto stop = seastar::defer([&shipper]() noexcept {
                    shipper.stop().get();
                });

                with_a_held_closed_segment(e, [&](db::commitlog& log, db::segment_id_type id, uint64_t used_size) {
                    const auto path = log.held_segment_path(id);
                    BOOST_REQUIRE(path);
                    // The name is what carries the id to a restore.
                    BOOST_REQUIRE(path->find(seastar::format("-{}.log", id)) != seastar::sstring::npos);

                    auto hooks = db::pitr::make_segment_hooks(shipper, [&log](db::segment_id_type id) {
                        return log.held_segment_path(id);
                    });
                    auto f = hooks.open_segment(id).get();
                    auto close_f = seastar::defer([&f]() noexcept {
                        f.close().get();
                    });
                    BOOST_REQUIRE_GE(f.size().get(), used_size);

                    // The same bytes the file holds, through a handle this test
                    // opened itself: a hook pointing at another segment's file,
                    // or at the start of the wrong one, would differ here.
                    constexpr size_t read_size = 4096;
                    auto through_the_hook = f.dma_read_exactly<char>(0, read_size).get();
                    auto direct = seastar::open_file_dma(*path, seastar::open_flags::ro).get();
                    auto close_direct = seastar::defer([&direct]() noexcept {
                        direct.close().get();
                    });
                    auto expected = direct.dma_read_exactly<char>(0, read_size).get();
                    BOOST_REQUIRE_EQUAL(through_the_hook.size(), expected.size());
                    BOOST_REQUIRE(std::equal(through_the_hook.begin(), through_the_hook.end(), expected.begin()));

                    // An id this log does not hold has no file to ship.
                    BOOST_REQUIRE_THROW(hooks.open_segment(id + 1000).get(), std::runtime_error);
                });
            },
            pitr_config());
}

// The flush the wiring owes once shipping has started: without it a table
// nothing writes to again would keep a later backup's bound below the chain's
// start, and that backup would be refused.
SEASTAR_TEST_CASE(test_the_flush_after_shipping_started_empties_a_memtable) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                auto& db = e.local_db();
                e.execute_cql("create table ks.cf (pk int primary key, v int)").get();
                e.execute_cql("insert into ks.cf (pk, v) values (1, 1)").get();
                auto& t = db.find_column_family("ks", "cf");
                BOOST_REQUIRE_GT(partitions_in_memtables(t), 0u);

                db::pitr::pitr_shipper shipper(db.get_config());
                shipper.start().get();
                shipper.note_cluster_size(1);
                BOOST_REQUIRE(shipper.active());
                shipper.flush_after_start([&db] {
                    return db.flush_all_memtables();
                });
                // stop() waits for the flush, so there is nothing to poll for.
                shipper.stop().get();

                BOOST_REQUIRE_EQUAL(partitions_in_memtables(t), 0u);
            },
            pitr_config());
}

BOOST_AUTO_TEST_SUITE_END()
