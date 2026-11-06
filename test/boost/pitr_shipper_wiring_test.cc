/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests the point-in-time-recovery hold as replica::database really builds it:
// that the PITR options turn commitlog::config::hold_segments on together with
// the close notification, and that a node without them gets the commitlog it
// had. The unit tests in pitr_shipper_test.cc drive the slot directly, so they
// cannot see a wiring that holds segments and reports nothing.
// See https://github.com/ofirwe86-weissberg/scylladb/issues/8.

#include <boost/test/unit_test.hpp>

#include <seastar/core/format.hh>
#include <seastar/testing/test_case.hh>

#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "db/pitr/pitr_shipper.hh"
#include "replica/database.hh"
#include "test/lib/cql_test_env.hh"

BOOST_AUTO_TEST_SUITE(pitr_shipper_wiring_test)

// With a usable store configured the hold is on, and until something fills the
// slot every segment that closes is released again, so nothing accumulates.
SEASTAR_TEST_CASE(test_a_configured_node_holds_segments_and_releases_them_before_shipping) {
    auto cfg = cql_test_config();
    cfg.db_config->pitr_endpoint.set("http://localhost:9000");
    cfg.db_config->pitr_bucket.set("pitr-bucket");

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
                for (int i = 0; i < 3; ++i) {
                    e.execute_cql(seastar::format("insert into ks.cf (pk, v) values ({}, {})", i, i)).get();
                    log->force_new_active_segment().get();
                }
                // A sealed segment can only go once its writes are in an
                // sstable, so the hold is the one thing left keeping it.
                db.flush_all_tables().get();
                log->wait_for_pending_deletes().get();

                BOOST_REQUIRE_EQUAL(log->held_bytes(), 0u);
                // The segment still being written to is listed as held and is
                // not shippable yet; every sealed one is gone.
                BOOST_REQUIRE_LE(log->held_segments().size(), 1u);
            },
            cfg);
}

SEASTAR_TEST_CASE(test_a_node_without_pitr_holds_nothing) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        auto& db = e.local_db();
        BOOST_REQUIRE(!db.pitr_segment_closed_slot());
        BOOST_REQUIRE(db.commitlog());
        BOOST_REQUIRE_EQUAL(db.commitlog()->segment_hold_limit(), 0u);
    });
}

BOOST_AUTO_TEST_SUITE_END()
