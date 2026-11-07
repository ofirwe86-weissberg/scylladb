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
#include <cstdlib>
#include <ranges>
#include <filesystem>
#include <string>
#include <tuple>
#include <vector>

#include <seastar/core/format.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "db/object_storage_endpoint_param.hh"
#include "db/pitr/ledger_entry.hh"
#include "db/pitr/pitr_shipper.hh"
#include "replica/database.hh"
#include "replica/memtable.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/sstables_manager.hh"
#include "test/lib/cql_test_env.hh"
#include "utils/UUID_gen.hh"

BOOST_AUTO_TEST_SUITE(pitr_shipper_wiring_test)

namespace {

constexpr const char* pitr_test_endpoint = "https://pitr-store.test";
// What the wiring passes in, fixed so that a chain can be asserted on.
constexpr int64_t test_boot_epoch_us = 1762300000000000LL;

// `known_endpoint` decides whether the node was configured with the endpoint
// pitr_endpoint names. Nothing talks to it: building a client does no I/O.
cql_test_config pitr_config(bool known_endpoint = false) {
    auto cfg = cql_test_config();
    cfg.db_config->pitr_endpoint.set(pitr_test_endpoint);
    cfg.db_config->pitr_bucket.set("pitr-bucket");
    if (known_endpoint) {
        // Static credentials, so that building the client never reaches the AWS
        // credential chain: without them the client's teardown waits for the
        // instance-metadata lookups, about 50 s per test. Already set when the
        // S3 mock runs, so an existing value wins.
        ::setenv("AWS_ACCESS_KEY_ID", "pitr-test-key", 0);
        ::setenv("AWS_SECRET_ACCESS_KEY", "pitr-test-secret", 0);
        ::setenv("AWS_DEFAULT_REGION", "local", 0);
        cfg.db_config->object_storage_endpoints({db::object_storage_endpoint_param(db::object_storage_endpoint_param::s3_storage{
                .endpoint = pitr_test_endpoint,
                .region = "local",
                .iam_role_arn = "",
                .legacy_format = false,
        })});
    }
    return cfg;
}

// An active shipper on this shard, as the wiring leaves it just before
// start_shipping_on_shard(): the release hook is installed, nothing else is.
template <typename Func>
void with_an_active_shipper(cql_test_env& e, db::commitlog& log, Func&& f) {
    db::pitr::pitr_shipper shipper(e.local_db().get_config());
    shipper.start().get();
    shipper.note_cluster_size(1);
    BOOST_REQUIRE(shipper.active());
    auto stop = seastar::defer([&shipper]() noexcept {
        shipper.stop().get();
    });
    shipper.set_release_hook([&log](db::segment_id_type id) {
        log.release_segment(id);
    });
    f(shipper);
}

std::string file_name_of(const seastar::sstring& path) {
    return std::filesystem::path(std::string(path)).filename().string();
}

using closed_segments = std::vector<std::tuple<db::segment_id_type, uint64_t, bool>>;

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

    closed_segments closed;
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
    // Two closed segments, so that a held id is not always the first segment
    // the commitlog holds: a path lookup that ignores the id would answer with
    // the wrong one. A row lands on the shard its token belongs to and only
    // this shard's log is under test, so it takes as many rows as it takes,
    // whatever the shard count is; the seal does nothing on an unused segment.
    constexpr int max_rows = 500;
    int pk = 0;
    while (closed.size() < 2 && pk < max_rows) {
        e.execute_cql(seastar::format("insert into ks.cf (pk, v) values ({}, {})", pk, pk)).get();
        ++pk;
        log->force_new_active_segment().get();
    }
    BOOST_REQUIRE_EQUAL(closed.size(), 2u);
    // One more row for this shard, so that the segment being written is held
    // too, as it is on a node that takes writes: the hold starts with the
    // first write to the segment.
    while (log->held_segments().size() < closed.size() + 1 && pk < max_rows) {
        e.execute_cql(seastar::format("insert into ks.cf (pk, v) values ({}, {})", pk, pk)).get();
        ++pk;
    }
    BOOST_REQUIRE_EQUAL(log->held_segments().size(), closed.size() + 1);
    for (const auto& [id, used_size, failed] : closed) {
        BOOST_REQUIRE(!failed);
        BOOST_REQUIRE_GT(used_size, 0u);
    }
    f(*log, closed);
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
// through a handle of the shipper's own, for every held id and not just the
// first one the commitlog holds.
SEASTAR_TEST_CASE(test_the_hooks_open_the_held_segments_own_file) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                with_a_held_closed_segment(e, [&](db::commitlog& log, const closed_segments& closed) {
                    with_an_active_shipper(e, log, [&](db::pitr::pitr_shipper& shipper) {
                        auto hooks = db::pitr::make_segment_hooks(shipper, [&log](db::segment_id_type id) {
                            return log.held_segment_path(id);
                        });
                        for (const auto& [id, used_size, failed] : closed) {
                            const auto path = log.held_segment_path(id);
                            BOOST_REQUIRE(path);
                            // The name is what carries the id to a restore, so
                            // it has to be this id's own name.
                            BOOST_REQUIRE_MESSAGE(path->find(seastar::format("-{}.log", id)) != seastar::sstring::npos,
                                    seastar::format("segment {} answered with {}", id, *path));

                            auto f = hooks.open_segment(id).get();
                            auto close_f = seastar::defer([&f]() noexcept {
                                f.close().get();
                            });
                            BOOST_REQUIRE_GE(f.size().get(), used_size);

                            // The same bytes the file holds, through a handle
                            // this test opened itself: a hook pointing at
                            // another segment's file would differ here.
                            constexpr size_t read_size = 4096;
                            auto through_the_hook = f.dma_read_exactly<char>(0, read_size).get();
                            auto direct = seastar::open_file_dma(*path, seastar::open_flags::ro).get();
                            auto close_direct = seastar::defer([&direct]() noexcept {
                                direct.close().get();
                            });
                            auto expected = direct.dma_read_exactly<char>(0, read_size).get();
                            BOOST_REQUIRE_EQUAL(through_the_hook.size(), expected.size());
                            BOOST_REQUIRE(std::equal(through_the_hook.begin(), through_the_hook.end(), expected.begin()));
                        }
                        // An id this log does not hold has no file to ship,
                        // whether it is above every held id or below them: a
                        // lookup that answered with a neighbouring segment
                        // would ship another segment's bytes under this name.
                        BOOST_REQUIRE_THROW(hooks.open_segment(std::get<0>(closed.back()) + 1000).get(), std::runtime_error);
                        BOOST_REQUIRE(!log.held_segment_path(std::get<0>(closed.front()) - 1));
                        BOOST_REQUIRE_THROW(hooks.open_segment(std::get<0>(closed.front()) - 1).get(), std::runtime_error);

                        // A segment the log has released, but still has, has no
                        // path either: nothing keeps its file from being
                        // recycled under a reader any more.
                        const auto released = std::get<0>(closed.back());
                        log.release_segment(released);
                        BOOST_REQUIRE(!log.held_segment_path(released));
                        BOOST_REQUIRE_THROW(hooks.open_segment(released).get(), std::runtime_error);
                    });
                });
            },
            pitr_config());
}

// An endpoint the node was not configured with: shipping is off with a reason
// naming the option, no chain is started, and the caller carries on.
SEASTAR_TEST_CASE(test_an_unknown_endpoint_leaves_shipping_off) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                auto* log = e.local_db().commitlog();
                BOOST_REQUIRE(log);
                with_an_active_shipper(e, *log, [&](db::pitr::pitr_shipper& shipper) {
                    db::pitr::start_shipping_on_shard(shipper, e.get_sstorage_manager().local(), *log, utils::UUID_gen::get_time_UUID(), 1, [] {
                        return make_ready_future<>();
                    });

                    BOOST_REQUIRE(shipper.state() == db::pitr::shipping_state::off_unknown_endpoint);
                    BOOST_REQUIRE(!shipper.chain());
                    BOOST_REQUIRE(shipper.break_reason().has_value());
                    BOOST_REQUIRE(shipper.break_reason()->find("pitr_endpoint") != seastar::sstring::npos);
                    // The reason has to be the endpoint being unknown, not some
                    // other refusal that also names the option.
                    BOOST_REQUIRE(shipper.break_reason()->find("object_storage_endpoints") != seastar::sstring::npos);
                });
            },
            pitr_config(false));
}

// A configured endpoint: shipping starts with a chain of this shard's own, and
// a segment's key is that chain's, under the segment's real file name.
SEASTAR_TEST_CASE(test_a_known_endpoint_starts_shipping_with_a_chain) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                with_a_held_closed_segment(e, [&](db::commitlog& log, const closed_segments& closed) {
                    with_an_active_shipper(e, log, [&](db::pitr::pitr_shipper& shipper) {
                        const auto host_id = utils::UUID_gen::get_time_UUID();
                        int flushes = 0;
                        db::pitr::start_shipping_on_shard(shipper, e.get_sstorage_manager().local(), log, host_id, test_boot_epoch_us, [&flushes] {
                            ++flushes;
                            return make_ready_future<>();
                        });

                        BOOST_REQUIRE(shipper.active());
                        BOOST_REQUIRE(shipper.chain());
                        BOOST_REQUIRE_EQUAL(shipper.chain()->shard, seastar::this_shard_id());
                        BOOST_REQUIRE_EQUAL(shipper.chain()->prefix, e.local_db().get_config().pitr_prefix());
                        // The identity the wiring passed in, which the chain's
                        // key and every ledger entry are read against.
                        BOOST_REQUIRE_EQUAL(shipper.chain()->host_id, host_id);
                        BOOST_REQUIRE_EQUAL(shipper.chain()->boot_epoch_us, test_boot_epoch_us);
                        // The flush the start owes, run once and no more.
                        BOOST_REQUIRE_EQUAL(flushes, 1);

                        // The key is this chain's, keeping the commitlog's own
                        // file name rather than one a test made up.
                        const auto id = std::get<0>(closed.front());
                        const auto name = db::pitr::make_segment_hooks(shipper, [&log](db::segment_id_type id) {
                            return log.held_segment_path(id);
                        }).object_name_of(id);
                        const auto expected = db::pitr::segment_object_key(*shipper.chain(), file_name_of(*log.held_segment_path(id)));
                        BOOST_REQUIRE_EQUAL(std::string(name.object()), std::string(expected));
                        BOOST_REQUIRE(name.object().find(seastar::format("-{}.log", id)) != std::string_view::npos);
                    });
                });
            },
            pitr_config(true));
}

// The floor is the newest segment the commitlog holds when shipping starts, not
// the oldest: everything below it was written before the chain began.
SEASTAR_TEST_CASE(test_the_chain_floor_is_the_newest_held_segment) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                with_a_held_closed_segment(e, [&](db::commitlog& log, const closed_segments& closed) {
                    with_an_active_shipper(e, log, [&](db::pitr::pitr_shipper& shipper) {
                        const auto held = log.held_segments();
                        // The two that closed plus the one being written.
                        BOOST_REQUIRE_EQUAL(held.size(), 3u);
                        int flushes = 0;
                        db::pitr::start_shipping_on_shard(
                                shipper, e.get_sstorage_manager().local(), log, utils::UUID_gen::get_time_UUID(), test_boot_epoch_us, [&flushes] {
                                    ++flushes;
                                    return make_ready_future<>();
                                });

                        BOOST_REQUIRE_EQUAL(flushes, 1);
                        BOOST_REQUIRE(shipper.chain_floor());
                        // The highest id held, whatever order the holds were
                        // taken in: anything lower would let a segment that is
                        // not in the chain be shipped into it.
                        BOOST_REQUIRE_EQUAL(*shipper.chain_floor(), std::ranges::max(held));
                        for (auto id : held) {
                            BOOST_REQUIRE_LE(id, *shipper.chain_floor());
                        }
                        // Every segment that closed before the chain is below
                        // it, so each is released rather than shipped.
                        for (const auto& [id, used_size, failed] : closed) {
                            BOOST_REQUIRE_LT(id, *shipper.chain_floor());
                        }
                    });
                });
            },
            pitr_config(true));
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
