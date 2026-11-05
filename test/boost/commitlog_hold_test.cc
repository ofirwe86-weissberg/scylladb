/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the commitlog's point-in-time-recovery hold
// (commitlog::config::hold_segments): a held segment cannot be deleted until
// it is released. See https://github.com/ofirwe86-weissberg/scylladb/issues/6.

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <ranges>
#include <vector>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/noncopyable_function.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/rp_set.hh"
#include "test/lib/tmpdir.hh"
#include "utils/UUID_gen.hh"
#include "utils/log.hh"

using namespace db;
using namespace std::chrono_literals;

namespace {

table_id make_table_id() {
    return table_id(utils::UUID_gen::get_time_UUID());
}

// Runs `f` against a commitlog in its own directory, then shuts it down and
// deletes everything, as cl_test in commitlog_test.cc does.
future<> with_log(commitlog::config cfg, noncopyable_function<future<> (commitlog&)> f) {
    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();
    auto log = co_await commitlog::create_commitlog(std::move(cfg));
    std::exception_ptr ep;
    try {
        co_await f(log);
    } catch (...) {
        ep = std::current_exception();
    }
    try {
        co_await log.shutdown();
        co_await log.clear();
    } catch (...) {
        if (!ep) {
            ep = std::current_exception();
        }
    }
    if (ep) {
        std::rethrow_exception(ep);
    }
}

commitlog::config base_config() {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.commitlog_total_space_in_mb = 64 * this_smp_shard_count();
    cfg.use_o_dsync = false;
    return cfg;
}

commitlog::config hold_config() {
    auto cfg = base_config();
    cfg.hold_segments = true;
    return cfg;
}

future<rp_handle> write_some(commitlog& log, const table_id& id, size_t size = 128) {
    return log.add_mutation(id, size, commitlog::force_sync::no, [size] (commitlog::output& dst) {
        dst.fill('x', size);
    });
}

struct sealed_segment {
    segment_id_type id;
    sstring name;
};

// Writes one entry, seals the segment it went to and discards the data, so
// that only the hold can be keeping that segment.
future<sealed_segment> write_and_seal(commitlog& log, const table_id& uuid) {
    rp_set rps;
    auto h = co_await write_some(log, uuid);
    auto id = h.rp().id;
    rps.put(std::move(h));

    // The segment just written to is the newest one with data in it.
    auto names = log.get_active_segment_names();
    BOOST_REQUIRE(!names.empty());
    auto name = names.back();

    co_await log.force_new_active_segment();
    co_await log.sync_all_segments();
    log.discard_completed_segments(uuid, rps);
    co_await log.wait_for_pending_deletes();
    co_return sealed_segment{id, name};
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE(commitlog_hold_test)

// ---------------------------------------------------------------- the hold

// Without the hold - which is what hints and the schema commitlog do - a
// sealed, discarded segment leaves the active list, so the replay floor moves
// past it and it is on its way to deletion. That is today's behaviour.
SEASTAR_TEST_CASE(test_sealed_segment_is_released_without_the_hold) {
    co_await with_log(base_config(), [] (commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto sealed = co_await write_and_seal(log, uuid);

        BOOST_REQUIRE_GT(log.min_position().id, sealed.id);
        BOOST_REQUIRE(log.held_segments().empty());
    });
}

// With the hold, the same segment stays: still in the active list, so the
// replay floor stays with it, and still on disk, so it can be shipped.
SEASTAR_TEST_CASE(test_held_segment_is_kept_after_discard) {
    co_await with_log(hold_config(), [] (commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto sealed = co_await write_and_seal(log, uuid);

        BOOST_REQUIRE_EQUAL(log.min_position().id, sealed.id);
        BOOST_REQUIRE(co_await file_exists(sealed.name));

        auto held = log.held_segments();
        BOOST_REQUIRE(std::ranges::find(held, sealed.id) != held.end());
    });
}

// Releasing the hold hands the segment back to the normal delete path.
SEASTAR_TEST_CASE(test_released_segment_is_deleted) {
    co_await with_log(hold_config(), [] (commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto sealed = co_await write_and_seal(log, uuid);

        log.release_segment(sealed.id);
        co_await log.wait_for_pending_deletes();

        BOOST_REQUIRE_GT(log.min_position().id, sealed.id);
        auto held = log.held_segments();
        BOOST_REQUIRE(std::ranges::find(held, sealed.id) == held.end());
    });
}

// Releasing something that is not held is ignored, not a crash: a consumer
// may retry a release after a restart or a repeated notification.
SEASTAR_TEST_CASE(test_releasing_an_unheld_segment_is_ignored) {
    co_await with_log(hold_config(), [] (commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto sealed = co_await write_and_seal(log, uuid);

        log.release_segment(sealed.id);
        log.release_segment(sealed.id);
        log.release_segment(segment_id_type(987654));
        co_await log.wait_for_pending_deletes();
    });
}

// The hold queue is in creation order, whatever order releases come in.
SEASTAR_TEST_CASE(test_held_segments_are_in_creation_order) {
    co_await with_log(hold_config(), [] (commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto first = (co_await write_and_seal(log, uuid)).id;
        auto second = (co_await write_and_seal(log, uuid)).id;
        BOOST_REQUIRE_LT(first, second);

        BOOST_REQUIRE(std::ranges::is_sorted(log.held_segments()));

        // Release the newer one first; the older one stays held.
        log.release_segment(second);
        co_await log.wait_for_pending_deletes();
        auto after = log.held_segments();
        BOOST_REQUIRE(std::ranges::find(after, second) == after.end());
        BOOST_REQUIRE(std::ranges::find(after, first) != after.end());
        BOOST_REQUIRE(std::ranges::is_sorted(after));
    });
}

// The hold is per commitlog instance, which is what keeps hints and the schema
// commitlog out of it.
SEASTAR_TEST_CASE(test_hold_is_per_commitlog_instance) {
    auto plain_cfg = base_config();
    plain_cfg.fname_prefix = "SchemaLog-"; // as the schema commitlog does

    co_await with_log(hold_config(), [&plain_cfg] (commitlog& held) -> future<> {
        co_await with_log(plain_cfg, [&held] (commitlog& plain) -> future<> {
            auto uuid = make_table_id();

            auto plain_sealed = co_await write_and_seal(plain, uuid);
            BOOST_REQUIRE_GT(plain.min_position().id, plain_sealed.id);
            BOOST_REQUIRE(plain.held_segments().empty());

            auto held_sealed = co_await write_and_seal(held, uuid);
            BOOST_REQUIRE_EQUAL(held.min_position().id, held_sealed.id);
            BOOST_REQUIRE(!held.held_segments().empty());
        });
    });
}

// Shutdown does not wait for a release, and does not delete the held file
// either: the next boot replays it and deletes it without any hold. That is
// the break the design accepts, pinned down here so a change is noticed.
SEASTAR_TEST_CASE(test_shutdown_leaves_held_segment_for_the_next_boot) {
    auto cfg = hold_config();
    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();

    auto log = co_await commitlog::create_commitlog(cfg);
    auto uuid = make_table_id();
    auto sealed = co_await write_and_seal(log, uuid);
    auto name = sealed.name;
    BOOST_REQUIRE(!log.held_segments().empty());

    co_await log.shutdown();
    co_await log.clear();
    BOOST_REQUIRE(co_await file_exists(name));

    auto next_boot = co_await commitlog::create_commitlog(cfg);
    auto to_replay = co_await next_boot.get_segments_to_replay();
    BOOST_REQUIRE(std::ranges::find(to_replay, name) != to_replay.end());
    co_await next_boot.delete_segments(to_replay);
    BOOST_REQUIRE(!co_await file_exists(name));

    co_await next_boot.shutdown();
    co_await next_boot.clear();
}

// What the hold does when nothing releases it: the held segments fill the
// commitlog's disk limit and the write path waits for space. Writes are not
// refused, they wait - and they wait for as long as the hold lasts, which is
// why whoever holds has to release.
SEASTAR_TEST_CASE(test_hold_at_disk_limit_blocks_writes_until_release) {
    auto cfg = hold_config();
    cfg.commitlog_total_space_in_mb = 4 * this_smp_shard_count();
    cfg.allow_going_over_size_limit = false;

    co_await with_log(std::move(cfg), [] (commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto size = log.max_record_size() / 4;
        // The handles are dropped, so every segment is clean right away: the
        // hold is the only thing keeping the segments on disk.
        auto write = [&] () -> future<> {
            auto h = co_await log.add_mutation(uuid, size, db::timeout_clock::now() + 2s, commitlog::force_sync::no,
                    [size] (commitlog::output& dst) { dst.fill('x', size); });
            (void)h;
        };

        bool timed_out = false;
        for (size_t writes = 0; !timed_out && writes < 4096; ++writes) {
            try {
                co_await write();
            } catch (timed_out_error&) {
                timed_out = true;
            }
        }
        BOOST_REQUIRE_MESSAGE(timed_out, "the hold did not stop the write path at the disk limit");
        BOOST_REQUIRE(!log.held_segments().empty());

        // Releasing the hold gives the space back, and writes go on.
        for (auto id : log.held_segments()) {
            log.release_segment(id);
        }
        co_await log.wait_for_pending_deletes();
        co_await write();

        for (auto id : log.held_segments()) {
            log.release_segment(id);
        }
        log.discard_completed_segments(uuid);
        co_await log.wait_for_pending_deletes();
    });
}

BOOST_AUTO_TEST_SUITE_END()
