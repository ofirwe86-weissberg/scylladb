/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the commitlog's point-in-time-recovery hold
// (commitlog::config::hold_segments) and its segment-closed callback
// (commitlog::config::segment_closed). The consumer of the callback here is a
// fake recorder: nothing uploads anything yet. See issue #6.

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <optional>
#include <ranges>
#include <vector>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_extensions.hh"
#include "db/commitlog/rp_set.hh"
#include "db/extensions.hh"
#include "test/lib/tmpdir.hh"
#include "utils/UUID_gen.hh"
#include "utils/error_injection.hh"
#include "utils/checked-file-impl.hh"
#include "utils/log.hh"

using namespace db;
using namespace std::chrono_literals;

namespace {

table_id make_table_id() {
    return table_id(utils::UUID_gen::get_time_UUID());
}

// Runs `f` against a commitlog in its own directory, then shuts it down and
// deletes everything, as cl_test in commitlog_test.cc does.
future<> with_log(commitlog::config cfg, noncopyable_function<future<>(commitlog&)> f) {
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
    return log.add_mutation(id, size, commitlog::force_sync::no, [size](commitlog::output& dst) {
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

// The fake consumer of config::segment_closed. It only records, like the real
// shipper will only queue: the handler runs inside the close.
class close_recorder {
public:
    struct event {
        segment_id_type id;
        uint64_t used_size;
        bool failed;
    };

private:
    std::vector<event> _events;
    std::optional<std::pair<segment_id_type, bool>> _awaited;
    std::optional<promise<>> _waiter;

public:
    commitlog::config::segment_closed_handler handler() {
        return [this](segment_id_type id, uint64_t used_size, bool failed) {
            _events.push_back(event{id, used_size, failed});
            if (_awaited && *_awaited == std::pair(id, failed)) {
                _awaited.reset();
                std::exchange(_waiter, std::nullopt)->set_value();
            }
        };
    }

    const std::vector<event>& events() const {
        return _events;
    }

    std::vector<segment_id_type> ids() const {
        return _events | std::views::transform([](const event& e) {
            return e.id;
        }) | std::ranges::to<std::vector>();
    }

    std::optional<event> find(segment_id_type id, bool failed = false) const {
        auto i = std::ranges::find_if(_events, [&](const event& e) {
            return e.id == id && e.failed == failed;
        });
        return i == _events.end() ? std::nullopt : std::optional(*i);
    }

    bool has(segment_id_type id, bool failed = false) const {
        return find(id, failed).has_value();
    }

    // A natural close is not awaited by the write path, so a test that needs
    // its notification has to wait for it. The timeout turns a notification
    // that never comes into a failure rather than a hang.
    future<> wait_for(segment_id_type id, bool failed = false) {
        if (has(id, failed)) {
            co_return;
        }
        _awaited = std::pair(id, failed);
        _waiter.emplace();
        try {
            co_await with_timeout(lowres_clock::now() + 30s, _waiter->get_future());
        } catch (timed_out_error&) {
            _awaited.reset();
            _waiter.reset();
            BOOST_FAIL(fmt::format("no {} notification for segment {} within 30s", failed ? "failure" : "close", id));
        }
    }
};

commitlog::config recording_config(close_recorder& rec) {
    auto cfg = hold_config();
    cfg.segment_closed = rec.handler();
    return cfg;
}

// What the test arms on the commitlog's files: a flush that fails once, and a
// gate that holds everything on one named file until the test opens it, which
// is how a close is kept in flight for as long as the test needs - no clock,
// so no race between the test and a sleep.
struct file_controls {
    bool fail_flush = false;
    sstring held_file;
    std::optional<shared_promise<>> gate;

    void hold(sstring name) {
        held_file = std::move(name);
        gate.emplace();
    }
    void release() {
        if (gate) {
            std::exchange(gate, std::nullopt)->set_value();
        }
    }
};

class failing_file_impl : public checked_file_impl {
    file_controls* _ctl;
    sstring _name;

public:
    failing_file_impl(file f, file_controls* ctl, sstring name)
        : checked_file_impl([](std::exception_ptr) {}, std::move(f))
        , _ctl(ctl)
        , _name(std::move(name)) {
    }

    future<> maybe_delay() {
        if (_ctl->gate && _name == _ctl->held_file) {
            return _ctl->gate->get_shared_future();
        }
        return make_ready_future<>();
    }

    future<> flush() override {
        if (_ctl->fail_flush) {
            _ctl->fail_flush = false;
            throw std::runtime_error("injected flush failure");
        }
        co_await maybe_delay();
        co_await checked_file_impl::flush();
    }

    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent* intent) override {
        co_await maybe_delay();
        co_return co_await checked_file_impl::write_dma(pos, buffer, len, intent);
    }

    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        co_await maybe_delay();
        co_return co_await checked_file_impl::write_dma(pos, std::move(iov), intent);
    }
};

class failing_file_extension : public commitlog_file_extension {
    file_controls* _ctl;

public:
    explicit failing_file_extension(file_controls* ctl)
        : _ctl(ctl) {
    }

    future<file> wrap_file(const sstring& name, file f, open_flags) override {
        co_return seastar::make_shared<failing_file_impl>(std::move(f), _ctl, name);
    }
    future<> before_delete(const sstring&) override {
        return make_ready_future<>();
    }
    future<> before_rename(const sstring&, const sstring&) override {
        return make_ready_future<>();
    }
    future<> after_rename(const sstring&, const sstring&) override {
        return make_ready_future<>();
    }
};

} // anonymous namespace

BOOST_AUTO_TEST_SUITE(commitlog_hold_test)

// ---------------------------------------------------------------- the hold

// Without the hold - which is what hints and the schema commitlog do - a
// sealed, discarded segment leaves the active list, so the replay floor moves
// past it and it is on its way to deletion. That is today's behaviour.
SEASTAR_TEST_CASE(test_sealed_segment_is_released_without_the_hold) {
    co_await with_log(base_config(), [](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto sealed = co_await write_and_seal(log, uuid);

        BOOST_REQUIRE_GT(log.min_position().id, sealed.id);
        BOOST_REQUIRE(log.held_segments().empty());
    });
}

// With the hold, the same segment stays: still in the active list, so the
// replay floor stays with it, and still on disk, so it can be shipped.
SEASTAR_TEST_CASE(test_held_segment_is_kept_after_discard) {
    co_await with_log(hold_config(), [](commitlog& log) -> future<> {
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
    co_await with_log(hold_config(), [](commitlog& log) -> future<> {
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
    co_await with_log(hold_config(), [](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto sealed = co_await write_and_seal(log, uuid);

        log.release_segment(sealed.id);
        co_await log.wait_for_pending_deletes();
        auto held = log.held_segments();
        auto floor = log.min_position();

        // The second release of the same id, and one for an id this commitlog
        // never had, change nothing.
        log.release_segment(sealed.id);
        log.release_segment(segment_id_type(987654));
        co_await log.wait_for_pending_deletes();

        BOOST_REQUIRE(log.held_segments() == held);
        BOOST_REQUIRE(log.min_position() == floor);
    });
}

#ifdef SCYLLA_ENABLE_ERROR_INJECTION

// The oversized-write rollback erases a fresh segment from the active list
// directly, so it has to drop the hold with it: a hold on a deleted file
// would be a hole in the chain.
SEASTAR_TEST_CASE(test_rollback_of_a_fresh_segment_drops_the_hold) {
    auto cfg = hold_config();
    cfg.allow_fragmented_entries = true;

    co_await with_log(std::move(cfg), [](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        // Nothing has been written yet, so the segment the oversized write
        // creates is empty, and the rollback erases it rather than rewinding
        // it. Its hold has to go with it.
        BOOST_REQUIRE(log.held_segments().empty());

        utils::get_local_injector().enable("commitlog_oversized_fail", true);
        bool threw = false;
        try {
            co_await write_some(log, uuid, log.max_record_size() * 2);
        } catch (...) {
            threw = true;
        }
        utils::get_local_injector().disable("commitlog_oversized_fail");
        BOOST_REQUIRE(threw);
        BOOST_REQUIRE_GT(log.get_num_segments_created(), 0u);

        BOOST_REQUIRE(log.held_segments().empty());
    });
}

#endif // SCYLLA_ENABLE_ERROR_INJECTION

// The hold queue is in creation order, whatever order releases come in.
SEASTAR_TEST_CASE(test_held_segments_are_in_creation_order) {
    co_await with_log(hold_config(), [](commitlog& log) -> future<> {
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

    co_await with_log(hold_config(), [&plain_cfg](commitlog& held) -> future<> {
        co_await with_log(plain_cfg, [&held](commitlog& plain) -> future<> {
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

    co_await with_log(std::move(cfg), [](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto size = log.max_record_size() / 4;
        // The handles are dropped, so every segment is clean right away: the
        // hold is the only thing keeping the segments on disk.
        auto write = [&]() -> future<> {
            auto h = co_await log.add_mutation(uuid, size, db::timeout_clock::now() + 2s, commitlog::force_sync::no, [size](commitlog::output& dst) {
                dst.fill('x', size);
            });
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

// ------------------------------------------------------- the close callback

// The seal the shipper will use notifies, with a used size that covers
// everything written to the segment.
SEASTAR_TEST_CASE(test_forced_seal_notifies_with_used_size) {
    close_recorder rec;

    co_await with_log(recording_config(rec), [&rec](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto h = co_await write_some(log, uuid);
        auto id = h.rp().id;
        auto position = log.current_position();
        BOOST_REQUIRE_EQUAL(position.id, id);

        co_await log.force_new_active_segment();

        auto event = rec.find(id);
        BOOST_REQUIRE(event.has_value());
        BOOST_REQUIRE_GE(event->used_size, position.pos);
    });
}

// A natural close - the one the write path starts and does not await - must
// notify too, and the used size must not claim the file's slack.
SEASTAR_TEST_CASE(test_natural_close_notifies_with_used_size) {
    close_recorder rec;

    co_await with_log(recording_config(rec), [&rec](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto size = log.max_record_size() / 4;

        // Fill the first segment until the write path moves on to a new one.
        rp_set rps;
        sstring name;
        segment_id_type first = 0;
        position_type last_pos = 0;
        while (true) {
            auto h = co_await write_some(log, uuid, size);
            auto rp = h.rp();
            rps.put(std::move(h));
            if (first == 0) {
                first = rp.id;
                name = log.get_active_segment_names().back();
            }
            if (rp.id != first) {
                break;
            }
            last_pos = rp.pos;
        }

        co_await rec.wait_for(first);
        auto event = rec.find(first);
        BOOST_REQUIRE(event.has_value());
        BOOST_REQUIRE_GE(event->used_size, last_pos);
        BOOST_REQUIRE_LE(event->used_size, co_await file_size(name));
    });
}

// A close that fails must not be reported as shippable, and must not leave the
// segment held: the consumer is told, and the commitlog goes on as it would
// without the hold.
SEASTAR_TEST_CASE(test_failed_close_is_reported_as_failed_and_not_held) {
    close_recorder rec;
    file_controls ctl;
    db::extensions exts;
    exts.add_commitlog_file_extension("failing", std::make_unique<failing_file_extension>(&ctl));

    auto cfg = recording_config(rec);
    cfg.extensions = &exts;

    co_await with_log(std::move(cfg), [&rec, &ctl](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        rp_set rps;
        auto h = co_await write_some(log, uuid);
        auto id = h.rp().id;
        rps.put(std::move(h));

        ctl.fail_flush = true;
        bool threw = false;
        try {
            co_await log.force_new_active_segment();
        } catch (...) {
            threw = true;
        }
        BOOST_REQUIRE(threw);

        BOOST_REQUIRE(rec.has(id, true));
        BOOST_REQUIRE(!rec.has(id, false));

        auto held = log.held_segments();
        BOOST_REQUIRE(std::ranges::find(held, id) == held.end());

        log.discard_completed_segments(uuid, rps);
        co_await log.wait_for_pending_deletes();
    });
}

// held_bytes() is what a consumer watches against the limit, so it counts what
// the held segments take on disk - their whole files, not the bytes written -
// and a release takes a segment back out of it.
SEASTAR_TEST_CASE(test_held_bytes_counts_whole_held_files) {
    close_recorder rec;

    co_await with_log(recording_config(rec), [&rec](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        BOOST_REQUIRE_EQUAL(log.held_bytes(), 0u);

        auto first = co_await write_and_seal(log, uuid);
        auto first_size = co_await file_size(first.name);
        BOOST_REQUIRE_EQUAL(log.held_bytes(), first_size);
        // The file is bigger than what was written to it, which is the whole
        // point: that space is taken either way.
        BOOST_REQUIRE_GT(first_size, rec.find(first.id)->used_size);

        auto second = co_await write_and_seal(log, uuid);
        BOOST_REQUIRE_EQUAL(log.held_bytes(), first_size + co_await file_size(second.name));

        log.release_segment(first.id);
        co_await log.wait_for_pending_deletes();
        BOOST_REQUIRE_EQUAL(log.held_bytes(), co_await file_size(second.name));

        log.release_segment(second.id);
        co_await log.wait_for_pending_deletes();
        BOOST_REQUIRE_EQUAL(log.held_bytes(), 0u);
    });
}

#ifdef SCYLLA_ENABLE_ERROR_INJECTION

// A segment the consumer has been told about must not take writes again. The
// oversized-write rollback rewinds the segments it used, and one of them may
// have been closed and reported on the way, so the rollback must leave it
// closed: a later write going back into it would never be shipped.
SEASTAR_TEST_CASE(test_rollback_does_not_reopen_a_reported_segment) {
    close_recorder rec;
    auto cfg = recording_config(rec);
    cfg.allow_fragmented_entries = true;
    // Only on the first close: arming on every one would catch the close at
    // shutdown too, and leave the injection on for the next test in this
    // process.
    bool armed = false;
    cfg.segment_closed = [&rec, &armed](segment_id_type id, uint64_t used_size, bool failed) {
        rec.handler()(id, used_size, failed);
        // The oversized write has just closed a segment. Make its next
        // segment fail, so that the write rolls back over this one.
        if (!failed && !std::exchange(armed, true)) {
            utils::get_local_injector().enable("commitlog_oversized_fail", true);
        }
    };
    auto disarm = defer([]() noexcept {
        utils::get_local_injector().disable("commitlog_oversized_fail");
    });

    co_await with_log(std::move(cfg), [&rec](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto h = co_await write_some(log, uuid);
        auto first = h.rp().id;

        bool threw = false;
        try {
            co_await write_some(log, uuid, log.max_record_size() * 2);
        } catch (...) {
            threw = true;
        }
        utils::get_local_injector().disable("commitlog_oversized_fail");
        BOOST_REQUIRE(threw);
        BOOST_REQUIRE(rec.has(first));

        // The next write goes somewhere else, and the reported segment is
        // counted once.
        auto next = co_await write_some(log, uuid);
        BOOST_REQUIRE_NE(next.rp().id, first);
        BOOST_REQUIRE_EQUAL(std::ranges::count(rec.ids(), first), 1);
    });
}

// The rollback erases a segment the consumer was already told had closed, so
// the file the consumer was pointed at is about to go. It has to hear that the
// segment failed, or it would ship a deleted file and call the chain complete.
SEASTAR_TEST_CASE(test_rollback_reports_an_erased_reported_segment_as_failed) {
    close_recorder rec;
    auto cfg = recording_config(rec);
    cfg.allow_fragmented_entries = true;
    bool armed = false;
    cfg.segment_closed = [&rec, &armed](segment_id_type id, uint64_t used_size, bool failed) {
        rec.handler()(id, used_size, failed);
        // The oversized write has just filled and closed the segment it
        // started in, which was empty when it took it. Fail the next segment,
        // so that the rollback erases that first one rather than rewinding it.
        // Only the first close arms it, so that the close at shutdown does not
        // leave the injection on for the next test in this process.
        if (!failed && !std::exchange(armed, true)) {
            utils::get_local_injector().enable("commitlog_oversized_fail", true);
        }
    };
    auto disarm = defer([]() noexcept {
        utils::get_local_injector().disable("commitlog_oversized_fail");
    });

    // Nothing is written first, so the oversized write starts in an empty
    // segment - the case the rollback erases.
    co_await with_log(std::move(cfg), [&rec](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        bool threw = false;
        try {
            co_await write_some(log, uuid, log.max_record_size() * 2);
        } catch (...) {
            threw = true;
        }
        utils::get_local_injector().disable("commitlog_oversized_fail");
        BOOST_REQUIRE(threw);

        BOOST_REQUIRE(!rec.events().empty());
        auto closed = rec.events().front();
        BOOST_REQUIRE(!closed.failed);
        BOOST_REQUIRE(rec.has(closed.id, true));

        auto held = log.held_segments();
        BOOST_REQUIRE(std::ranges::find(held, closed.id) == held.end());
        co_await log.wait_for_pending_deletes();
        BOOST_REQUIRE_EQUAL(log.held_bytes(), 0u);
    });
}

#endif // SCYLLA_ENABLE_ERROR_INJECTION

// Once a segment's hold is gone, the consumer must hear nothing more about it:
// its file is on its way out. A close is in flight long before it reports, and
// the segment can lose its hold in between - here by a release, in the
// commitlog by an oversized rollback - so the close has to keep quiet.
SEASTAR_TEST_CASE(test_a_close_in_flight_is_silent_once_the_hold_is_gone) {
    close_recorder rec;

    co_await with_log(recording_config(rec), [&rec](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        segment_id_type id;
        sstring name;
        {
            // The write's handle holds the segment, so it goes out of scope
            // here: only the hold is to keep the segment after this.
            rp_set rps;
            auto h = co_await write_some(log, uuid);
            id = h.rp().id;
            name = log.get_active_segment_names().back();
            rps.put(std::move(h));
            log.discard_completed_segments(uuid, rps);
        }

        // The close is started and not awaited, so it is still in its own
        // I/O: the segment is marked closed and has not been reported yet.
        auto seal = log.force_new_active_segment();
        BOOST_REQUIRE(!seal.available());
        BOOST_REQUIRE(!rec.has(id));

        // The hold goes while that close is in flight.
        log.release_segment(id);

        co_await std::move(seal);
        co_await log.wait_for_pending_deletes();

        BOOST_REQUIRE(rec.events().empty());
        auto held = log.held_segments();
        BOOST_REQUIRE(std::ranges::find(held, id) == held.end());
        BOOST_REQUIRE_EQUAL(log.held_bytes(), 0u);
        BOOST_REQUIRE(!co_await file_exists(name));
    });
}

// Closes are not ordered: a slow close can be overtaken by a later one, and
// the consumer has to cope. The hold queue stays in creation order.
SEASTAR_TEST_CASE(test_closes_can_be_reported_out_of_order) {
    close_recorder rec;
    file_controls ctl;
    db::extensions exts;
    exts.add_commitlog_file_extension("slow", std::make_unique<failing_file_extension>(&ctl));

    auto cfg = recording_config(rec);
    cfg.extensions = &exts;

    co_await with_log(std::move(cfg), [&rec, &ctl](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto h = co_await write_some(log, uuid);
        auto first = h.rp().id;

        // Hold the first segment's close back, and do not wait for it.
        ctl.hold(log.get_active_segment_names().back());
        auto open_gate = defer([&ctl]() noexcept {
            ctl.release();
        });
        auto slow_seal = log.force_new_active_segment();

        auto h2 = co_await write_some(log, uuid);
        auto second = h2.rp().id;
        BOOST_REQUIRE_NE(first, second);
        co_await log.force_new_active_segment();

        // The later segment was reported first.
        co_await rec.wait_for(second);
        BOOST_REQUIRE(!rec.has(first));
        ctl.release();
        co_await std::move(slow_seal);
        co_await rec.wait_for(first);
        BOOST_REQUIRE_EQUAL(rec.ids().front(), second);

        BOOST_REQUIRE(std::ranges::is_sorted(log.held_segments()));
    });
}

// The data-lifetime check closes an active segment of its own accord. That
// close has to notify too, or a shipper would never hear of that segment.
SEASTAR_TEST_CASE(test_lifetime_close_notifies) {
    close_recorder rec;
    auto cfg = recording_config(rec);
    cfg.commitlog_sync_period_in_ms = 10;
    cfg.commitlog_data_max_lifetime_in_seconds = 1;

    co_await with_log(std::move(cfg), [&rec](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto h = co_await write_some(log, uuid);
        auto id = h.rp().id;

        // The lifetime check runs from the commitlog's own timer.
        co_await rec.wait_for(id);
        BOOST_REQUIRE(rec.has(id));
    });
}

// Shutdown closes the segment that is still being written, so the consumer
// hears about it before the node goes away.
SEASTAR_TEST_CASE(test_shutdown_notifies_the_open_segment) {
    close_recorder rec;
    auto cfg = recording_config(rec);
    tmpdir tmp;
    cfg.commit_log_location = tmp.path().string();

    auto log = co_await commitlog::create_commitlog(cfg);
    auto uuid = make_table_id();
    auto h = co_await write_some(log, uuid);
    auto id = h.rp().id;
    BOOST_REQUIRE(!rec.has(id));

    co_await log.shutdown();
    BOOST_REQUIRE(rec.has(id));

    co_await log.clear();
}

// A write whose own flush fails marks the segment closed on the spot, without
// going through close(). That segment can never be shipped, and nothing else
// will report it, so this path has to report it itself and let it go.
SEASTAR_TEST_CASE(test_batch_write_failure_reports_the_segment_failed_and_unheld) {
    close_recorder rec;
    file_controls ctl;
    db::extensions exts;
    exts.add_commitlog_file_extension("failing", std::make_unique<failing_file_extension>(&ctl));

    auto cfg = recording_config(rec);
    cfg.mode = commitlog::sync_mode::BATCH;
    cfg.extensions = &exts;

    co_await with_log(std::move(cfg), [&rec, &ctl](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto h = co_await write_some(log, uuid);
        auto id = h.rp().id;

        ctl.fail_flush = true;
        bool threw = false;
        try {
            auto h2 = co_await write_some(log, uuid);
        } catch (...) {
            threw = true;
        }
        BOOST_REQUIRE(threw);

        BOOST_REQUIRE(rec.has(id, true));
        auto held = log.held_segments();
        BOOST_REQUIRE(std::ranges::find(held, id) == held.end());
    });
}

// A close is reported with the hold off too - which is what the hold being per
// instance allows - once, and without holding or counting anything. Only the
// held-segment guarantees need the hold; a rollback reports nothing here.
SEASTAR_TEST_CASE(test_close_notifies_without_the_hold) {
    close_recorder rec;
    auto cfg = base_config();
    cfg.segment_closed = rec.handler();

    co_await with_log(std::move(cfg), [&rec](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        auto h = co_await write_some(log, uuid);
        auto id = h.rp().id;

        co_await log.force_new_active_segment();

        BOOST_REQUIRE(rec.has(id));
        BOOST_REQUIRE_EQUAL(std::ranges::count(rec.ids(), id), 1);
        BOOST_REQUIRE(log.held_segments().empty());
        BOOST_REQUIRE_EQUAL(log.held_bytes(), 0u);
    });
}

// And a failed close is reported as failed with the hold off too, so the skip
// that keeps an unheld segment quiet cannot swallow this one.
SEASTAR_TEST_CASE(test_failed_close_notifies_without_the_hold) {
    close_recorder rec;
    file_controls ctl;
    db::extensions exts;
    exts.add_commitlog_file_extension("failing", std::make_unique<failing_file_extension>(&ctl));

    auto cfg = base_config();
    cfg.segment_closed = rec.handler();
    cfg.extensions = &exts;

    co_await with_log(std::move(cfg), [&rec, &ctl](commitlog& log) -> future<> {
        auto uuid = make_table_id();
        rp_set rps;
        auto h = co_await write_some(log, uuid);
        auto id = h.rp().id;
        rps.put(std::move(h));

        ctl.fail_flush = true;
        bool threw = false;
        try {
            co_await log.force_new_active_segment();
        } catch (...) {
            threw = true;
        }
        BOOST_REQUIRE(threw);

        BOOST_REQUIRE(rec.has(id, true));
        BOOST_REQUIRE(!rec.has(id, false));

        log.discard_completed_segments(uuid, rps);
        co_await log.wait_for_pending_deletes();
    });
}

BOOST_AUTO_TEST_SUITE_END()
