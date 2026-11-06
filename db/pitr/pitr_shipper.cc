/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/pitr_shipper.hh"

#include <fmt/format.h>

#include <seastar/core/abort_source.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>

#include "db/config.hh"
#include "service/topology_state_machine.hh"
#include "sstables/object_storage_client.hh"

namespace db::pitr {

seastar::logger pitr_logger("pitr");

size_t count_cluster_nodes(const service::topology& topology) noexcept {
    // left_nodes and left_nodes_rs are left out on purpose: they are the nodes
    // the cluster no longer has.
    return topology.normal_nodes.size() + topology.transition_nodes.size() + topology.new_nodes.size();
}

bool commitlog_disabled_for_user_tables(bool enable_commitlog, bool enable_in_memory_data_store) noexcept {
    return !enable_commitlog || enable_in_memory_data_store;
}

std::string_view format_as(shipping_state state) noexcept {
    switch (state) {
    case shipping_state::off_not_configured:
        return "off_not_configured";
    case shipping_state::off_incomplete_config:
        return "off_incomplete_config";
    case shipping_state::off_invalid_config:
        return "off_invalid_config";
    case shipping_state::off_multi_node:
        return "off_multi_node";
    case shipping_state::off_node_count_unreadable:
        return "off_node_count_unreadable";
    case shipping_state::off_commitlog_disabled:
        return "off_commitlog_disabled";
    case shipping_state::off_upload_failed:
        return "off_upload_failed";
    case shipping_state::waiting_for_cluster_size:
        return "waiting_for_cluster_size";
    case shipping_state::active:
        return "active";
    }
    return "unknown";
}

std::expected<shipper_config, shipping_state> make_shipper_config(const db::config& cfg) {
    const auto& endpoint = cfg.pitr_endpoint();
    const auto& bucket = cfg.pitr_bucket();

    if (endpoint.empty() && bucket.empty()) {
        return std::unexpected(shipping_state::off_not_configured);
    }
    if (endpoint.empty() || bucket.empty()) {
        return std::unexpected(shipping_state::off_incomplete_config);
    }

    const auto seal_interval = std::chrono::seconds(cfg.pitr_seal_interval_in_seconds());
    if (seal_interval <= std::chrono::seconds(0) || seal_interval > max_seal_interval) {
        return std::unexpected(shipping_state::off_invalid_config);
    }

    const auto unshipped_limit_in_mb = uint64_t(cfg.pitr_unshipped_limit_in_mb());
    if (unshipped_limit_in_mb == 0) {
        return std::unexpected(shipping_state::off_invalid_config);
    }

    return shipper_config{
            .endpoint = endpoint,
            .bucket = bucket,
            .prefix = cfg.pitr_prefix(),
            .seal_interval = seal_interval,
            .unshipped_limit_bytes = unshipped_limit_in_mb << 20,
    };
}

bool should_hold_segments(const db::config& cfg) {
    return make_shipper_config(cfg).has_value() && !commitlog_disabled_for_user_tables(cfg.enable_commitlog(), cfg.enable_in_memory_data_store());
}

segment_closed_slot::segment_closed_slot(release_hook release)
    : _release(std::move(release)) {
    if (!_release) {
        seastar::on_internal_error(pitr_logger, "a segment_closed slot needs a release hook");
    }
}

void segment_closed_slot::fill(handler h) {
    if (!h) {
        seastar::on_internal_error(pitr_logger, "an empty handler cannot fill the segment_closed slot");
        return;
    }
    _handler = std::move(h);
}

void segment_closed_slot::clear() noexcept {
    _handler = nullptr;
}

void segment_closed_slot::segment_closed(db::segment_id_type id, uint64_t used_size, bool failed) {
    if (_handler) {
        _handler(id, used_size, failed);
        return;
    }
    if (failed) {
        // The commitlog has dropped the hold itself, so there is nothing to
        // release and nothing that could be shipped.
        return;
    }
    pitr_logger.debug("No consumer for the close of segment {}, releasing its hold", id);
    _release(id);
}

pitr_shipper::pitr_shipper(const db::config& cfg) noexcept
    : _cfg(cfg)
    , _seal_timer([this] {
        seal_tick();
    }) {
}

future<> pitr_shipper::start() {
    auto shipper_cfg = make_shipper_config(_cfg);
    // Only shard 0 logs, so that a reason is stated once per node.
    const auto log_this_shard = this_shard_id() == 0;

    if (!shipper_cfg) {
        _state = shipper_cfg.error();
        if (log_this_shard) {
            switch (_state) {
            case shipping_state::off_not_configured:
                pitr_logger.info("Point-in-time recovery shipping is off: pitr_endpoint and pitr_bucket are unset");
                break;
            case shipping_state::off_incomplete_config:
                pitr_logger.error("Point-in-time recovery shipping is off: both pitr_endpoint and pitr_bucket must be set, got endpoint={}, bucket={}",
                        _cfg.pitr_endpoint(), _cfg.pitr_bucket());
                break;
            case shipping_state::off_invalid_config:
                pitr_logger.error("Point-in-time recovery shipping is off: pitr_seal_interval_in_seconds must be between 1 and {}, "
                                  "and pitr_unshipped_limit_in_mb must be greater than 0, got {} and {}",
                        max_seal_interval.count(), _cfg.pitr_seal_interval_in_seconds(), _cfg.pitr_unshipped_limit_in_mb());
                break;
            default:
                seastar::on_internal_error(pitr_logger, fmt::format("unexpected PITR configuration failure: {}", _state));
            }
        }
        return make_ready_future<>();
    }

    _shipper_cfg = std::move(*shipper_cfg);
    _state = shipping_state::waiting_for_cluster_size;
    if (log_this_shard) {
        pitr_logger.info("Point-in-time recovery shipping is configured for endpoint={}, bucket={}, prefix={}, "
                         "seal interval={}s, unshipped limit={} bytes; waiting for the node's cluster membership",
                _shipper_cfg->endpoint, _shipper_cfg->bucket, _shipper_cfg->prefix, _shipper_cfg->seal_interval.count(), _shipper_cfg->unshipped_limit_bytes);
    }
    return make_ready_future<>();
}

// Neither a coroutine nor a continuation when there is nothing to wait for: a
// co_await on a ready future can still suspend when the reactor wants to
// preempt, and then() always schedules under SEASTAR_DEBUG, while stop() runs
// in the node's shutdown path and has to stay ready with no uploads in flight.
future<> pitr_shipper::stop() {
    _seal_timer.cancel();
    // Closing marks the gate at once, so no close reported from here on starts
    // an upload and nothing is added to _uploading while it is walked. An abort
    // only resolves the waiter's promise, which schedules its continuation
    // rather than running it, so no upload erases its entry under this loop -
    // which is also why the loop cannot yield. Each iteration is O(1), and
    // there is one per upload in flight.
    auto uploads_closed = _upload_gate.close();
    for (auto& [id, as] : _uploading) {
        pitr_logger.debug("Aborting the upload of segment {} for shutdown", id);
        as->request_abort();
    }
    // The limiter outlives every call it started, waiters included, because
    // every one of them unwinds through the upload gate.
    if (uploads_closed.available() && !uploads_closed.failed()) {
        // The common case: nothing was in flight, so both gates are resolved
        // already. Returning one of them keeps stop() ready in every build
        // mode, which a discard_result() would not: it goes through then(),
        // and then() always schedules under SEASTAR_DEBUG
        // (seastar/core/future.hh:1727-1735).
        return _seal_gate.close();
    }
    return when_all_succeed(_seal_gate.close(), std::move(uploads_closed)).discard_result();
}

void pitr_shipper::set_release_hook(release_hook hook) {
    if (!hook) {
        seastar::on_internal_error(pitr_logger, "set_release_hook() needs a hook");
        return;
    }
    _release_hold = std::move(hook);
}

void pitr_shipper::start_shipping(seastar::shared_ptr<sstables::object_storage_client> client, segment_hooks hooks) {
    if (!active() || _client || _upload_gate.is_closed()) {
        return;
    }
    if (!client || !hooks.open_segment || !hooks.object_name_of || !_release_hold) {
        seastar::on_internal_error(pitr_logger, "start_shipping() needs a store client, both hooks and the release hook");
        return;
    }
    _client = std::move(client);
    _hooks = std::move(hooks);
}

void pitr_shipper::segment_closed(db::segment_id_type id, uint64_t used_size, bool failed) {
    if (!_client) {
        // start_shipping() has not run, so nothing can be shipped yet. The
        // hold, though, was taken when the commitlog was built: keeping it
        // would pin the segment until the commitlog's disk filled up.
        if (!failed && _release_hold) {
            pitr_logger.debug("Not shipping segment {}: shipping has not started ({}), releasing its hold", id, _state);
            _release_hold(id);
        }
        return;
    }
    if (!active()) {
        // Shipping has broken: nothing is uploaded, and the hold goes at once
        // rather than pinning the segment for the rest of the node's life. A
        // failed close has no hold left to release.
        if (!failed) {
            pitr_logger.debug("Not shipping segment {}: shipping is off ({}), releasing its hold", id, _state);
            _release_hold(id);
        }
        return;
    }
    if (failed) {
        // A failed close or a rollback: the bytes are not shippable and the
        // commitlog has dropped the hold itself. The chain this breaks is the
        // ledger's, which #9 and #11 own.
        ++_segments_unshipped;
        if (auto it = _uploading.find(id); it != _uploading.end()) {
            // A rollback of a segment already reported as closed: the file may
            // be gone, so the upload must not record a success for it.
            it->second->request_abort();
        }
        pitr_logger.warn("Segment {} will not be shipped: the commitlog reported its close as failed", id);
        return;
    }
    // Before the used size is looked at: a repeated close can report a size of
    // 0 for a segment whose upload is in flight, and releasing its hold then
    // would let the file be recycled under the upload.
    if (_uploading.contains(id)) {
        pitr_logger.debug("Segment {} is already being shipped, ignoring the repeated close", id);
        return;
    }
    if (used_size == 0) {
        // A forced seal leaves segments with nothing in them, and an empty
        // object would describe a segment that holds no writes.
        pitr_logger.debug("Segment {} closed with no used bytes, releasing its hold", id);
        _release_hold(id);
        return;
    }
    if (_upload_gate.is_closed()) {
        pitr_logger.debug("Not shipping segment {}: shutdown has begun", id);
        return;
    }
    // Background: the close this runs inside cannot wait. The gate is what
    // stop() waits for, and every continuation below runs inside it.
    (void)seastar::try_with_gate(_upload_gate, [this, id, used_size] {
        return ship_segment(id, used_size);
    }).handle_exception([id](std::exception_ptr ex) {
        // ship_segment() reports its own failures, so only a release hook that
        // broke its contract, or a closed gate, can land here.
        pitr_logger.error("Shipping segment {} ended unexpectedly: {}", id, ex);
    });
}

future<> pitr_shipper::ship_segment(db::segment_id_type id, uint64_t used_size) {
    abort_source as;
    _uploading.emplace(id, &as);
    auto forget = seastar::defer([this, id]() noexcept {
        _uploading.erase(id);
    });
    retry_policy policy{
            .on_attempt_failed =
                    [id](std::exception_ptr ex, std::chrono::milliseconds wait) {
                        pitr_logger.debug("Upload of segment {} failed, retrying in {}: {}", id, wait, ex);
                    },
    };
    try {
        co_await _uploads.upload(
                *_client, _hooks.object_name_of(id), used_size,
                [this, id] {
                    return _hooks.open_segment(id);
                },
                std::move(policy), as);
    } catch (const seastar::abort_requested_exception&) {
        // The abort wins over an attempt that had already finished, so nothing
        // records this segment. sleep_aborted, which a plain request_abort()
        // during the backoff gives, is one of these.
        note_upload_aborted(id);
        co_return;
    } catch (...) {
        // Everything that is not an abort: a missing segment file, a segment
        // shorter than its used size, and anything else no retry could fix.
        // The chain now has a hole in it, so shipping breaks and the hold goes.
        // Provisional, pending the decision on the outage policy (#11).
        ++_segments_unshipped;
        auto ex = std::current_exception();
        break_shipping(shipping_state::off_upload_failed,
                fmt::format(
                        "upload_failed: uploading segment {} failed in a way no retry can fix ({}). Fix the cause and restart the node to ship again", id, ex));
        _release_hold(id);
        co_return;
    }
    // The abort can land in the gap between the upload returning and this
    // resumption, which is two task hops wide, so it is checked once more here
    // as segment_upload.hh requires: a rollback that arrives in that gap has
    // erased the file, and recording it as shipped would point the chain at
    // nothing. Nothing is awaited from here on, so this is the last gap.
    if (as.abort_requested()) {
        note_upload_aborted(id);
        co_return;
    }
    ++_segments_shipped;
    _release_hold(id);
    pitr_logger.debug("Shipped segment {} ({} bytes) and released its hold", id, used_size);
}

// The hold is kept for a shutdown, which deletes the held segments anyway, and
// for a rollback, where the commitlog has dropped it already; a break releases
// it, since no chain is left to hold it for.
void pitr_shipper::note_upload_aborted(db::segment_id_type id) {
    if (!active() && !_upload_gate.is_closed()) {
        pitr_logger.info("Upload of segment {} was aborted by the break, releasing its hold", id);
        _release_hold(id);
        return;
    }
    pitr_logger.info("Upload of segment {} was aborted, its hold is kept", id);
}

void pitr_shipper::start_sealing(seal_function seal, node_count_function count_nodes) {
    // A closed gate means stop() has begun: arming the timer then would leave a
    // tick behind that no stop() waits for.
    if (!active() || _seal || _seal_gate.is_closed()) {
        return;
    }
    _seal = std::move(seal);
    _count_nodes = std::move(count_nodes);
    _seal_timer.arm_periodic(_shipper_cfg->seal_interval);
}

void pitr_shipper::seal_tick() {
    if (!active() || !_seal) {
        return;
    }
    if (_seal_running) {
        // Dropping it would leave a write that lands in the next segment
        // waiting two intervals, so one follow-up cycle is owed instead.
        _seal_again = true;
        pitr_logger.debug("Seal tick during a running cycle, one follow-up is owed");
        return;
    }
    if (_seal_gate.is_closed()) {
        // stop() has begun: no cycle may start, and none is owed.
        return;
    }
    // Background: the timer callback cannot wait. The gate makes stop() wait,
    // and every continuation that touches this shipper runs inside it.
    _seal_running = true;
    (void)seastar::try_with_gate(_seal_gate, [this] {
        return seal_loop().then_wrapped([this](future<> f) {
            // Released on every path, including one where the reporting below
            // throws under memory pressure: a run that does not release the
            // flag would stop sealing on this shard for good.
            auto release = seastar::defer([this]() noexcept {
                _seal_again = false;
                _seal_running = false;
                _cycle_done.broadcast();
            });
            if (f.failed()) {
                // seal_cycle() records its own failures, so nothing is expected
                // here.
                _last_outcome = cycle_outcome::seal_failed;
                _last_failure = fmt::format("the seal cycle failed unexpectedly: {}", f.get_exception());
                pitr_logger.error("{}", *_last_failure);
            }
        });
    }).handle_exception_type([](const seastar::gate_closed_exception&) {
        // Unreachable: the gate was open just above, with no yield in between.
        // Captures nothing, so it cannot outlive the shipper.
    });
}

future<> pitr_shipper::seal_loop() {
    do {
        _seal_again = false;
        co_await seal_cycle();
    } while (_seal_again && active() && !_seal_gate.is_closed());
}

future<> pitr_shipper::wait_for_cycles(std::chrono::milliseconds timeout) {
    return _cycle_done.wait(timeout, [this] {
        return !_seal_running;
    });
}

// Turns shipping off for good and records why, for the status report to read.
void pitr_shipper::break_shipping(shipping_state state, sstring reason) {
    _state = state;
    _break_reason = std::move(reason);
    _last_outcome = cycle_outcome::shipping_broke;
    _last_failure = _break_reason;
    _seal_timer.cancel();
    // No chain is left to keep segments for, so every upload in flight is
    // aborted and its hold released by ship_segment()'s abort path, which sees
    // that shipping is off. O(1) per upload and no yield, so nothing erases its
    // entry under this loop.
    for (auto& [id, as] : _uploading) {
        pitr_logger.debug("Aborting the upload of segment {}: shipping broke", id);
        as->request_abort();
    }
    // Every shard keeps its own latch, so the shard that breaks is the one that
    // has to say so: a break happens at most once per shard per boot.
    pitr_logger.error("Point-in-time recovery shipping is off on shard {}: {}", this_shard_id(), *_break_reason);
}

// Records every failure instead of throwing, so seal_loop() always finishes.
future<> pitr_shipper::seal_cycle() {
    // Every cycle repeats the startup check, because a node can join later.
    size_t node_count = 0;
    try {
        node_count = co_await _count_nodes();
    } catch (...) {
        _last_failure = fmt::format("reading the node count failed: {}", std::current_exception());
        if (_seal_gate.is_closed()) {
            // stop() has begun: a count that fails on the way out says nothing
            // about the chain, and there is no next cycle to break.
            _last_outcome = cycle_outcome::count_failed;
            co_return;
        }
        // A count nobody can read is not a count of one, so the chain can no
        // longer be shown to come from a single node.
        break_shipping(shipping_state::off_node_count_unreadable,
                fmt::format("node_count_unreadable: the node count could not be read, so the shipped chain can no longer be shown to come from a single node "
                            "({}). Fix the cause and restart the node to ship again",
                        *_last_failure));
        co_return;
    }
    // Reading the count yields, so stop() may have begun in the meantime. In
    // the node's shutdown order the commitlog is drained and shut down before
    // this service stops, so a seal started now would seal nothing at best.
    if (_seal_gate.is_closed()) {
        co_return;
    }
    if (node_count != 1) {
        // TODO(#7, PM-52): a join can move ranges well inside one seal
        // interval, so the break has to be stamped with the join's own time
        // from topology rather than the time this cycle noticed it. Pending the
        // decision on PM-52.
        break_shipping(
                shipping_state::off_multi_node, fmt::format("the node counted {} nodes while shipping, and single-node PITR supports exactly one", node_count));
        co_return;
    }
    try {
        co_await _seal();
        _last_outcome = cycle_outcome::sealed;
        _last_failure.reset();
    } catch (...) {
        _last_outcome = cycle_outcome::seal_failed;
        _last_failure = fmt::format("sealing the commitlog segment failed: {}", std::current_exception());
        static thread_local seastar::logger::rate_limit seal_failed_rate_limit(std::chrono::seconds(30));
        pitr_logger.log(seastar::log_level::warn, seal_failed_rate_limit,
                "{}; shipping stays on, and the next cycle seals whatever is active then rather than retrying this segment", *_last_failure);
    }
}

void pitr_shipper::note_cluster_size(size_t node_count) {
    if (_state != shipping_state::waiting_for_cluster_size) {
        // Either the configuration already refused shipping, or the count has
        // already been seen. Both are latched: shipping never turns itself on.
        return;
    }
    // Anything but exactly one node refuses, including a count of zero: a
    // count the caller could not establish must not read as "single node".
    if (node_count != 1) {
        _state = shipping_state::off_multi_node;
        if (this_shard_id() == 0) {
            pitr_logger.error("Point-in-time recovery shipping is off: it supports a single node only, and this node counted {} nodes. "
                              "Unset pitr_endpoint and pitr_bucket to silence this message",
                    node_count);
        }
        return;
    }
    _state = shipping_state::active;
    if (this_shard_id() == 0) {
        pitr_logger.info("Point-in-time recovery shipping is on");
    }
}

void pitr_shipper::note_commitlog_disabled() {
    if (_state != shipping_state::waiting_for_cluster_size) {
        return;
    }
    _state = shipping_state::off_commitlog_disabled;
    // The reason text is written for the operator. The startup off_multi_node
    // refusal has none yet; #14 makes the two consistent.
    _break_reason = "commitlog_disabled: the commitlog is disabled, so there is nothing to ship. Enable the commitlog and restart the node to ship again";
    if (this_shard_id() == 0) {
        pitr_logger.error("Point-in-time recovery shipping is off: {}", *_break_reason);
    }
}

const shipper_config& pitr_shipper::shipping_config() const {
    if (!_shipper_cfg) {
        seastar::on_internal_error(pitr_logger, "shipping_config() queried while shipping is not configured");
    }
    return *_shipper_cfg;
}

} // namespace db::pitr
