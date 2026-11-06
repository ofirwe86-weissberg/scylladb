/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/pitr_shipper.hh"

#include <fmt/format.h>

#include <seastar/core/on_internal_error.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/util/defer.hh>

#include "db/config.hh"
#include "service/topology_state_machine.hh"

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

future<> pitr_shipper::stop() {
    // The uploads of the later tickets are drained here too.
    _seal_timer.cancel();
    return _seal_gate.close();
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
