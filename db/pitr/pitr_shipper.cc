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

#include "db/config.hh"

namespace db::pitr {

seastar::logger pitr_logger("pitr");

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
    if (!active()) {
        return;
    }
    _seal = std::move(seal);
    _count_nodes = std::move(count_nodes);
    _seal_timer.arm_periodic(_shipper_cfg->seal_interval);
}

void pitr_shipper::seal_tick() {
    // A tick during a running seal is dropped rather than queued: the seal in
    // flight already covers what the dropped tick would have sealed.
    if (!active() || _seal_running || _seal_gate.is_closed()) {
        return;
    }
    _seal_running = true;
    // Background: the timer callback cannot wait. The gate tracks the seal, so
    // stop() waits for it, and seal_cycle() never throws.
    (void)seal_cycle().finally([this, holder = _seal_gate.hold()] {
        _seal_running = false;
    });
}

future<> pitr_shipper::seal_cycle() {
    // Every cycle repeats the startup check, because a node can join later.
    const auto node_count = _count_nodes();
    if (node_count != 1) {
        _state = shipping_state::off_multi_node;
        _break_reason = fmt::format("the node counted {} nodes while shipping, and single-node PITR supports exactly one", node_count);
        _seal_timer.cancel();
        if (this_shard_id() == 0) {
            pitr_logger.error("Point-in-time recovery shipping is off: {}", *_break_reason);
        }
        co_return;
    }
    try {
        co_await _seal();
    } catch (...) {
        pitr_logger.warn("Sealing the commitlog segment failed, the next cycle will try again: {}", std::current_exception());
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

const shipper_config& pitr_shipper::shipping_config() const {
    if (!_shipper_cfg) {
        seastar::on_internal_error(pitr_logger, "shipping_config() queried while shipping is not configured");
    }
    return *_shipper_cfg;
}

} // namespace db::pitr
