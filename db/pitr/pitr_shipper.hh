/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <expected>
#include <optional>
#include <string_view>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include "seastarx.hh"

namespace db {

class config;

}

namespace db::pitr {

extern seastar::logger pitr_logger;

// Seal intervals above this are refused: the interval is the floor of the
// recovery point objective, so an interval of hours would promise protection
// that the shipped chain does not give.
constexpr std::chrono::seconds max_seal_interval = std::chrono::hours(1);

// Why the shipper is, or is not, shipping. Every reason is reported as is, so
// that the status report (phase 1.2) can tell the user what to change.
enum class shipping_state {
    // No PITR option is set: this is the default, and shipping does nothing.
    off_not_configured,
    // Only one of the endpoint and the bucket is set.
    off_incomplete_config,
    // The endpoint and bucket are set, but another option is out of range.
    off_invalid_config,
    // The node counted anything but exactly one node, at startup or in a seal
    // cycle, which single-node PITR does not support.
    off_multi_node,
    // Configured, waiting for the node's cluster membership to be counted.
    waiting_for_cluster_size,
    active,
};

std::string_view format_as(shipping_state) noexcept;

// The validated configuration shipping runs with. Built once at start, since
// none of the options is live-updatable: turning shipping on takes a restart.
struct shipper_config {
    sstring endpoint;
    sstring bucket;
    sstring prefix;
    std::chrono::seconds seal_interval;
    uint64_t unshipped_limit_bytes;
};

// Validates the PITR options. Returns the reason shipping has to stay off when
// they do not describe a usable store. Does no I/O and cannot block.
std::expected<shipper_config, shipping_state> make_shipper_config(const db::config&);

// Ships sealed commitlog segments to object storage, one instance per shard.
//
// It validates its configuration, reports why shipping is off, and while
// shipping is on runs the seal timer. The hold on segments, the uploads and the
// ledger come with the later tickets. While shipping is off it owns no timer
// and no store client, so a node that does not configure PITR behaves exactly
// as it did before.
class pitr_shipper final {
public:
    // Seals the active commitlog segment (force_new_active_segment on the main
    // commitlog only). Must skip unused segments and request no flush.
    using seal_function = seastar::noncopyable_function<future<>()>;
    // The number of nodes the local topology holds in any state, none and left
    // included. Must be constant work: it runs on every seal cycle.
    using node_count_function = seastar::noncopyable_function<size_t()>;

private:
    const db::config& _cfg;
    shipping_state _state = shipping_state::off_not_configured;
    std::optional<shipper_config> _shipper_cfg;
    seal_function _seal;
    node_count_function _count_nodes;
    seastar::timer<> _seal_timer;
    seastar::gate _seal_gate;
    bool _seal_running = false;
    std::optional<sstring> _break_reason;

    future<> seal_cycle();

public:
    explicit pitr_shipper(const db::config& cfg) noexcept;

    pitr_shipper(const pitr_shipper&) = delete;
    pitr_shipper& operator=(const pitr_shipper&) = delete;

    // Reads the configuration and logs why shipping is off, if it is. Shipping
    // does not begin here: the cluster size is not known yet, so the state ends
    // up at waiting_for_cluster_size at best. Returns a ready future.
    future<> start();

    // Stops the seal timer and waits for a seal that is running. Safe without a
    // preceding start().
    future<> stop();

    // Arms the seal timer, if shipping is active: otherwise does nothing, so a
    // shipper that is off owns no timer. The timer only triggers seal_tick().
    void start_sealing(seal_function seal, node_count_function count_nodes);

    // One timer tick. Starts a seal cycle in the background, unless one is
    // still running: a slow close makes ticks drop, not queue up. The cycle is
    // tracked by a gate, which stop() waits for. Public so that tests can drive
    // ticks without waiting for the timer.
    void seal_tick();

    bool sealing_scheduled() const noexcept {
        return _seal_timer.armed();
    }

    // Why shipping was turned off after it had begun, for the status report of
    // phase 1.2 to read. Empty while shipping is on or was never on.
    const std::optional<sstring>& break_reason() const noexcept {
        return _break_reason;
    }

    // Called on every shard once the node has joined, with the number of nodes
    // the local topology holds. This is the startup check; the seal cycle
    // repeats it, so a node added later is noticed there. Anything but
    // exactly one node, a count of zero included, refuses shipping until the
    // next restart; the refusal does not undo itself if the count later drops.
    void note_cluster_size(size_t node_count);

    shipping_state state() const noexcept {
        return _state;
    }

    bool active() const noexcept {
        return _state == shipping_state::active;
    }

    // True when the options describe a usable store, whether or not the cluster
    // size has allowed shipping to start.
    bool configured() const noexcept {
        return _shipper_cfg.has_value();
    }

    // Valid only while configured() holds.
    const shipper_config& shipping_config() const;
};

} // namespace db::pitr
