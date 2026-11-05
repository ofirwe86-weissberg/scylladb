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

#include <seastar/core/condition-variable.hh>
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
    // The number of nodes the cluster holds in any state, none and left
    // included. A future, because the real count is read on shard 0. It runs on
    // every seal cycle, so it must be cheap, and a failure of it is recorded
    // and retried next cycle, never taken as a count.
    using node_count_function = seastar::noncopyable_function<future<size_t>()>;

    // How the latest seal cycle ended, for the status report to read.
    enum class cycle_outcome {
        none,
        sealed,
        // The node count could not be read: nothing was sealed, and shipping
        // stays on.
        count_failed,
        seal_failed,
        // The count was not one: shipping is off, see break_reason().
        shipping_broke,
    };

private:
    const db::config& _cfg;
    shipping_state _state = shipping_state::off_not_configured;
    std::optional<shipper_config> _shipper_cfg;
    seal_function _seal;
    node_count_function _count_nodes;
    seastar::timer<> _seal_timer;
    seastar::gate _seal_gate;
    // True from the first tick of a run of cycles until the last one ends.
    bool _seal_running = false;
    // A tick came in while a seal ran: one more cycle follows, so that a write
    // that landed in the new segment is sealed within an interval of the tick.
    bool _seal_again = false;
    bool _slow_seal_warned = false;
    seastar::condition_variable _cycle_done;
    std::optional<sstring> _break_reason;
    cycle_outcome _last_outcome = cycle_outcome::none;
    std::optional<sstring> _last_failure;

    future<> seal_loop();
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

    // Arms the seal timer, if shipping is active and stop() has not begun:
    // otherwise does nothing, so a shipper that is off owns no timer. The timer
    // only triggers seal_tick(). Only the first call counts; later calls are
    // ignored, so the timer is never armed twice.
    void start_sealing(seal_function seal, node_count_function count_nodes);

    // One timer tick. Starts a seal cycle in the background. A tick during a
    // running cycle starts nothing but asks for one more cycle after it, so
    // ticks never stack: at most one follow-up runs. Does nothing before
    // start_sealing() and once stop() has begun, including when the cycle that
    // is running reaches its seal after that. The cycles are tracked by a gate,
    // which stop() waits for. Public so that tests can drive ticks without
    // waiting for the timer.
    void seal_tick();

    // Resolves once no cycle is running or owed, or fails with
    // condition_variable_timed_out after `timeout`.
    future<> wait_for_cycles(std::chrono::milliseconds timeout);

    bool sealing_scheduled() const noexcept {
        return _seal_timer.armed();
    }

    // When the next tick is due. Valid only while sealing_scheduled() holds;
    // it lets a test check the interval without waiting for it to pass.
    seastar::timer<>::time_point next_seal_time() const noexcept {
        return _seal_timer.get_timeout();
    }

    // Why shipping was turned off after it had begun. Empty while shipping is
    // on or was never on.
    const std::optional<sstring>& break_reason() const noexcept {
        return _break_reason;
    }

    cycle_outcome last_outcome() const noexcept {
        return _last_outcome;
    }

    // What went wrong in the latest cycle; empty when it succeeded.
    const std::optional<sstring>& last_failure() const noexcept {
        return _last_failure;
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
