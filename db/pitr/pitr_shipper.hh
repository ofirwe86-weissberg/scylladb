/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <optional>
#include <string_view>
#include <unordered_map>

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include "db/commitlog/replay_position.hh"
#include "db/pitr/ledger_entry.hh"
#include "db/pitr/segment_upload.hh"
#include "seastarx.hh"

namespace db {

class config;

}

namespace service {

struct topology;

}

namespace sstables {

class object_name;
class object_storage_client;
class storage_manager;

} // namespace sstables

namespace db {

class commitlog;

}

namespace db::pitr {

extern seastar::logger pitr_logger;

// The interval is the floor of the recovery point objective, so refuse hours.
constexpr std::chrono::seconds max_seal_interval = std::chrono::hours(1);

// The nodes the cluster holds, as the topology state machine sees them: normal
// members, the ones a transition is moving, and the ones waiting to be joined.
// Nodes that have left are not counted (issue #7). The one count source, for
// the startup check and every seal cycle. Pure and constant-time; the caller
// must read the topology on shard 0, the only shard that loads it.
size_t count_cluster_nodes(const service::topology&) noexcept;

// Whether the node's user tables write to the commitlog, which is what the
// chain is made of. Mirrors database::make_keyspace_config(): an in-memory data
// store writes none either, and the commitlog object exists anyway (issue #7).
bool commitlog_disabled_for_user_tables(bool enable_commitlog, bool enable_in_memory_data_store) noexcept;

// Why the shipper is, or is not, shipping; reported as is to the user.
enum class shipping_state {
    // No PITR option is set, which is the default.
    off_not_configured,
    // Only one of the endpoint and the bucket is set.
    off_incomplete_config,
    // Another option is out of range.
    off_invalid_config,
    // Anything but exactly one node, which single-node PITR does not support.
    off_multi_node,
    // The node count could not be read in a seal cycle, so the chain can no
    // longer be shown to be single-node.
    off_node_count_unreadable,
    // The commitlog is disabled: nothing to seal, so refused at startup.
    off_commitlog_disabled,
    // pitr_endpoint has no object_storage_endpoints entry.
    off_unknown_endpoint,
    // It has one, but no store client could be built from it.
    off_store_client_failed,
    // An upload failed in a way no retry can fix, so the chain has a hole:
    // shipping stops for good and every hold is released (D25).
    off_upload_failed,
    // Waiting for the node's cluster membership to be counted.
    waiting_for_cluster_size,
    active,
};

std::string_view format_as(shipping_state) noexcept;

// The validated configuration shipping runs with. Built once at start: turning
// shipping on takes a restart.
struct shipper_config {
    sstring endpoint;
    sstring bucket;
    sstring prefix;
    std::chrono::seconds seal_interval;
    uint64_t unshipped_limit_bytes;
};

// Validates the PITR options, returning why shipping has to stay off. No I/O.
std::expected<shipper_config, shipping_state> make_shipper_config(const db::config&);

// What the wiring sets commitlog::config::hold_segments from: only a usable
// store with user tables writing to the commitlog holds segments. The hold rule
// the rest of this header refers to: the hold is fixed when the commitlog is
// built, long before shipping starts, so until it does every non-failed close
// is released at once, and a failed close releases nothing - the commitlog has
// dropped it itself (issue #8).
bool should_hold_segments(const db::config&);

// Drops one hold. Runs inside the close: must not block or throw.
using release_hook = std::function<void(db::segment_id_type)>;

// What only the wiring knows. Both required; the release hook is separate.
struct segment_hooks {
    // One handle per upload attempt, by segment_upload.hh's rules for `f`:
    // never reused by a retry, and never the commitlog's own.
    std::function<future<seastar::file>(db::segment_id_type)> open_segment;
    // Where a segment's bytes are stored, under its own file name.
    std::function<sstables::object_name(db::segment_id_type)> object_name_of;
};

// A held segment's id to its path, or nothing: held_segment_path()'s shape.
using segment_path_function = std::function<std::optional<sstring>(db::segment_id_type)>;

// What only the wiring knows about the chain this shard is about to start.
struct chain_origin {
    // Part of every object key of the chain.
    utils::UUID host_id;
    // One value per boot for the whole node, so every shard names the boot.
    int64_t boot_epoch_us = 0;
    // The newest segment held when shipping starts, from the ordered
    // commitlog::held_segments(). The chain's floor: below it a segment was
    // written before the chain began, so it is released, not shipped (D27).
    // Empty when nothing is held, which lets every later close ship.
    std::optional<db::segment_id_type> floor;
};

// Where the commitlog's close notification goes, one per shard. Not a member of
// the shipper, because it answers closes on both sides of the shipper's life:
// the wiring fills it once the shipper can take closes and clears it before the
// shipper stops. `release` must stay usable as long as the slot is.
class segment_closed_slot {
public:
    using handler = std::function<void(db::segment_id_type id, uint64_t used_size, bool failed)>;

private:
    release_hook _release;
    handler _handler;

public:
    explicit segment_closed_slot(release_hook release);

    void fill(handler h);
    void clear() noexcept;

    bool filled() const noexcept {
        return bool(_handler);
    }

    // Forwards, or applies the hold rule while there is no handler.
    void segment_closed(db::segment_id_type id, uint64_t used_size, bool failed);
};

// Ships sealed commitlog segments to object storage, one per shard. While
// shipping is on it runs the seal timer and uploads the segments whose close the
// commitlog reports, releasing each hold once its bytes are stored. While off it
// owns no timer and no client, so a node without PITR behaves as before.
class pitr_shipper final {
public:
    // Seals the active segment of the main commitlog only, skipping unused
    // ones and requesting no flush. A failure is recorded but not repeated: the
    // segment is gone from the active slot either way, and the next cycle seals
    // whatever is active then. A failed close arrives through segment_closed().
    using seal_function = seastar::noncopyable_function<future<>()>;
    // count_cluster_nodes()'s count. A future, because the real one is read on
    // shard 0; it runs every cycle, so it must be cheap. A count that could not
    // be established must fail rather than return 0: any count but one turns
    // shipping off for good, and so does a failure, except during stop().
    using node_count_function = seastar::noncopyable_function<future<size_t>()>;
    // Flushes this shard's user tables once. See flush_after_start().
    using flush_function = seastar::noncopyable_function<future<>()>;

    // How the latest seal cycle ended, for the status report to read.
    enum class cycle_outcome {
        none,
        // The seal function returned, nothing more: it covers a cycle that
        // sealed nothing, the log being idle or shut down. Whether a segment
        // closed is known only from the close notifications.
        sealed,
        // The count could not be read during stop(), where nothing was sealed.
        count_failed,
        // The seal failed. Recorded; the next cycle does not repeat it.
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
    bool _seal_running = false;
    bool _seal_again = false;
    seastar::condition_variable _cycle_done;
    std::optional<sstring> _break_reason;
    cycle_outcome _last_outcome = cycle_outcome::none;
    std::optional<sstring> _last_failure;
    // Held, not referenced: the client must outlive its sinks.
    seastar::shared_ptr<sstables::object_storage_client> _client;
    release_hook _release_hold;
    segment_hooks _hooks;
    std::optional<chain_key_parts> _chain;
    std::optional<db::segment_id_type> _chain_floor;
    segment_upload_limiter _uploads;
    seastar::gate _upload_gate;
    // One per upload in flight, owned by the upload's frame. Per segment,
    // because a rollback cancels the one it erased; stop() aborts them all.
    std::unordered_map<db::segment_id_type, abort_source*> _uploading;
    uint64_t _segments_shipped = 0;
    uint64_t _segments_unshipped = 0;

    future<> seal_loop();
    future<> seal_cycle();
    void break_shipping(shipping_state, sstring reason);
    // Uploads one segment's used bytes, then releases its hold. Reports its own
    // failures rather than throwing.
    future<> ship_segment(db::segment_id_type id, uint64_t used_size);
    // No success is recorded; the hold follows segment_closed()'s rule.
    void note_upload_aborted(db::segment_id_type id);

public:
    explicit pitr_shipper(const db::config& cfg) noexcept;

    pitr_shipper(const pitr_shipper&) = delete;
    pitr_shipper& operator=(const pitr_shipper&) = delete;

    // Reads the configuration and logs why shipping is off, if it is. Shipping
    // cannot begin here: the cluster size is not known yet. Ready future.
    future<> start();

    // Stops the seal timer, aborts every upload in flight and waits for the
    // seal and the uploads to unwind. The abort wins over an upload that has
    // just finished, so a segment aborted here keeps its hold; shutdown deletes
    // the held segments anyway. Safe without a preceding start().
    //
    // It waits as long as the store client takes to abandon a request it has
    // issued, which can be that client's own timeout: the S3 credential lookup
    // does not watch the abort source, and a shutdown during one measured about
    // 30 s per shard. Shutdown is not bounded by this unit.
    future<> stop();

    // Installs the hook that drops a hold, before the store client, as the hold
    // rule needs. It must outlive stop().
    void set_release_hook(release_hook hook);

    // Installs the store client and the hooks uploads run with, and needs the
    // release hook already installed. Takes effect only while shipping is
    // active and before stop(); a second call is ignored. A call that takes
    // effect is where this shard's chain begins: identity and floor come from
    // `origin` and stay as they are until the node restarts.
    void start_shipping(seastar::shared_ptr<sstables::object_storage_client> client, segment_hooks hooks, chain_origin origin);

    // Empty until start_shipping() has taken effect. #9 and the bound file
    // read it.
    const std::optional<chain_key_parts>& chain() const noexcept {
        return _chain;
    }

    // The lowest id this chain ships; see chain_origin::floor.
    std::optional<db::segment_id_type> chain_floor() const noexcept {
        return _chain_floor;
    }

    // The one flush the wiring owes once shipping has started: a table never
    // written again would keep a later backup's bound below this chain's start,
    // and that backup would be refused (D27). Runs in the background inside the
    // gate stop() waits for; a failure is logged and breaks nothing. Dropped
    // unless active and before stop().
    void flush_after_start(flush_function flush);

    // The commitlog's close notification for a held segment. It runs inside the
    // close, so it must not block: it starts the upload in the background.
    // `used_size` is the bytes a replay would read; `failed` means the segment
    // must not be shipped, and the commitlog has dropped its hold already.
    //
    // What each close does, in order:
    // - before start_shipping(), in every state, and once shipping has broken
    //   (D25), the hold rule applies; with no release hook it is ignored;
    // - `failed` for a segment being shipped is a rollback: that upload is
    //   aborted, so a file it may have erased is never recorded as shipped;
    // - below chain_floor() it is released, not shipped: it was written before
    //   this chain, and closes are not ordered by id (commitlog.hh);
    // - a repeat for a segment being shipped is ignored, so it cannot start a
    //   second upload or release the hold early by reporting a used size of 0;
    // - `used_size` 0 is released at once: an empty object would describe a
    //   segment that holds nothing. Any other hold goes once its bytes are
    //   stored;
    // - once stop() has begun it is released, and the chain ends at the last
    //   segment shipped. The drain's closes arrive before stop(), so stop()
    //   waits each out or aborts it, for as long as the client takes (see
    //   stop()); an aborted one keeps its hold.
    void segment_closed(db::segment_id_type id, uint64_t used_size, bool failed);

    // Arms the seal timer and does nothing else. Dropped, and not remembered,
    // unless shipping is already active and stop() has not begun, so it must
    // follow note_cluster_size(1); a second call is ignored, so the timer is
    // never armed twice. The timer only triggers seal_tick().
    void start_sealing(seal_function seal, node_count_function count_nodes);

    // One timer tick, which starts a seal cycle in the background. A tick during
    // a running cycle owes one more cycle rather than stacking. Does nothing
    // before start_sealing() or once stop() has begun. The cycles run in a gate
    // stop() waits for. Public so that tests can drive ticks.
    void seal_tick();

    // Resolves once no cycle is running or owed, or fails with
    // condition_variable_timed_out. For tests only, and not alongside stop(),
    // which does not resolve the condition.
    future<> wait_for_cycles(std::chrono::milliseconds timeout);

    bool sealing_scheduled() const noexcept {
        return _seal_timer.armed();
    }

    // Valid only while sealing_scheduled() holds.
    seastar::timer<>::time_point next_seal_time() const noexcept {
        return _seal_timer.get_timeout();
    }

    // Why shipping was turned off after it had begun.
    const std::optional<sstring>& break_reason() const noexcept {
        return _break_reason;
    }

    cycle_outcome last_outcome() const noexcept {
        return _last_outcome;
    }

    // Empty when the latest cycle succeeded.
    const std::optional<sstring>& last_failure() const noexcept {
        return _last_failure;
    }

    // Called once the node has joined, with count_cluster_nodes()'s count. The
    // startup check; the seal cycle repeats it, so a node added later is
    // noticed there. Anything but one node, zero included, refuses shipping
    // until the next restart.
    void note_cluster_size(size_t node_count);

    // Instead of note_cluster_size() when the node has no commitlog: a cycle
    // would seal nothing and still report active. Latched.
    void note_commitlog_disabled();

    // Called instead of start_shipping() when pitr_endpoint names an endpoint
    // with no object_storage_endpoints entry. Refused with a reason naming the
    // option, latched, and the seal timer dropped. Does nothing once shipping
    // is off for another reason, and never throws: a mistyped option must not
    // stop the node from booting.
    void note_endpoint_unknown();

    // The same for a configured endpoint whose client could not be built.
    void note_store_client_failed(std::string_view cause);

    shipping_state state() const noexcept {
        return _state;
    }

    bool active() const noexcept {
        return _state == shipping_state::active;
    }

    // True when the options describe a usable store, shipping or not.
    bool configured() const noexcept {
        return _shipper_cfg.has_value();
    }

    // Valid only while configured() holds.
    const shipper_config& shipping_config() const;

    // Bytes stored and hold released.
    uint64_t segments_shipped() const noexcept {
        return _segments_shipped;
    }

    // Segments that will not be shipped, which breaks the chain: a failed
    // close, or an upload that failed for good.
    uint64_t segments_unshipped() const noexcept {
        return _segments_unshipped;
    }

    // Started and not finished, the ones waiting for a slot included.
    size_t uploads_in_flight() const noexcept {
        return _uploading.size();
    }

    size_t max_concurrent_uploads() const noexcept {
        return _uploads.max_concurrent_uploads();
    }
};

// The hooks the wiring installs, built here rather than in main.cc so that the
// key a segment is stored under is under test. Both throw for an id the log no
// longer holds, which ends that one upload as any permanent failure does.
// `shipper` must be the one they are installed on, and must outlive them.
segment_hooks make_segment_hooks(const pitr_shipper& shipper, segment_path_function segment_path);

// Starts shipping on this shard, which is all the wiring at boot does: checks
// pitr_endpoint against the node's endpoints, takes the chain floor from the
// commitlog's held segments, installs the client and the hooks, and starts the
// flush. Does nothing unless shipping is active, and never throws: a client
// that cannot be built turns shipping off rather than failing the boot. `log`
// and the store manager must outlive `shipper`, which keeps using the log.
void start_shipping_on_shard(pitr_shipper& shipper, sstables::storage_manager& sstm, db::commitlog& log, utils::UUID host_id, int64_t boot_epoch_us,
        pitr_shipper::flush_function flush);

} // namespace db::pitr
