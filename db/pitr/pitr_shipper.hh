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

} // namespace sstables

namespace db::pitr {

extern seastar::logger pitr_logger;

// Seal intervals above this are refused: the interval is the floor of the
// recovery point objective, so an interval of hours would promise protection
// that the shipped chain does not give.
constexpr std::chrono::seconds max_seal_interval = std::chrono::hours(1);

// Counts the nodes the cluster holds, as the topology state machine sees them:
// the normal members, the ones a topology transition is moving, and the ones
// waiting to be joined, which together cover a joining or zero-token node.
// Nodes that have left are not counted, so a node whose only peer was removed
// counts as one again (issue #7).
//
// This is the one count source: the startup check and every seal cycle use it,
// or a node that once had a peer would pass startup and break shipping later.
// Pure and constant-time, so it cannot stall, and the caller must read the
// topology on shard 0, which is the only shard that loads it.
size_t count_cluster_nodes(const service::topology&) noexcept;

// Whether the node's user tables write to the commitlog, which is what the
// shipped chain is made of. It mirrors the per-keyspace decision in
// database::make_keyspace_config(): an in-memory data store writes no commitlog
// either, whatever enable_commitlog says. The commitlog object itself exists
// even then, so it cannot answer this (issue #7).
bool commitlog_disabled_for_user_tables(bool enable_commitlog, bool enable_in_memory_data_store) noexcept;

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
    // The node count could not be read in a seal cycle, so the chain can no
    // longer be shown to be single-node. Reported to the user as
    // node_count_unreadable.
    off_node_count_unreadable,
    // The commitlog is disabled, so there is nothing to seal and nothing to
    // ship. Refused at startup rather than reported as shipping (P32's pattern
    // for an encrypted commitlog).
    off_commitlog_disabled,
    // A segment's upload failed in a way no retry can fix, so the chain has a
    // hole in it. Provisional, pending the decision on the outage policy (#11).
    off_upload_failed,
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

// Everything shipping a segment needs that only the wiring knows, so that this
// unit needs no commitlog of its own. All three are required together.
struct segment_hooks {
    // Opens a handle dedicated to one upload attempt, by the rules stated for
    // `f` in segment_upload.hh. Called once per attempt, so a retry never
    // reuses a handle, and never the commitlog's own.
    std::function<future<seastar::file>(db::segment_id_type)> open_segment;
    // Where a segment's bytes are stored. Must keep the segment's own file
    // name, which a restore matches against the prefix.
    std::function<sstables::object_name(db::segment_id_type)> object_name_of;
    // Releases the commitlog's hold on a segment whose close was reported.
    // Called from a continuation, so it must neither block nor throw. It must
    // also tolerate a segment the commitlog has already dropped, which a break
    // can reach after a rollback reported it: commitlog::release_segment() of
    // an id that is not held is a no-op.
    std::function<void(db::segment_id_type)> release_segment;
};

// Ships sealed commitlog segments to object storage, one instance per shard.
//
// It validates its configuration, reports why shipping is off, and while
// shipping is on runs the seal timer and uploads the segments whose close the
// commitlog reports, releasing each hold once its bytes are stored. The ledger
// comes with a later ticket. While shipping is off it owns no timer and no
// store client, so a node that does not configure PITR behaves exactly as it
// did before.
class pitr_shipper final {
public:
    // Seals the active commitlog segment (force_new_active_segment on the main
    // commitlog only). Must skip unused segments and request no flush. A
    // failure of it is recorded but not repeated: force_new_active_segment()
    // marks the segment closed before it syncs, so the segment this cycle
    // failed on is gone from the active slot either way, and the next cycle
    // seals whatever is active then. A close that failed reaches the shipper
    // through the commitlog's close notification (segment_closed with
    // failed=true), which is where an unshippable segment is dealt with.
    using seal_function = seastar::noncopyable_function<future<>()>;
    // The number of nodes the cluster holds, as count_cluster_nodes() counts
    // them. A future, because the real count is read on shard 0. It runs on
    // every seal cycle, so it must be cheap. A count that could not be
    // established must fail the future: returning 0 is not "unknown", it is a
    // count, and any count but one turns shipping off for good. A failed future
    // turns it off too, except during stop().
    using node_count_function = seastar::noncopyable_function<future<size_t>()>;

    // How the latest seal cycle ended, for the status report to read.
    enum class cycle_outcome {
        none,
        // The seal function returned, and nothing more than that: it also
        // covers a cycle that sealed nothing, because the log was idle, or
        // already shut down by the drain that runs before this service stops,
        // or because the previous cycle's close failed and left the segment
        // marked closed. Whether a segment was really closed is known only
        // from the commitlog's close notifications.
        sealed,
        // The node count could not be read during stop(), where it breaks
        // nothing: nothing was sealed.
        count_failed,
        // The seal function failed. Recorded for the status report; the next
        // cycle does not repeat it, and a failed close is reported through
        // segment_closed(failed=true) instead.
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
    seastar::condition_variable _cycle_done;
    std::optional<sstring> _break_reason;
    cycle_outcome _last_outcome = cycle_outcome::none;
    std::optional<sstring> _last_failure;
    // Held, not referenced: an upload outlives the caller that handed the
    // client over, and the store client must outlive its sinks.
    seastar::shared_ptr<sstables::object_storage_client> _client;
    segment_hooks _hooks;
    segment_upload_limiter _uploads;
    seastar::gate _upload_gate;
    // One abort source per upload in flight, owned by the upload's own frame.
    // Per segment, because a rollback cancels the upload of the one segment it
    // erased while the rest keep going, as the outage latch (#11) will too;
    // stop() and a break abort them all.
    std::unordered_map<db::segment_id_type, abort_source*> _uploading;
    uint64_t _segments_shipped = 0;
    uint64_t _segments_unshipped = 0;

    future<> seal_loop();
    future<> seal_cycle();
    void break_shipping(shipping_state, sstring reason);
    // Uploads one closed segment's used bytes and then releases its hold.
    // Reports its own failures rather than throwing.
    future<> ship_segment(db::segment_id_type id, uint64_t used_size);
    // What an aborted upload leaves behind: no success is recorded, and the
    // hold is kept or released by the rule stated for segment_closed().
    void note_upload_aborted(db::segment_id_type id);

public:
    explicit pitr_shipper(const db::config& cfg) noexcept;

    pitr_shipper(const pitr_shipper&) = delete;
    pitr_shipper& operator=(const pitr_shipper&) = delete;

    // Reads the configuration and logs why shipping is off, if it is. Shipping
    // does not begin here: the cluster size is not known yet, so the state ends
    // up at waiting_for_cluster_size at best. Returns a ready future.
    future<> start();

    // Stops the seal timer, aborts every upload in flight and waits for the
    // seal and the uploads to unwind. The abort wins over an upload that has
    // just finished, so a segment aborted here keeps its hold; shutdown
    // deletes the held segments anyway. Safe without a preceding start().
    future<> stop();

    // Installs the store client and the hooks uploads run with. Until it is
    // called a reported close does nothing at all, so the wiring must not set
    // the commitlog's hold_segments before this: a hold taken earlier would
    // have nobody to release it. Takes effect only while shipping is active
    // and before stop(), and a second call is ignored, as start_sealing()'s is.
    void start_shipping(seastar::shared_ptr<sstables::object_storage_client> client, segment_hooks hooks);

    // The commitlog's close notification for a held segment, which runs inside
    // the close, so this must not block: it starts the upload in the
    // background and returns. `used_size` is the bytes a replay would read,
    // and `failed` means the segment must not be shipped, in which case the
    // commitlog has already dropped the hold.
    //
    // A `used_size` of 0 is not uploaded - an empty object would describe a
    // segment that holds nothing - and its hold is released at once. The hold
    // on any other segment is released only after its bytes are stored.
    //
    // `failed` for a segment whose upload is in flight is a rollback of a
    // segment already reported as closed: that upload is aborted, so that a
    // file the rollback may have erased is never recorded as shipped.
    //
    // Once shipping has broken there is no chain left to pin segments for, so
    // nothing is uploaded and every hold reported from then on is released at
    // once. Provisional, pending the decision on the outage policy (#11).
    //
    // A close reported for a segment already being shipped is ignored, so a
    // duplicate report cannot start a second upload to the same name, nor
    // release the hold early by reporting a used size of 0 for it.
    void segment_closed(db::segment_id_type id, uint64_t used_size, bool failed);

    // Arms the seal timer and does nothing else. It does nothing at all unless
    // shipping is already active on this shard and stop() has not begun, so a
    // shipper that is off owns no timer, and the caller must call this after
    // note_cluster_size(1): a call made earlier is dropped and is not
    // remembered. Once a call has taken effect, later ones are ignored, so the
    // timer is never armed twice. The timer only triggers seal_tick().
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
    // condition_variable_timed_out after `timeout`. For tests only, and not to
    // be waited on alongside stop(): stop() does not resolve the condition, and
    // a shipper may be destroyed as soon as it has stopped.
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

    // Called on every shard once the node has joined, with the node count from
    // count_cluster_nodes(). This is the startup check; the seal cycle
    // repeats it, so a node added later is noticed there. Anything but
    // exactly one node, a count of zero included, refuses shipping until the
    // next restart; the refusal does not undo itself if the count later drops.
    void note_cluster_size(size_t node_count);

    // Called on every shard at startup, instead of note_cluster_size(), when
    // the node runs without a commitlog: a cycle would seal nothing and still
    // report active. Latched like every other refusal.
    void note_commitlog_disabled();

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

    // Segments whose bytes are stored and whose hold was released.
    uint64_t segments_shipped() const noexcept {
        return _segments_shipped;
    }

    // Segments that will not be shipped: a failed close, and one whose upload
    // failed for good. Both break the shipped chain.
    uint64_t segments_unshipped() const noexcept {
        return _segments_unshipped;
    }

    // Uploads started and not finished, the ones waiting for a slot included.
    size_t uploads_in_flight() const noexcept {
        return _uploading.size();
    }

    size_t max_concurrent_uploads() const noexcept {
        return _uploads.max_concurrent_uploads();
    }
};

} // namespace db::pitr
