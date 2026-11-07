/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
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

// Subtracted from every coverage capture (design, "Ledger"): a buffer against
// a stepping clock and a client's own timestamps.
constexpr std::chrono::microseconds coverage_margin = std::chrono::seconds(1);

// The nodes the cluster holds, as the topology state machine sees them: normal
// members, moving ones and ones waiting to be joined, but not the ones that
// have left (issue #7). The one count source, for the startup check and every
// cycle. Pure, constant-time, and read on shard 0, the only shard with it.
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
    // A ledger entry could not be written, so the chain ends before it.
    off_ledger_failed,
    // A segment the chain holds, or was about to, will not be in the store.
    off_chain_broken,
    // The segments waiting to be shipped have filled the commitlog's hold
    // limit, so the latch broke the coverage to keep the node writing (P56).
    off_outage,
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
// this header refers to: the hold is fixed when the commitlog is built, so
// until shipping starts every non-failed close is released at once and a failed
// one releases nothing, the commitlog having dropped it (issue #8).
bool should_hold_segments(const db::config&);

// Drops one hold. Runs inside the close: must not block or throw.
using release_hook = std::function<void(db::segment_id_type)>;

// What only the wiring knows. All required; the release hook is separate.
struct segment_hooks {
    // One handle per upload attempt, by segment_upload.hh's rules for `f`:
    // never reused by a retry, and never the commitlog's own.
    std::function<future<seastar::file>(db::segment_id_type)> open_segment;
    // Where a segment's bytes are stored, under its own file name.
    std::function<sstables::object_name(db::segment_id_type)> object_name_of;
    // commitlog::held_segments(). A rotated segment is listed here before its
    // close is reported, the commitlog not awaiting the close (commitlog.cc,
    // finish_and_get_new()), which is the only way to tell "no lower segment
    // exists" from "its close has not arrived".
    std::function<std::vector<db::segment_id_type>()> held_segments;
    // commitlog::held_bytes(), the disk the closed held segments take, and
    // commitlog::segment_hold_limit(), the bytes they may reach before a write
    // waits for one. The latch watches both; a limit of 0 turns it off.
    std::function<uint64_t()> held_bytes;
    std::function<uint64_t()> segment_hold_limit;
};

// A held segment's id to its path, or nothing: held_segment_path()'s shape.
using segment_path_function = std::function<std::optional<sstring>(db::segment_id_type)>;
using held_segments_function = std::function<std::vector<db::segment_id_type>()>;
using held_bytes_function = std::function<uint64_t()>;
using hold_limit_function = std::function<uint64_t()>;

// What only the wiring knows about the chain this shard is about to start.
struct chain_origin {
    utils::UUID host_id;       // part of every object key of the chain
    int64_t boot_epoch_us = 0; // one per boot, so every shard names the boot
    // std::ranges::max(held_segments()) when shipping starts, not its last
    // element: the hold order is not the id order. Below this floor a segment
    // was written before the chain, so it is released, not shipped (D27).
    // Empty when nothing is held, so every later close ships.
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
// shipping is on it runs the seal timer, uploads the segments whose close the
// commitlog reports, and records each in the ledger in segment-id order, which
// is what releases its hold. While off it owns no timer and no client, so a
// node without PITR behaves as before.
class pitr_shipper final {
public:
    // Seals the active segment of the main commitlog only, skipping unused
    // ones and requesting no flush. A failure is recorded but not repeated: the
    // segment is gone from the active slot either way, and the next cycle seals
    // whatever is active then. A failed close arrives through segment_closed().
    using seal_function = seastar::noncopyable_function<future<>()>;
    // count_cluster_nodes()'s count, read on shard 0, so a future; it runs
    // every cycle and must be cheap. A count that could not be established
    // must fail rather than return 0: any count but one turns shipping off.
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
    // One segment whose close was taken for shipping and whose ledger entry is
    // not in the store yet.
    struct pending_segment {
        uint64_t used_size = 0;
        uint32_t crc32 = 0;
        sstring file_name;
        // Its bytes are in the store: only its turn in the order is left.
        bool stored = false;
    };
    // In segment-id order, bounded by the held segments.
    std::map<db::segment_id_type, pending_segment> _pending;
    // One entry put at a time; `_recording_id` is its segment.
    bool _recording = false;
    std::optional<db::segment_id_type> _recording_id;
    uint64_t _next_sequence = 0;
    // The segment of the last entry, which the next one links to; 0 at first.
    db::segment_id_type _prev_recorded_id = 0;
    // Aborts the entry put in flight; every abort of it also means no later
    // entry is ever written, so one per shard is enough.
    abort_source _ledger_as;
    // One cycle's capture: the time, and the highest id the log held then.
    // Every write acknowledged before the time is in that segment or below it,
    // so only an entry at or above `top_held_id` may claim it.
    struct coverage_capture {
        db::segment_id_type top_held_id = 0;
        int64_t covered_through_us = 0;
    };
    // In increasing `top_held_id`, pruned on the way in and at each entry.
    std::deque<coverage_capture> _captures;
    // The last entry's coverage, which a segment with no capture takes.
    int64_t _prev_coverage_us = 0;
    uint64_t _segments_shipped = 0;
    uint64_t _segments_unshipped = 0;

    future<> seal_loop();
    future<> seal_cycle();
    // Takes a capture, before anything else a cycle does (P50).
    void capture_coverage();
    // The newest capture this segment may carry, else the previous entry's.
    int64_t coverage_for(db::segment_id_type id);
    // Breaks shipping, releasing every hold, once the segments waiting to be
    // shipped have reached the commitlog's limit. True when it broke here.
    bool outage_latch_tripped();
    // A segment held below `id` and at or above the floor whose close has not
    // been reported: `id` cannot be recorded before it.
    std::optional<db::segment_id_type> unreported_held_below(db::segment_id_type id) const;
    // Drops holds one at a time, yielding, inside the upload gate.
    future<> release_holds(std::vector<db::segment_id_type> ids);
    void break_shipping(shipping_state, sstring reason);
    // Uploads one segment and marks it ready to record; reports its own
    // failures rather than throwing.
    future<> ship_segment(db::segment_id_type id, uint64_t used_size);
    // No success is recorded; the hold follows segment_closed()'s rule.
    void note_upload_aborted(db::segment_id_type id);
    void note_entry_aborted(db::segment_id_type id);
    // Starts the recorder unless one runs already or nothing is ready.
    void record_ready_entries();
    // Writes the entry of every stored pending segment, lowest id first,
    // releasing each hold once its entry is in the store. Stops at the first
    // segment not stored yet or not yet reported; reports its own failures.
    future<> record_entries();

public:
    explicit pitr_shipper(const db::config& cfg) noexcept;

    pitr_shipper(const pitr_shipper&) = delete;
    pitr_shipper& operator=(const pitr_shipper&) = delete;

    // Reads the configuration and logs why shipping is off, if it is. Shipping
    // cannot begin here: the cluster size is not known yet. Ready future.
    future<> start();

    // Stops the seal timer, aborts every upload and the entry put in flight,
    // and waits for the seal, the uploads and the recorder to unwind. The abort
    // wins over one that has just finished, so a segment aborted here keeps its
    // hold and the chain ends at the last entry written (SHP-7). Safe without a
    // preceding start(). It waits as long as the store client takes to abandon
    // a request, which can be that client's own timeout (about 30 s per shard
    // for an S3 credential lookup), so shutdown is not bounded here.
    future<> stop();

    // Installs the hook that drops a hold, before the store client, as the hold
    // rule needs. It must outlive stop().
    void set_release_hook(release_hook hook);

    // Installs the store client and the hooks uploads run with; needs the
    // release hook installed, and takes effect only while shipping is active
    // and before stop(), a second call being ignored. A call that takes effect
    // is where the chain begins: its identity and floor come from `origin`.
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
    // and that backup would be refused (D27). Runs in the background inside
    // the gate stop() waits for, a failure being logged; dropped unless
    // shipping is active and stop() has not begun.
    void flush_after_start(flush_function flush);

    // The commitlog's close notification for a held segment. It runs inside the
    // close, so it must not block: it starts the upload in the background.
    // `used_size` is the bytes a replay would read; `failed` means the segment
    // must not be shipped, and the commitlog has dropped its hold already.
    //
    // What each close does, in the order the cases are checked:
    // - before start_shipping(), once shipping has broken (D25), or once stop()
    //   has begun, the hold rule applies; with no release hook it is ignored;
    // - `failed` at or above the floor ends the chain, because that segment's
    //   writes will never be in the store and no entry above it may cover
    //   them; its upload or entry put in flight is aborted;
    // - below chain_floor() it is released and not shipped, failed or not: it
    //   was written before this chain, and closes are not ordered by id;
    // - a repeat for a segment being shipped or waiting for its entry is
    //   ignored, so it cannot start a second upload or release the hold early;
    // - below the last entry recorded it breaks the chain too: it cannot be
    //   appended in order, and skipping it would leave the entries above it
    //   covering writes it holds;
    // - `used_size` 0 is released at once;
    // - a hold that would be kept while the holds have already reached the
    //   commitlog's limit trips the outage latch instead: shipping breaks and
    //   every hold goes, this one with them (P56, P63);
    // - any other hold goes once the bytes and the ledger entry are both in
    //   the store, which is what puts the segment in the chain.
    void segment_closed(db::segment_id_type id, uint64_t used_size, bool failed);

    // Arms the seal timer and does nothing else, so it must follow
    // note_cluster_size(1); dropped unless shipping is active and stop() has
    // not begun, and a second call is ignored. The timer only ticks.
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

    // Called once the node has joined, with count_cluster_nodes()'s count: the
    // startup check, which every cycle repeats, so a node added later is
    // noticed. Anything but one node, zero included, refuses shipping.
    void note_cluster_size(size_t node_count);

    // Instead of note_cluster_size() when the node has no commitlog: a cycle
    // would seal nothing and still report active. Latched.
    void note_commitlog_disabled();

    // Called instead of start_shipping() when pitr_endpoint names an endpoint
    // with no object_storage_endpoints entry: refused with a reason naming the
    // option, latched, and the seal timer dropped. Does nothing once shipping
    // is off, and never throws, a mistyped option not being fatal.
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

    // The latest capture's coverage: microseconds since the UNIX epoch, UTC,
    // less coverage_margin, 0 before the first cycle. An entry carries no more.
    int64_t covered_through_us() const noexcept {
        return _captures.empty() ? 0 : _captures.back().covered_through_us;
    }

    // Stored, recorded and released: the segments this chain holds.
    uint64_t segments_shipped() const noexcept {
        return _segments_shipped;
    }

    // Segments that will not be shipped, each ending the chain: a failed
    // close, an upload that failed for good, or an unwritable entry.
    uint64_t segments_unshipped() const noexcept {
        return _segments_unshipped;
    }

    // Closed, taken for shipping and not recorded yet.
    size_t segments_pending() const noexcept {
        return _pending.size();
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
// key a segment is stored under is under test. The path and name hooks throw
// for an id the log no longer holds, which is a permanent upload failure (D25).
// `shipper` must be the one they are installed on, and must outlive them.
segment_hooks make_segment_hooks(const pitr_shipper& shipper, segment_path_function segment_path, held_segments_function held_segments,
        held_bytes_function held_bytes, hold_limit_function hold_limit);

// What a snapshot's bound file asks this shard for (`db/pitr/bound_file.hh`).
// It reads the shipper on every call, so `shipper` must outlive it, which is
// why the wiring clears it before the shipper stops. The sink is where the
// wiring puts it: replica::database's, which db/pitr does not know about.
chain_provider make_chain_provider(const pitr_shipper& shipper);
using chain_provider_sink = std::function<void(chain_provider)>;

// Starts shipping on this shard, which is all the wiring at boot does: checks
// pitr_endpoint against the node's endpoints, takes the chain floor from the
// commitlog's held segments, installs the client, the hooks and - once the
// chain exists - the chain provider, and starts the flush. Does nothing unless
// shipping is active, and never throws: a client
// that cannot be built turns shipping off rather than failing the boot. `log`
// and the store manager must outlive `shipper`, which keeps using the log.
void start_shipping_on_shard(pitr_shipper& shipper, sstables::storage_manager& sstm, db::commitlog& log, utils::UUID host_id, int64_t boot_epoch_us,
        pitr_shipper::flush_function flush, chain_provider_sink install_chain_provider = {});

} // namespace db::pitr
