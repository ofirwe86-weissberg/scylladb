/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <optional>
#include <string_view>
#include <unordered_set>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include "db/pitr/bound_file.hh"
#include "db/pitr/refusal.hh"
#include "mutation/timestamp.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace seastar {
class abort_source;
} // namespace seastar

namespace replica {
class database;
} // namespace replica

namespace sstables {
class storage_manager;
class object_storage_client;
} // namespace sstables

namespace db::pitr {

// The endpoint's parameters as they arrived. T stays text: a malformed one is
// ordinary user input and comes back as a refusal, not as a bad request. There
// is no list of sstables: a replay takes them from the backup itself.
struct replay_to_params {
    // The same three the restore endpoint takes.
    sstring endpoint;
    sstring bucket;
    sstring prefix;
    // The target the backup is replayed into, which the user created.
    sstring keyspace;
    sstring table;
    // The backup, as its snapshot was named; recorded in the mark.
    sstring snapshot;
    sstring t;
};

// A larger schema.cql is damage or a wrong prefix, and is refused unread.
constexpr size_t max_schema_cql_size = 1024 * 1024;

constexpr const char* schema_cql_file_name = "schema.cql";

// The targets a replay is running into on this node, so that a second call on
// one target is refused rather than writing over the first one's work (P62).
// One instance, on shard 0, which is where every replay's checks and mark run.
class replay_in_flight {
    std::unordered_set<::table_id> _targets;

public:
    // Holds one target for as long as the run lasts. Not copied, and released
    // on every path out of the run, a failure and a shutdown included.
    class guard {
        replay_in_flight* _owner = nullptr;
        ::table_id _target;

    public:
        guard() = default;
        guard(replay_in_flight& owner, ::table_id target) noexcept
            : _owner(&owner)
            , _target(target) {
        }
        guard(const guard&) = delete;
        guard& operator=(const guard&) = delete;
        guard(guard&& o) noexcept
            : _owner(std::exchange(o._owner, nullptr))
            , _target(o._target) {
        }
        guard& operator=(guard&&) = delete;
        ~guard();
    };

    // `nullopt` when a replay is already running into this target.
    std::optional<guard> try_take(::table_id target);
    bool holds(::table_id target) const {
        return _targets.contains(target);
    }
};

// A table is `incomplete` from the moment `started` is written until `done`
// replaces it (requirement 12), so a crash in the middle is told from a table
// no replay ever touched.
enum class replay_phase {
    started,
    done,
};

// The durable mark, one file per target table: what a later call and a later
// status need to say what was done to this table.
struct replay_state {
    replay_phase phase = replay_phase::started;
    // The backup and T as the user wrote them: a human reads this file too.
    sstring snapshot;
    sstring t;
    // When the mark was written, microseconds since the UNIX epoch, UTC.
    int64_t started_at_us = 0;

    bool operator==(const replay_state&) const = default;
};

// The format, version 1: six `key=value\n` lines in exactly this order and
// nothing else - `format=pitr-replay`, `version=1`, `phase`, `snapshot`, `t`,
// `started_at_us` - and no checksum, since a replay refuses a target whose
// mark it cannot read. Throws `std::runtime_error` on anything else.
sstring encode(const replay_state&);
replay_state decode_replay_state(std::string_view);

// The table's id and not its name, so that a dropped and recreated table is a
// new target, and a rename does not hide a mark.
std::filesystem::path replay_state_path(const std::filesystem::path& state_dir, ::table_id target);

// A crash leaves either no file or a complete one: the bytes go to a temporary
// name, are flushed and fsynced, and only then renamed over the final name,
// after which the directory is fsynced. Creates the directory if needed.
future<> write_replay_state(const std::filesystem::path& state_dir, ::table_id target, const replay_state&);

// `nullopt` only when there is no mark for this target. An unreadable one
// throws: damage is not a target that was never replayed into.
future<std::optional<replay_state>> read_replay_state(const std::filesystem::path& state_dir, ::table_id target);

// Drops from a snapshot's `schema.cql` what a replay into another table is
// allowed to differ in: the table's name wherever a statement names it, its
// `ID` option and its `compaction` option (P52). Both sides come from the same
// describe code, which writes one option per line, so this works line by line.
// Linear in the text, which the caller caps, and yields per line.
future<sstring> normalize_schema_cql(std::string_view);

// Compares the two statements, ignoring the three above.
future<std::optional<refusal>> check_schema_match(std::string_view target_cql, std::string_view backup_cql);

// What a checked and marked replay hands to the load and, after it, to ep.3.
struct replay_start {
    ::table_id target;
    // T, microseconds since the UNIX epoch, UTC.
    api::timestamp_type t = 0;
    bound_file bound;
    // Held until the run ends, so no second replay enters this target.
    replay_in_flight::guard in_flight;
};

// Runs every check a replay makes before it loads anything, in this order, and
// then writes the `started` mark: T parses, a store location was given and is
// known, the target exists, no replay is running into it, its auto-compaction
// is off, it is empty, no mark exists for it, the backup carries a bound file
// the strict reader accepts, and the target's schema matches the backup's. The cheap local checks come first, so
// a plainly wrong call never reaches the store, and the mark is written last,
// so a refusal leaves the node as it was.
//
// Runs on shard 0: `in_flight` is that shard's, and two calls on one target
// meet there whichever shards they arrived on.
//
// A refusal is the answer to a question about the call; a store or disk error is
// an exception, since it says nothing about the backup. Reads two small
// objects, yields per read, and needs every reference to outlive the call.
future<std::expected<replay_start, refusal>> start_replay_to(sharded<replica::database>& db, sstables::storage_manager& sm, replay_in_flight& in_flight,
        std::filesystem::path state_dir, const replay_to_params&, seastar::abort_source& as);

} // namespace db::pitr
