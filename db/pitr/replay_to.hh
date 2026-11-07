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

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include "db/pitr/bound_file.hh"
#include "db/pitr/refusal.hh"
#include "db/pitr/replay_target.hh"
#include "mutation/timestamp.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"
#include "utils/chunked_vector.hh"
#include "utils/rjson.hh"

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

// A larger schema.cql is damage or a wrong prefix, and is refused unread. The
// manifest's cap is the one that grows with the backup, at about 300 bytes per
// sstable.
constexpr size_t max_schema_cql_size = 1024 * 1024;
constexpr size_t max_manifest_size = 32 * 1024 * 1024;
constexpr const char* schema_cql_file_name = "schema.cql";
constexpr const char* manifest_file_name = "manifest.json";

// What a replay takes from the backup's manifest.
struct backup_manifest {
    // The backup's own names, for the log and the refusal: the target may be
    // named differently, which is the point of a replay (P52).
    sstring snapshot;
    sstring keyspace;
    sstring table;
    // The TOC object key of every sstable of the backup, relative to the
    // prefix, as the restore path takes them. Empty for a backup of a table
    // that held nothing.
    utils::chunked_vector<sstring> toc_names;
};

// Reads the manifest's fields a replay needs and refuses one that names a
// snapshot other than the call's: one prefix is one backup, so a prefix
// pointing at another backup is a user error and not a restore.
//
// The keyspace and the table are read but not checked: the target of a replay
// is allowed to be a table of another name, so only its schema is compared.
//
// Yields while parsing and once per sstable entry; the caller caps the bytes.
future<std::expected<backup_manifest, refusal>> parse_backup_manifest(rjson::chunked_content&& content, const sstring& expected_snapshot);

// `incomplete` until `done` replaces `started` (requirement 12).
enum class replay_phase {
    started,
    done,
};

// The durable mark, one file per target table.
struct replay_state {
    replay_phase phase = replay_phase::started;
    // The backup and T as the user wrote them: a human reads this file too.
    sstring snapshot;
    sstring t;
    // The table the backup was taken from, as its manifest records it: the
    // target may be named differently, so this is the only record of what was
    // loaded. Empty when no manifest was read.
    sstring source_keyspace;
    sstring source_table;
    // When the mark was written, microseconds since the UNIX epoch, UTC.
    int64_t started_at_us = 0;

    bool operator==(const replay_state&) const = default;
};

// The format, version 1: eight `key=value\n` lines in exactly this order and
// nothing else - `format=pitr-replay`, `version=1`, `phase`, `snapshot`, `t`,
// `source_keyspace`, `source_table`, `started_at_us` - and no checksum, since
// a replay refuses a mark it cannot read. Throws on anything else.
sstring encode(const replay_state&);
replay_state decode_replay_state(std::string_view);

// By table id, so a dropped and recreated table is a new target.
std::filesystem::path replay_state_path(const std::filesystem::path& state_dir, ::table_id target);

// A crash leaves either no file or a complete one: fsynced under a temporary
// name, renamed over the final one, then the directory fsynced.
future<> write_replay_state(const std::filesystem::path& state_dir, ::table_id target, const replay_state&);

// `nullopt` only when there is no mark. An unreadable one throws: damage is not
// a target that was never replayed into.
future<std::optional<replay_state>> read_replay_state(const std::filesystem::path& state_dir, ::table_id target);

// Drops from a snapshot's `schema.cql` what a replay into another table is
// allowed to differ in: the table's name wherever a statement names it, its
// `ID` option and its `compaction` option (P52). Both sides come from the same
// describe code, one option per line, so this works line by line. Yields.
future<sstring> normalize_schema_cql(std::string_view);

// Compares the two statements, ignoring the three above.
future<std::optional<refusal>> check_schema_match(std::string_view target_cql, std::string_view backup_cql);

// What a checked and marked replay hands to the load and, after it, to ep.3.
struct replay_start {
    ::table_id target;
    // T, microseconds since the UNIX epoch, UTC.
    api::timestamp_type t = 0;
    bound_file bound;
    // The backup's sstables, from its manifest.
    utils::chunked_vector<sstring> toc_names;
    // Held until the run ends, so no second replay enters this target.
    replay_in_flight::guard in_flight;
};

// Runs every check a replay makes before it loads anything, in this order, and
// then writes the `started` mark: T parses, a store location was given and is
// known, the target exists, no replay is running into it, no mark exists for
// it, its auto-compaction is off, it is empty, the backup's manifest names the
// call's snapshot, it carries a bound file the strict reader accepts, and the
// target's schema matches the backup's. The mark comes before the state of the
// table, because a marked target is answered with "drop it" whatever state a
// run left it in. The cheap local checks come first, so
// a plainly wrong call never reaches the store, and the mark is written last,
// so a refusal leaves the node as it was.
//
// Runs on shard 0: `in_flight` is that shard's, so two calls on one target meet
// there whichever shards they arrived on.
// A refusal is the answer to a question about the call; a store or disk error is
// an exception, since it says nothing about the backup. Reads three objects,
// yields per read and per manifest entry, and needs every reference to outlive
// the call.
future<std::expected<replay_start, refusal>> start_replay_to(sharded<replica::database>& db, sstables::storage_manager& sm, replay_in_flight& in_flight,
        std::filesystem::path state_dir, const replay_to_params&, seastar::abort_source& as);

} // namespace db::pitr
