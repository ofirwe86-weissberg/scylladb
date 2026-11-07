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
#include "db/pitr/chain_check.hh"
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

// A larger object is damage or a wrong prefix, and is refused unread; the
// manifest's cap is the one that grows with the backup.
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
    // The TOC object key of every sstable, relative to the prefix, as the
    // restore path takes them. Empty for a backup of an empty table.
    utils::chunked_vector<sstring> toc_names;
};

// Reads the manifest's fields a replay needs and refuses one that names a
// snapshot other than the call's: one prefix is one backup, so a prefix
// pointing at another is a user error and not a restore. The keyspace and the
// table are read but not checked, since a replay into a table of another name
// is the point. Yields while parsing and per entry; the caller caps the bytes.
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
    // only record of what was loaded. Empty when no manifest was read.
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

// Where a run's fetched segments live: under the work directory, one directory
// per target, and never under the commitlog directory - startup replays
// whatever it finds there (`commitlog.cc`, list_existing_descriptors).
std::filesystem::path restore_root(const std::filesystem::path& work_dir);
std::filesystem::path restore_dir_path(const std::filesystem::path& work_dir, ::table_id target);

// Empty: what a crashed run of the same target left is of no use to this one.
future<std::filesystem::path> make_restore_dir(const std::filesystem::path& work_dir, ::table_id target);

// Called on every path out of a run, a failure and an abort too, so neither
// throws: a leftover costs disk and nothing else. The sweep runs at node start,
// where no run is in flight, so it drops the whole root.
future<> remove_restore_dir(const std::filesystem::path& work_dir, ::table_id target) noexcept;
future<> sweep_restore_dirs(const std::filesystem::path& work_dir) noexcept;

// Streams one shipped segment into `dir`, under the file name the source wrote
// it with, since replay reads the segment's id and base position out of it, and
// proves what landed is what the ledger recorded: the entry's size and its
// `utils::crc32`. Refuses what says something about the history - an object the
// store does not hold, one whose size or checksum is not the entry's - and
// throws on a read, disk or abort error; either way no file is left behind.
// Nothing is buffered whole, and the read stops one byte past the recorded
// size, so an object that grew cannot fill the disk. Yields per chunk.
future<std::expected<std::filesystem::path, refusal>> fetch_segment(sstables::object_storage_client& client, sstring bucket, const shipped_segment& segment,
        uint32_t expected_crc32, std::filesystem::path dir, seastar::abort_source& as);

// Drops from a snapshot's `schema.cql` what a replay into another table is
// allowed to differ in: the table's name wherever a statement names it, its
// `ID` option and its `compaction` option (P52). Both sides come from the same
// describe code, one option per line, so this works by line. Yields.
future<sstring> normalize_schema_cql(std::string_view);

// Compares the two statements, ignoring the three above.
future<std::optional<refusal>> check_schema_match(std::string_view target_cql, std::string_view backup_cql);

// The id of the table the backup was taken from, which is the id its commitlog
// entries carry and the only one a replay may apply: the target's id is a
// different one, and the manifest records the source's name but not its id.
// `nullopt` when the statement carries no `ID` option, which a snapshot's
// `schema.cql` always does.
std::optional<::table_id> parse_source_table_id(std::string_view schema_cql);

// What a checked and marked replay hands to the load and, after it, to ep.3.
struct replay_start {
    ::table_id target;
    // T, microseconds since the UNIX epoch, UTC.
    api::timestamp_type t = 0;
    bound_file bound;
    // Every shard's chain, checked and proved to cover T, with the segments the
    // replay reads in ledger order.
    checked_history history;
    // The backup's sstables and the table it came from: a shared snapshot tag
    // makes the names the only record of what was loaded.
    utils::chunked_vector<sstring> toc_names;
    sstring source_keyspace;
    sstring source_table;
    // From the backup's schema: what the shipped entries are matched on.
    ::table_id source_table_id;
    // Held until the run ends, so no second replay enters this target.
    replay_in_flight::guard in_flight;
};

// Runs every check a replay makes before it loads anything, in this order, and
// then writes the `started` mark: T parses, a store location was given and is
// known, the target exists, no replay is running into it, no mark exists for
// it, its auto-compaction is off, it is empty, the backup's manifest names the
// call's snapshot, it carries a bound file the strict reader accepts, the
// target's schema matches the backup's, this node has a shipping location to
// read the history from, and every chain the bound names holds together and
// covers T. The cheap local checks come first, so a plainly wrong call never
// reaches the store; the chain checks come last and still before the mark, so a
// T above coverage can be retried on the same target. A marked target is
// answered with "drop it" whatever state a run left it in, so a refusal must
// leave no mark.
//
// Runs on shard 0: `in_flight` is that shard's, so two calls on one target meet
// there whichever shards they arrived on. A refusal answers a question about the
// call; a store or disk error is an exception, saying nothing about the backup.
// Reads the backup's three files and every ledger entry of every chain it names,
// yields per read and per entry, and needs every reference to outlive the call.
future<std::expected<replay_start, refusal>> start_replay_to(sharded<replica::database>& db, sstables::storage_manager& sm, replay_in_flight& in_flight,
        std::filesystem::path state_dir, const replay_to_params&, seastar::abort_source& as);

} // namespace db::pitr
