/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string_view>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "db/pitr/refusal.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace db::pitr {

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

} // namespace db::pitr
