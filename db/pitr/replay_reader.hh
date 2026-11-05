/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <functional>
#include <optional>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "db/commitlog/replay_position.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

class column_mapping;
class frozen_mutation;

namespace db {
class extensions;
}

namespace db::pitr {

// One commitlog entry that passed every filter below.
struct replay_entry {
    // Both references point into the reader's own buffers and are valid only
    // for the duration of the consumer call.
    const frozen_mutation& mutation;
    // The source table's column mapping as the segment carried it, absent when
    // this entry was written without one: only the first entry for a schema
    // version in a segment is guaranteed to have it, so a caller that needs the
    // mapping must remember the ones it has seen.
    const std::optional<column_mapping>& mapping;
    // Where the entry starts in its segment. For a write split across segments
    // this is the position of its first fragment, which is where the source
    // ordered it.
    db::replay_position position;
};

// Called once per surviving entry, in ascending segment order. Throwing, or
// returning a failed future, aborts the read and fails the operation; nothing
// after the failing entry is read.
using replay_consumer = std::function<future<>(const replay_entry&)>;

// What one shard's segments are read with. Restore builds one of these per
// source shard, because the bound, the truncation floor and the reassembly of
// split writes are all per shard.
struct shard_replay_params {
    // The directory the shipped segments were fetched into. Never the live
    // commitlog directory, which startup replays whatever it finds in.
    sstring directory;
    // The filename prefix the segments were written with. Files that do not
    // start with it, and files whose name is not a segment descriptor, are
    // ignored.
    sstring filename_prefix;
    // The source shard whose segments are read. Every other shard's segments in
    // the directory are left alone, so that one reader per shard can run and
    // each keeps its own reassembly state.
    unsigned shard = 0;
    // The only table whose entries reach the consumer, matched on table id
    // before anything resolves a schema: system tables share the commitlog, and
    // a table out of scope may not exist in this node at all.
    table_id source_table;
    // The lowest position restore replays. An entry exactly at the bound is
    // replayed; everything below it is already in the snapshot.
    db::replay_position bound;
    // The source table's latest TRUNCATE position at or below the backup. An
    // entry at or below it was truncated away at the source and must not come
    // back, so this floor is exclusive where the bound is inclusive, as normal
    // replay has it. A default-constructed position means no truncation, and
    // then drops nothing: no real entry sits at position zero.
    db::replay_position truncate_floor;
    const db::extensions* extensions = nullptr;
};

struct replay_reader_stats {
    uint64_t segments_read = 0;
    uint64_t entries_read = 0;
    uint64_t entries_replayed = 0;
    uint64_t dropped_below_bound = 0;
    uint64_t dropped_at_or_below_truncate_floor = 0;
    uint64_t dropped_other_table = 0;
    uint64_t raft_batches_skipped = 0;

    bool operator==(const replay_reader_stats&) const = default;
};

// Reads every segment in `params.directory` belonging to `params.shard`, in
// ascending segment order, and hands each surviving entry to `consume`.
//
// Unlike normal replay this is strict: any error reading a segment fails the
// whole call rather than being counted and skipped, because a restore that
// silently dropped part of the log would hand back a wrong database. The
// position filters are compared per entry and never passed to the reader as a
// start offset: an offset only selects whole chunks, so an entry starting below
// it would still be read in full.
//
// One reassembly state is shared across the shard's segments, so a write split
// across two segments arrives once, whole, at the position it started at.
//
// Yields between segments and between entries. Decoding one entry does not
// yield and is not bounded by this code: a single write can be as large as the
// commitlog allows.
future<replay_reader_stats> replay_shard_segments(const shard_replay_params& params, replay_consumer consume);

} // namespace db::pitr
