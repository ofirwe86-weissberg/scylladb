/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <functional>
#include <optional>
#include <stdexcept>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "db/commitlog/replay_position.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"
#include "utils/chunked_vector.hh"

class column_mapping;
class frozen_mutation;

namespace db {
class extensions;
}

namespace db::pitr {

// One commitlog entry that passed every filter below.
struct replay_entry {
    // Both references are into the reader's buffers, valid for the consumer call only.
    const frozen_mutation& mutation;
    // Only the first entry for a schema version in a segment is guaranteed to carry one, so a caller that needs it must remember what it has seen.
    const std::optional<column_mapping>& mapping;
    // Where the entry starts, and for a split write where its first fragment started, which is where the source ordered it.
    db::replay_position position;
};

// Called once per surviving entry. Throwing, or returning a failed future, fails the call at that entry; nothing past it is read.
using replay_consumer = std::function<future<>(const replay_entry&)>;

// One shipped segment, as the caller verified it.
struct shipped_segment {
    // The filename must still be the source's segment name: the segment id and the entry format are read from it.
    sstring path;
    // A shipped file holds only the bytes the source wrote, so this is also the size the file must have.
    uint64_t used_size = 0;
};

// Input that is not what the caller promised. Entries from earlier segments in the list may already have reached the consumer.
class invalid_replay_input : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Per source shard: the bound, the floor and the reassembly are all per shard.
struct shard_replay_params {
    // Read in this order, which must be ascending segment id without duplicates. The caller's chain check builds the list;
    // the reader never scans a directory, so a segment not listed is not read.
    utils::chunked_vector<shipped_segment> segments;
    sstring filename_prefix;
    // The shard the segments and both positions below belong to.
    unsigned shard = 0;
    // The only table that reaches the consumer, matched on the id before anything resolves a schema: system tables share
    // the commitlog, and a table out of scope may not exist in this node at all.
    table_id source_table;
    // The lowest position replayed, inclusive; below it is in the snapshot.
    db::replay_position bound;
    // The source table's latest TRUNCATE position at or below the backup, which nothing may come back from; exclusive, as
    // normal replay has it. Default means no truncation, and drops nothing: no entry sits at position zero.
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

// Reads `params.segments` in list order and hands each surviving entry to `consume`. `params` must outlive the call.
//
// Guaranteed. A segment that is not the one its list entry named is refused with invalid_replay_input before any of its entries is read:
// a size other than the recorded one (so also a file cut anywhere, including to nothing), a missing or unreadable file, a name that is
// not a segment descriptor of `filename_prefix`, a tag other than the variant one, an id out of order or repeated, another shard, or a
// header that is zeroed, is not a commitlog header, or names another segment. So is a bound or floor of another shard. Any read error,
// decode error or consumer failure fails the call, where normal replay counts the damage and carries on. A split write is reassembled
// from one state shared by the shard's segments and delivered once, whole.
//
// Not guaranteed, and known open (#17): a segment of the recorded size with an intact header whose middle is a zeroed chunk still ends
// early, with success, because read_log_file reports a zero chunk header as a clean end of log. A split write whose fragments never
// complete raises nothing here either.
//
// Delivered positions are not monotonic: a split write arrives when its last fragment is read, at the position of its first.
//
// Yields between segments and entries. Decoding one entry does not, and is not bounded here: a write can be as large as the commitlog
// allows.
future<replay_reader_stats> replay_shard_segments(const shard_replay_params& params, replay_consumer consume);

} // namespace db::pitr
