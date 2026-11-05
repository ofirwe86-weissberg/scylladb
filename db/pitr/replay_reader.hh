/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <functional>
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
    // This mutation's schema version's mapping, resolved from whichever entry carried it, often one the filters dropped.
    const column_mapping& mapping;
    // Where the entry starts, and for a split write where its first fragment started, which is where the source ordered it.
    db::replay_position position;
};

// Called once per surviving entry. Throwing, or returning a failed future, fails the call at that entry; nothing past it is read.
using replay_consumer = std::function<future<>(const replay_entry&)>;

// One shipped segment, as the caller verified it.
struct shipped_segment {
    // The filename must still be the source's segment name: the segment id and the entry format are read from it.
    sstring path;
    uint64_t used_size = 0; // Only the bytes the source wrote are shipped, so this is also the size the file must have.
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
// a size other than the recorded one (so also a file cut anywhere, including to nothing), a file that cannot be opened, a name that does
// not start with `filename_prefix` or is not a segment descriptor of it, a tag other than the variant one, an id out of order, repeated
// or too large to parse, another shard, or a header that is zeroed, is not a commitlog header, or names another segment. So is a bound or
// floor of another shard. Any read error, decode error or consumer failure fails the call, where normal replay counts the damage and
// carries on. A split write is reassembled from one state shared by the shard's segments and delivered once, whole. Every entry of the
// source table is decoded, dropped or not, since a segment carries a schema version's mapping only on the first entry for it in a chunk:
// the mappings are kept across the shard's segments, and an entry whose version none of them covers refuses the segment. The caller
// therefore remembers nothing.
//
// Not guaranteed, and known open (#17): a zeroed sector in the middle of a segment of the recorded size with an intact header is not
// detected, and the call still succeeds. read_log_file skips the sector checksum for an all-zero sector, so such a sector can end the
// segment early, leave a hole with the entries after it still delivered, or hand the consumer a wrong mutation assembled across the
// hole. A split write whose fragments never complete raises nothing here either. A disk error during the read can also isolate the node
// rather than only fail the call, because read_log_file reads through the commitlog's own error handler: no production caller may use
// this until the commitlog read path takes an error handler of its own.
//
// Delivered positions are not monotonic: a split write arrives when its last fragment is read, at the position of its first.
//
// Yields between segments and entries. Decoding one entry does not, and is not bounded here: a write can be as large as the commitlog
// allows.
future<replay_reader_stats> replay_shard_segments(const shard_replay_params& params, replay_consumer consume);

} // namespace db::pitr
