/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <expected>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "db/pitr/ledger_entry.hh"
#include "db/pitr/refusal.hh"
#include "db/pitr/replay_reader.hh"
#include "seastarx.hh"
#include "utils/UUID.hh"
#include "utils/chunked_vector.hh"

namespace seastar {
class abort_source;
} // namespace seastar

namespace sstables {
class object_storage_client;
} // namespace sstables

namespace db::pitr {

// What the backup's bound file records for one source shard, which is where
// restore gets the shards and the chain ids from and never from a listing
// (D20). Taken here as a plain struct: the file's own format is #12's.
struct chain_bound {
    utils::UUID host_id;
    uint32_t shard = 0;
    int64_t boot_epoch_us = 0;
    int64_t chain_start_us = 0;
    chain_id chain;
    // The segment id of the bound position the backup captured: the lowest
    // position replay takes, so everything below it is in the backup's
    // sstables. Never 0, since segment ids start at 1 and no shipped segment
    // sits at the default position.
    uint64_t bound_segment_id = 0;
};

// One chain that held together, and what of it a restore needs.
struct checked_chain {
    // Every segment of the chain, in ledger order, which is ascending segment
    // id with no gap in the chain, and all of one boot: exactly what
    // `shard_replay_params::segments` asks for. Each `path` is the segment's
    // object key, so a fetch may replace the directory but must keep the file
    // name, which replay reads the segment's descriptor from.
    //
    // To the end of the chain and not to T: replay runs the whole list and the
    // T filter drops what is above T (P43), because an entry's coverage is a
    // bound on the writes a segment holds and not a cut-off point - a segment
    // can hold writes on either side of it.
    utils::chunked_vector<shipped_segment> segments;
    // The chain's coverage, which is its last entry's: it is what the restore's
    // own range check reports as restorable, and what T is checked against.
    int64_t covered_through_us = 0;
    // Entries walked, which is one more than the segments when an idle mark
    // joins the chain. For the restore's log.
    uint64_t entries_read = 0;
};

// Reads one chain's ledger entries from the store and decides whether a restore
// to `target_time_us` (T, microseconds since the UNIX epoch, UTC) may use it.
//
// How the entries are found. They sit at dense `<sequence>.entry` keys under the
// chain's prefix (`ledger_entry.hh`, "Object keys"), so this walks sequence 0,
// 1, 2 and so on by key until the first one the store does not hold, and that
// is the end of the chain. Dense keys make a listing unnecessary, and a walk by
// key has no pagination to get wrong. The cost is that an entry missing in the
// middle, with later entries above it, reads as the end of the chain: coverage
// then comes out lower than the store could have given, which refuses a restore
// that could have run but never accepts one it should not. A refusal of the
// shipper's own rule - every segment that held data is an entry, or the chain
// ends there - stays impossible either way.
//
// What it refuses, each with a code of its own and nothing changed:
//   - a chain with no entries at all (PM-50);
//   - an entry that will not decode: the checksum, the size, the kind or the
//     version (so a kind 3 idle mark is refused until the piece that defines it
//     lands);
//   - an entry whose body does not match its key or the bound file: the host,
//     the shard, the boot epoch, the chain start, the chain id or the sequence.
//     This is what keeps one list to one boot, which the reader needs because a
//     split write's fragment ids restart at zero in each commitlog;
//   - a broken link: entry 0 with a previous segment id, a later entry whose
//     previous segment id is not the entry before it, or a segment id that does
//     not rise. A gap in the middle of a list would let an acknowledged write's
//     tail sit outside it, and the reader would count one incomplete write too
//     few;
//   - a bound below the chain's first segment, which means the commitlog
//     between the backup and the chain's floor was never shipped (D27);
//   - T above the chain's coverage, with the interval it could have given.
//
// A store error is not a refusal and comes back as an exception: a refusal says
// the backup cannot serve this restore, and an unreachable store says nothing
// about the backup. Only an entry the store does not hold ends the walk, and
// that is told apart from an error by asking the store for the object, so a
// failing read is never read as the end of a chain.
//
// Yields per entry, and holds only the segment list, in chunked storage: a
// chain carries an entry per seal interval per shard and can be weeks long.
//
// `client` and `as` are this shard's and must outlive the returned future; the
// rest is copied.
future<std::expected<checked_chain, refusal>> check_chain(
        sstables::object_storage_client& client, sstring bucket, sstring key_prefix, chain_bound bound, int64_t target_time_us, seastar::abort_source& as);

} // namespace db::pitr
