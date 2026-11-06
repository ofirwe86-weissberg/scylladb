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
// restore gets its shards and chain ids, never from a listing (D20). A plain
// struct: the file's own format is #12's.
struct chain_bound {
    utils::UUID host_id;
    uint32_t shard = 0;
    int64_t boot_epoch_us = 0;
    int64_t chain_start_us = 0;
    chain_id chain;
    // The segment id of the bound the backup captured, which is the lowest
    // position replay takes. Never 0: segment ids start at 1.
    uint64_t bound_segment_id = 0;
};

// One chain that held together, and what of it a restore needs.
struct checked_chain {
    // Every segment of the chain, in ledger order, which is what
    // `shard_replay_params::segments` asks for: ascending segment id, no gap,
    // one boot. Each `path` is the segment's object key, so a fetch may replace
    // the directory but must keep the file name, which replay reads the
    // descriptor from. To the end of the chain and not to T, since replay runs
    // the whole list and the T filter drops what is above T (P43).
    utils::chunked_vector<shipped_segment> segments;
    // `utils::crc32` of each segment's bytes, as its entry recorded it, in the
    // order of `segments`: the fetch checks it against what it downloaded.
    utils::chunked_vector<uint32_t> segment_crc32s;
    // The chain's coverage, which is its last entry's: what the restore's own
    // range check reports as restorable, and what T is checked against.
    int64_t covered_through_us = 0;
    // Entries walked, which exceeds the segments by the number of idle marks in
    // the chain. For the restore's log.
    uint64_t entries_read = 0;
};

// Reads one chain's ledger entries from the store and decides whether a restore
// to `target_time_us` (T, microseconds since the UNIX epoch, UTC) may use it.
// The entries sit at dense `<sequence>.entry` keys under the chain's prefix
// (`ledger_entry.hh`, "Object keys"), so this walks sequence 0, 1, 2 and so on
// until the first key the store does not hold; dense keys make a listing
// unnecessary. An entry missing in the middle therefore reads as the end, which
// lowers the coverage this chain can prove but never raises it.
//
// What it refuses, each with a code of its own and nothing changed: a chain
// with no entries at all (PM-50); an entry that will not decode, a kind 3 idle
// mark among them until its piece lands; an entry whose body does not match its
// key, which is built from the bound file, and which is what keeps one list to
// one boot; a broken link or a segment id that does not rise, which is how a
// gap in a shipped run shows; a bound below the chain's first segment (D27);
// and T above the chain's coverage, with the interval it could have given,
// whose `from_us` is this chain's start, so the restore narrows it to the
// backup it would restore from.
//
// A store error is not a refusal but an exception, since an unreachable store
// says nothing about the backup: only an object the store does not hold ends
// the walk, and that is told from an error by asking the store. A bound no key
// can be built from throws `ledger_format_error` before any read, which the
// bound parser prevents and a restore does not report. Yields per entry and
// keeps the lists chunked, as a chain can be weeks long. `client` and `as` are
// this shard's and must outlive the returned future; the rest is copied.
future<std::expected<checked_chain, refusal>> check_chain(
        sstables::object_storage_client& client, sstring bucket, sstring key_prefix, chain_bound bound, int64_t target_time_us, seastar::abort_source& as);

} // namespace db::pitr
