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

// What the bound file records for one source shard: where restore gets its
// shards and chain ids, never from a listing (D20). Its format is #12's.
struct chain_bound {
    utils::UUID host_id;
    uint32_t shard = 0;
    int64_t boot_epoch_us = 0;
    int64_t chain_start_us = 0;
    chain_id chain;
    // The bound the backup captured, the lowest position replay takes; never 0.
    uint64_t bound_segment_id = 0;
};

// One chain that held together, and what of it a restore needs.
struct checked_chain {
    // Every segment of the chain, in ledger order, as
    // `shard_replay_params::segments` asks for it. Each `path` is the segment's
    // object key: a fetch may replace the directory, never the file name.
    utils::chunked_vector<shipped_segment> segments;
    // `utils::crc32` of each segment's bytes, as its entry recorded it, in the
    // order of `segments`: the fetch checks it against what it downloaded.
    utils::chunked_vector<uint32_t> segment_crc32s;
    // The chain's coverage, its last entry's, and what T is checked against.
    int64_t covered_through_us = 0;
    // Entries walked, which equals the segment count until kind 3 idle marks
    // land and then exceeds it by their number. For the restore's log.
    uint64_t entries_read = 0;
};

// Reads one chain's ledger entries and decides whether a restore to
// `target_time_us` (T, microseconds since the UNIX epoch, UTC) may use it. The
// entries sit at dense `<sequence>.entry` keys (`ledger_entry.hh`, "Object
// keys"), so this walks sequence 0, 1, 2 on until the first key the store does
// not hold; a listing is unnecessary. An entry missing in the middle reads as
// the end, which lowers this chain's coverage and never raises it.
//
// Every condition it refuses has a code of its own in `refusal_reason`, and
// nothing has changed by the time one is built. A `t_past_coverage` interval
// runs from this chain's start, which the restore narrows to its backup.
//
// A store error is an exception and not a refusal, since an unreachable store
// says nothing about the backup: only an object the store does not hold ends
// the walk, told from an error by asking the store. A bound no key can be built
// from throws `ledger_format_error`, which the bound parser prevents. Yields
// per entry, keeps the lists chunked, and needs `client` and `as` to outlive it.
future<std::expected<checked_chain, refusal>> check_chain(
        sstables::object_storage_client& client, sstring bucket, sstring key_prefix, chain_bound bound, int64_t target_time_us, seastar::abort_source& as);

} // namespace db::pitr
