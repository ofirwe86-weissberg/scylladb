/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <stdexcept>
#include <vector>

#include <seastar/core/sstring.hh>

#include "db/commitlog/replay_position.hh"
#include "db/pitr/chain_provider.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"
#include "utils/UUID.hh"

namespace db::pitr {

// One file per snapshot, written once by the orchestrator and uploaded with the
// backup. Restore takes the set of shards, their replay bounds and their chain
// ids from it, never from a bucket listing, so a shard whose directory is gone
// reads as a missing recorded chain and not as one that never shipped (#12).
//
// The name is no sstable component, and the lister refuses what it cannot parse
// rather than ignoring it, so `manifest_json_filter` skips it by name. An older
// Scylla has no such filter and cannot load a snapshot holding this file.
constexpr const char* bound_file_name = "pitr-bound.txt";

// Every shard the replay position can address: it packs the shard into the id.
constexpr size_t max_shards_in_a_bound_file = size_t(1) << db::replay_position::max_cpu_bits;
// A row with a chain and full-width values, measured at 305 bytes: shard 1023,
// a segment id and a floor at 2^64-1, a position at 2^32-1 and both times at
// INT64_MAX. Rounded up, so a field that grows by a digit does not break it.
constexpr size_t max_bound_file_row_size = 320;
// Refused before a byte is read, and decode allocates nothing per line. Never
// raised: a version 1 reader refuses a larger file, so raising it needs a new
// version.
constexpr size_t max_bound_file_size = 512 * 1024;
// One decision: the cap holds every shard a position can name, or a wide
// machine's snapshot would fail.
static_assert(max_shards_in_a_bound_file * max_bound_file_row_size + 1024 <= max_bound_file_size);

constexpr uint32_t bound_file_version_1 = 1;

// Two errors, as the ledger has, because restore reports them differently:
// damage means another backup, a newer version means an upgrade.
class bound_file_format_error : public std::runtime_error {
public:
    explicit bound_file_format_error(const sstring& what)
        : std::runtime_error(what.c_str()) {
    }
};

class bound_file_unknown_version : public bound_file_format_error {
public:
    explicit bound_file_unknown_version(const sstring& what)
        : bound_file_format_error(what) {
    }
};

struct shard_bound {
    uint32_t shard = 0;
    // Where replay starts: at or below every write the snapshot's sstables do
    // not hold. See table::snapshot_sstables().
    db::replay_position bound;
    // `none` when the shard had no chain at capture time, because PITR was off
    // or the shipper had not started shipping on it yet.
    std::optional<shard_chain> chain;

    bool operator==(const shard_bound&) const = default;
};

struct bound_file {
    // Microseconds since the UNIX epoch, UTC, never negative.
    int64_t capture_time_us = 0;
    // The schema at capture time, so a restore into another one is refused
    // rather than replayed into a shape the mutations do not fit.
    table_schema_version schema_version;
    // Exactly one row per shard, in order 0..N-1, never empty.
    std::vector<shard_bound> shards;

    bool operator==(const bound_file&) const = default;
};

// Returns the file's bytes. Throws `bound_file_format_error` on anything the
// reader would refuse: no shards, more than the cap, a shard out of order, a
// negative time, a null host or chain id, a floor of 0. Never builds past the
// cap.
sstring encode(const bound_file&);

// The format, version 1. Every line is `key=value\n`, the keys in exactly this
// order, and the last line's value is `utils::crc32` over every byte before it
// as eight lowercase hex digits:
//
//   format=pitr-bound
//   version=1
//   capture_time_us=<decimal>
//   schema_version=<canonical uuid>
//   shard_count=<decimal>
//   then per shard, in order 0..N-1:
//     shard=<decimal>
//     bound_segment_id=<decimal>
//     bound_position=<decimal>
//     chain=none            (or `chain=present` followed by)
//     chain_host_id=<canonical uuid>
//     chain_boot_epoch_us=<decimal>
//     chain_start_us=<decimal>
//     chain_id=<canonical uuid>
//     chain_floor_segment_id=<decimal, never 0, or `none`>
//   crc32=<8 hex digits>
//
// Text, not the ledger's binary, because an operator reads this one when a
// restore refuses, and it is written once per snapshot.
//
// The order of the checks is part of the format, so a reader built today
// diagnoses every later version the same way: size, `format`, checksum,
// version, fields. The checksum precedes the version so a flipped bit there is
// damage and not "written by a newer Scylla", which would send an operator
// upgrading. Decimals carry no sign, no leading zero and no overflow; a UUID is
// canonical and not null; nothing follows the checksum line. Throws
// `bound_file_unknown_version` above this version, `bound_file_format_error`
// otherwise, version 0 included. The checksum catches accidents, not
// tampering.
bound_file decode_bound_file(std::string_view);

} // namespace db::pitr
