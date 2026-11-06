/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <vector>

#include <seastar/core/sstring.hh>

#include "db/commitlog/replay_position.hh"
#include "db/pitr/ledger_entry.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"
#include "utils/UUID.hh"

namespace db::pitr {

// One file per snapshot, written once by the snapshot orchestrator and uploaded
// with the backup. It is what restore takes the set of shards, their replay
// bounds and their chain ids from: never a bucket listing, so a shard whose
// directory is gone reads as a missing recorded chain and not as a shard that
// never shipped (#12, #15).
//
// The name must not parse as an sstable component, or loading the restored
// snapshot would try to open it as one.
constexpr const char* bound_file_name = "pitr-bound.txt";

// Refused before the file is read, and nothing preallocates per the declared
// shard count, so a damaged length costs a reader nothing. Never raised: a
// reader has to bound its read before it knows what it is reading, and a
// version 1 reader refuses a larger file, so raising it needs a new version.
constexpr size_t max_bound_file_size = 64 * 1024;

// What the cap leaves room for, at under 190 bytes a row: every shard of any
// machine Scylla runs on today. `encode` refuses past it, so a machine that
// outgrows it gets a failed snapshot and not an unreadable backup.
constexpr size_t max_shards_in_a_bound_file = 340;

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

// One shard's shipped chain as the restore of this backup needs it: the chain's
// key parts without the prefix, which the restore command supplies, plus the
// floor (decision P62). Everything else about the chain is in the chain's own
// objects, which are checksummed, so nothing else belongs here.
struct shard_chain {
    // Never null: a null id is what a zeroed file holds.
    utils::UUID host_id;
    int64_t boot_epoch_us = 0;
    int64_t chain_start_us = 0;
    chain_id chain;
    // The lowest segment id the chain still carries, so restore can tell a
    // chain that cannot reach this bound from one that can. 0 when the chain
    // has shipped nothing yet.
    uint64_t floor_segment_id = 0;

    bool operator==(const shard_chain&) const = default;
};

struct shard_bound {
    uint32_t shard = 0;
    // Where replay starts for this shard: at or below every write the
    // snapshot's sstables do not hold. See table::snapshot_sstables().
    db::replay_position bound;
    // `none` when the shard had no chain when the snapshot was taken, which is
    // every shard until the shipper is wired in (#8, piece 2b).
    std::optional<shard_chain> chain;

    bool operator==(const shard_bound&) const = default;
};

struct bound_file {
    // Microseconds since the UNIX epoch, UTC, never negative.
    int64_t capture_time_us = 0;
    // The table's schema at capture time, so a restore into another schema is
    // refused rather than replayed into a shape the mutations do not fit.
    table_schema_version schema_version;
    // Exactly one row per shard, in order 0..N-1, never empty.
    std::vector<shard_bound> shards;

    bool operator==(const bound_file&) const = default;
};

// Returns the file's bytes. Throws `bound_file_format_error` on anything the
// reader below would refuse: no shards, a shard out of order, a negative time,
// a null host or chain id. Linear in one node's shard count.
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
//     chain_floor_segment_id=<decimal>
//   crc32=<8 hex digits>
//
// Text, unlike the ledger's entries, because this is what an operator reads
// when a restore refuses, and it is written once per snapshot.
//
// The order of the checks is part of the format, so that a reader built today
// diagnoses every later version the same way: the size, then the `format` line,
// then the checksum, then the version, then the fields. The checksum comes
// before the version so that one flipped bit there is reported as damage and
// not as "written by a newer Scylla", which would send an operator upgrading.
//
// Decimal values carry no sign, no leading zero and no value that overflows
// their field. A UUID must be canonical and not null. Nothing may follow the
// checksum line. Throws `bound_file_format_error`, or
// `bound_file_unknown_version` for a newer version; restore refuses on both.
// The checksum is an accident check, not a defence against tampering.
bound_file decode_bound_file(std::string_view);

// What a snapshot asks its shard for. Installed from the shipper's chain
// (#8, piece 2b); while none is installed a shard reports no chain. It is
// called on the shard it belongs to and must not block.
using chain_provider = std::function<std::optional<shard_chain>()>;

} // namespace db::pitr
