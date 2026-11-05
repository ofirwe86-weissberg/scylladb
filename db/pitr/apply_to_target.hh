/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <stdexcept>
#include <unordered_map>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "db/pitr/replay_reader.hh"
#include "db/pitr/t_filter.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace replica {
class database;
}

namespace db::pitr {

// The target cannot hold what the source wrote, so applying the entry would lose it. The converting applier skips such a
// cell in silence (mutation/converting_mutation_partition_applier.cc:38), and a target built from the backup's schema has
// every column of every version replayed into it, so this is an operator error, not data to count and carry on from.
class target_schema_mismatch : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

struct apply_stats {
    uint64_t entries_applied = 0;
    // Everything at or below T was already in the snapshot, or the entry held nothing at all: nothing is applied.
    uint64_t entries_empty_after_filter = 0;
    uint64_t cells_applied = 0;
    uint64_t cells_dropped_after_t = 0;
    // Collections the filter could not resolve from the bytes alone, from filter_stats::ambiguous_collections_at_t; the
    // result reports them as not verified.
    uint64_t cells_not_verified = 0;

    bool operator==(const apply_stats&) const = default;
};

// Per shard, the source mappings already copied to it, by the source's schema version. A copy crosses shards cheaply but
// rebuilds every column's type from its name (column_mapping_entry's constructor), so entry after entry on the same
// version pays for it once. Start and stop it around the replay; one instance serves the whole replay.
class shard_mapping_cache {
    std::unordered_map<table_schema_version, column_mapping> _mappings;

public:
    // `source` is the mapping as the reader resolved it, which may live on another shard; the returned reference is this
    // shard's copy and stays valid until stop().
    const column_mapping& get(table_schema_version version, const column_mapping& source);

    future<> stop();
};

// A replay_consumer that applies each entry to `target`, which may be named differently from the source table and carry a
// different schema version. Lives on the shard that runs the replay; the work of each entry happens on the shard that
// owns its key.
//
// Per entry: resolve the owning shards from the target's effective replication map (as commitlog_replayer::recover does,
// via shard_for_writes), convert the frozen mutation to the target's schema there with
// converting_mutation_partition_applier, refuse anything the target cannot hold, drop what is newer than T with
// filter_at_timestamp, and apply the rest to the memtable. A write that the target owns on more than one shard, which
// tablet migration can produce, is converted and applied on each of them and counted once; vnodes with RF=1, which is
// all this restore allows, never give more than one owner, so no test here covers that.
//
// Refuses, rather than applying part of an entry: a key invalid for the target's schema, and a cell at or below T whose
// column the target lacks or holds with an incompatible type. Above T such a cell is counted as dropped after T and
// skipped, since the filter would drop it anyway and a column added after T is expected to be missing from the target
// (REF-9). A column the target dropped and re-added is not refused: the converting applier hides cells at or below its
// dropped_at, which is what the source does too, so the answer at T is right. Not checked, because a column_mapping
// carries no key columns: that the target's key types match the source's. A key of another shape is caught by the key
// validation, one of compatible shape and other meaning is not.
//
// Stalls: converting one entry and applying it have no preemption point, so a single large write costs the shard that
// much. The filter yields. Measuring the largest write is #17's next piece.
class target_applier {
    sharded<replica::database>& _db;
    sharded<shard_mapping_cache>& _mappings;
    table_id _target;
    api::timestamp_type _t;
    apply_stats _stats;

public:
    target_applier(sharded<replica::database>& db, sharded<shard_mapping_cache>& mappings, table_id target, api::timestamp_type t)
        : _db(db)
        , _mappings(mappings)
        , _target(target)
        , _t(t) {
    }

    future<> operator()(const replay_entry& e);

    const apply_stats& stats() const {
        return _stats;
    }
};

} // namespace db::pitr
