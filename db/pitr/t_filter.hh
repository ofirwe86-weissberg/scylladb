/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <stdexcept>

#include <seastar/core/future.hh>

#include "mutation/mutation.hh"
#include "mutation/timestamp.hh"

namespace db::pitr {

// Thrown when the filter is handed a cell of a counter column. Counters are
// out of scope for PITR restore (the restore path refuses counter tables
// up-front), and a counter cell cannot be filtered on its write timestamp
// without changing the value it represents, so the filter fails loudly
// instead of returning a wrong answer.
class counter_cells_unsupported : public std::runtime_error {
public:
    explicit counter_cells_unsupported(const sstring& what) : std::runtime_error(what.c_str()) { }
};

// What the filter kept and what it threw away. Every counter counts
// individually timestamped items, which is what the restore result reports
// (see design-must.md section 6, LVL-4).
struct filter_stats {
    // Atomic cells (regular and static) plus individual collection members.
    uint64_t cells_kept = 0;
    uint64_t cells_dropped = 0;
    // Collection-level (and non-frozen UDT-level) tombstones.
    uint64_t collection_tombstones_kept = 0;
    uint64_t collection_tombstones_dropped = 0;
    uint64_t row_markers_kept = 0;
    uint64_t row_markers_dropped = 0;
    // The regular and the shadowable half of a row_tombstone are counted
    // separately, because they are filtered separately.
    uint64_t row_tombstones_kept = 0;
    uint64_t row_tombstones_dropped = 0;
    uint64_t range_tombstones_kept = 0;
    uint64_t range_tombstones_dropped = 0;
    uint64_t partition_tombstones_kept = 0;
    uint64_t partition_tombstones_dropped = 0;
    // Clustering rows which held nothing at or below T and are therefore
    // absent from the result.
    uint64_t rows_dropped = 0;

    uint64_t total_kept() const {
        return cells_kept + collection_tombstones_kept + row_markers_kept + row_tombstones_kept
                + range_tombstones_kept + partition_tombstones_kept;
    }
    uint64_t total_dropped() const {
        return cells_dropped + collection_tombstones_dropped + row_markers_dropped + row_tombstones_dropped
                + range_tombstones_dropped + partition_tombstones_dropped;
    }

    bool operator==(const filter_stats&) const = default;

    filter_stats& operator+=(const filter_stats& o);
};

struct filter_result {
    // The input mutation with everything written after T removed. Holds the
    // same key and schema as the input. May be empty (no writes at all), which
    // the caller is expected to skip rather than apply.
    mutation m;
    filter_stats stats;
};

// Returns `m` reduced to the writes whose timestamp is at or below `t`,
// per cell. A write exactly at T is kept, which resolves a delete and an
// insert tied at T the same way the normal write path does (the tombstone
// wins over the cell at an equal timestamp, see compare_atomic_cell_for_merge).
//
// Filtered separately: the partition tombstone, range tombstones, the regular
// and the shadowable half of every row tombstone, row markers, live and dead
// atomic cells (static and regular), collection/UDT tombstones and individual
// collection/UDT members.
//
// A cell kept by the filter keeps its original TTL and expiry: the filter
// rewinds the write history, not the clock, so a cell which has already
// expired stays expired.
//
// Throws counter_cells_unsupported if `m`'s schema has counter columns which
// are actually set in `m`.
//
// Yields between clustering rows and while walking a large collection, so a
// huge mutation does not stall the reactor. The caller must not mutate `m`
// across the call; `m` is not modified.
seastar::future<filter_result> filter_at_timestamp(const mutation& m, api::timestamp_type t);

} // namespace db::pitr

template <> struct fmt::formatter<db::pitr::filter_stats> : fmt::formatter<string_view> {
    auto format(const db::pitr::filter_stats&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
