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

// Filtering a counter cell on its write timestamp would change the value it
// represents, so the filter refuses rather than answer wrongly.
class counter_cells_unsupported : public std::runtime_error {
public:
    explicit counter_cells_unsupported(const sstring& what)
        : std::runtime_error(what.c_str()) {
    }
};

struct filter_stats {
    // Atomic cells and individual collection members.
    uint64_t cells_kept = 0;
    uint64_t cells_dropped = 0;
    uint64_t collection_tombstones_kept = 0;
    uint64_t collection_tombstones_dropped = 0;
    uint64_t row_markers_kept = 0;
    uint64_t row_markers_dropped = 0;
    // The two halves of a row_tombstone are counted apart, as they are filtered apart.
    uint64_t row_tombstones_kept = 0;
    uint64_t row_tombstones_dropped = 0;
    uint64_t range_tombstones_kept = 0;
    uint64_t range_tombstones_dropped = 0;
    uint64_t partition_tombstones_kept = 0;
    uint64_t partition_tombstones_dropped = 0;
    uint64_t rows_dropped = 0;

    // Cells whose collection tombstone sits at exactly `t`, the only `t` where
    // the statement behind it changes the answer and is unrecoverable from the
    // bytes; callers report them as unverified. The shapes are the first two
    // limits below. A plain delete at `t` counts, though its answer is right.
    uint64_t ambiguous_collections_at_t = 0;
    uint64_t ambiguous_collection_overwrites_at_t = 0;
    uint64_t ambiguous_collection_deletes_at_t = 0;

    uint64_t total_kept() const {
        return cells_kept + collection_tombstones_kept + row_markers_kept + row_tombstones_kept + range_tombstones_kept + partition_tombstones_kept;
    }
    uint64_t total_dropped() const {
        return cells_dropped + collection_tombstones_dropped + row_markers_dropped + row_tombstones_dropped + range_tombstones_dropped +
               partition_tombstones_dropped;
    }

    bool operator==(const filter_stats&) const = default;

    filter_stats& operator+=(const filter_stats& o);
};

struct filter_result {
    // May be empty, which the caller skips rather than applies.
    mutation m;
    filter_stats stats;
};

// Returns `m` reduced to the writes at or below `t`, per cell. An item exactly
// at `t` is kept, so a delete and an insert tied at `t` both survive and the tie
// resolves in the target as it did in the source. Cells are copied verbatim, so
// a TTL is not rewound: one already expired stays expired.
//
// The tombstone of a non-frozen collection or UDT is the one item not filtered
// on its own timestamp. Overwriting such a column writes it one microsecond
// below the statement's timestamp and the new members at it
// (update_parameters::make_tombstone_just_before()), so a tombstone with a
// member one microsecond above it goes with that statement. Otherwise a `t`
// between the two keeps the delete and drops the data it came with, emptying a
// collection nothing ever emptied.
//
// Three cases that rule gets wrong, because nothing in a mutation records where
// one statement ended and the next began. Limits of the data, not of the code;
// frozen collections and UDTs are unaffected.
//  - An overwrite with an empty value is what a plain delete writes, and is
//    kept as a delete.
//  - A batch merges its statements into one mutation per key, each keeping its
//    timestamp, so a delete at X batched with a member write at X + 1 is the
//    bytes of one overwrite at X + 1: at `t` = X older elements survive.
//  - Merging collection mutations drops members covered by the other side's
//    tombstone (collection_mutation.cc), so what a batch overwrote within
//    itself is gone before replay reads it, invisible and uncounted.
//
// Views into `m` are held across the yields, so `m` must not be mutated during
// the call, nor live in an LSA region where a yield lets compaction move it.
//
// Yields between rows, between cells and every few hundred collection members.
// Materialising the mutation first can stall where this does not:
// unfreeze_gently() and apply_gently() (mutation/async_utils.hh) preempt,
// schema conversion and apply_in_memory() do not.
seastar::future<filter_result> filter_at_timestamp(const mutation& m, api::timestamp_type t);

} // namespace db::pitr

template <>
struct fmt::formatter<db::pitr::filter_stats> : fmt::formatter<string_view> {
    auto format(const db::pitr::filter_stats&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
