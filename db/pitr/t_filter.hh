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
    explicit counter_cells_unsupported(const sstring& what)
        : std::runtime_error(what.c_str()) {
    }
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

    // Collection (or non-frozen UDT) cells whose tombstone sits at exactly T,
    // which is the only T at which the statement that wrote it cannot be
    // identified from the bytes and the two readings disagree. One per cell,
    // not per member. This is what the restore result reports as "Not
    // verified: N collections may differ at this exact T" (RST-7, limit 3);
    // the filter only counts, it does not refuse and does not change what it
    // produces.
    //
    // ambiguous_collections_at_t is the sum of the two shapes below.
    uint64_t ambiguous_collections_at_t = 0;
    // A tombstone at T with a member at T + 1 in the same cell. Read as an
    // overwrite at T + 1 and dropped; if it was a DELETE at T batched with
    // that member, elements deleted at T wrongly survive.
    uint64_t ambiguous_collection_overwrites_at_t = 0;
    // A tombstone at T with no member at T + 1. Read as a DELETE at T and
    // kept; if it was an overwrite at T + 1 with an empty or null value, the
    // collection wrongly reads as deleted instead of as it was before. A plain
    // DELETE whose timestamp happens to be exactly T is counted here too and
    // is a false alarm: see the note above filter_at_timestamp().
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
// One exception to "per cell": the tombstone of a non-frozen collection or UDT
// is filtered on the timestamp of the *statement* it belongs to. Overwriting
// such a column ("SET c = ...") writes the tombstone one microsecond before the
// statement's timestamp and the new members at it
// (update_parameters::make_tombstone_just_before()), so a tombstone with a
// member exactly one microsecond above it in the same cell is taken to belong
// to that statement and goes with it. Without this, T between the two would
// keep the delete and drop the data written with it, emptying a collection that
// no statement ever emptied.
//
// The cost is two shapes which the bytes cannot tell apart from an overwrite,
// so both are decided by the rule above and both are decided wrongly. They are
// stated limits of the filter, not bugs in it: the statement boundary is simply
// not recorded anywhere the filter can read. Frozen collections and UDTs are
// plain atomic cells and are not affected by either.
//
//  1. Overwriting a non-frozen collection or UDT with an empty or null value
//     writes exactly what a plain "DELETE c" writes: a tombstone just below the
//     statement's timestamp, with no members. With no member above it, it is
//     taken at face value and kept as a delete, so a T between the overwrite's
//     tombstone and its timestamp keeps a delete the user did not issue then.
//
//  2. A BATCH merges its statements' mutations into one mutation per key
//     (cql3/statements/eventual_consistency/batch_executor.cc), each statement
//     keeping its own timestamp. A batch holding "DELETE c" at X and a write of
//     a member at X + 1 therefore produces the same bytes as one overwrite at
//     X + 1, and the delete is dropped together with the member. At T = X the
//     collection's older members survive, where the real "DELETE c" at X should
//     have removed them. test_batched_delete_then_member_is_a_stated_limit
//     pins the behaviour.
//
//  3. A loss the filter never sees at all, and therefore cannot count. Merging
//     two collection mutations drops the members covered by the other side's
//     tombstone (collection_mutation.cc, merge() calls filter_cells() with the
//     opposite tombstone), so a batch of "SET c = {a}" at X and "DELETE c" at
//     X + 9 arrives as a tombstone at X + 9 with no members at all: {a} is
//     gone before replay reads the segment. At T = X the restored collection is
//     empty where it should hold {a}, the shape looks like an ordinary delete,
//     and the ambiguity count below is 0. Nothing in the filter can recover it;
//     the information is destroyed at write time, by the same merge that any
//     coordinator does. The only fix is upstream of replay (recording statement
//     boundaries), which is out of scope here.
//     test_batch_overwriting_itself_loses_members_before_the_filter pins it.
//
// Shapes 1 and 2 only ever give a different answer when T falls exactly on the
// tombstone's timestamp: one microsecond either side, the two readings of the
// bytes agree. So filter_stats counts, per collection cell, the cells whose
// tombstone sits exactly at T, which is what the restore result reports as
// "Not verified: N collections may differ at this exact T" (RST-7, limit 3).
// The filter does not refuse such a mutation and does not change what it
// produces for it.
//
// That count is deliberately noisy in one direction: a plain "DELETE c" whose
// own timestamp is exactly T is byte-identical to an empty overwrite at T + 1,
// so it is counted although its answer is right. Nothing in the mutation can
// separate the two, and the alternative - counting only shape 2 - would stay
// silent exactly when the answer is wrong. The least noisy rule that is still
// safe is the one implemented here: count only when T equals the tombstone's
// timestamp, and report the two shapes separately, so a caller which cares can
// say "a collection was deleted or overwritten at exactly this microsecond"
// for the common case and keep the stronger warning for shape 1. Users are
// told to retry one microsecond off, where no ambiguity exists.
//
// It is also incomplete in the other direction, and cannot be completed: it
// says nothing about shape 3, where the members a batch overwrote within
// itself are already gone from the mutation. Retrying with a different T does
// not bring them back either.
//
// A cell kept by the filter keeps its original TTL and expiry: the filter
// rewinds the write history, not the clock, so a cell which has already
// expired stays expired.
//
// Throws counter_cells_unsupported if `m`'s schema has counter columns which
// are actually set in `m`.
//
// Yields between clustering rows and while walking a large collection, so a
// huge mutation does not stall the reactor. `m` is not modified, but views into
// it are held across those yields, so `m` must not be mutated for the duration
// of the call and must not live in an LSA region (where a yield lets LSA
// compaction move it). A mutation built on the heap, as commitlog replay builds
// one, satisfies both; a partition owned by a memtable or the row cache does
// not.
//
// On what it costs to get here: replay reads a frozen_mutation, so something
// has to materialise it first. unfreeze_gently() and apply_gently()
// (mutation/async_utils.hh) do that with preemption points, and strict replay
// should use them. What has no gentle path is conversion to another schema
// version (converting_mutation_partition_applier is a synchronous
// mutation_partition_visitor) and replica::database::apply_in_memory(), so for
// one oversized partition the conversion before this filter and the apply after
// it are the unpreemptable parts, not the filter. Unmeasured here; worth
// measuring against the largest permitted write (task 1.3.3).
seastar::future<filter_result> filter_at_timestamp(const mutation& m, api::timestamp_type t);

} // namespace db::pitr

template <>
struct fmt::formatter<db::pitr::filter_stats> : fmt::formatter<string_view> {
    auto format(const db::pitr::filter_stats&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
