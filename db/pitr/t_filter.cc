/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/t_filter.hh"

#include <optional>
#include <utility>
#include <vector>

#include <seastar/coroutine/maybe_yield.hh>

#include "mutation/atomic_cell.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/mutation_partition.hh"
#include "schema/schema.hh"
#include "utils/chunked_vector.hh"

namespace db::pitr {

// How many collection members are walked between preemption points. A
// collection (or non-frozen UDT) can hold a very large number of members, so
// walking one is bounded the same way the rest of the filter is.
static constexpr size_t collection_members_per_yield = 1024;

filter_stats& filter_stats::operator+=(const filter_stats& o) {
    cells_kept += o.cells_kept;
    cells_dropped += o.cells_dropped;
    collection_tombstones_kept += o.collection_tombstones_kept;
    collection_tombstones_dropped += o.collection_tombstones_dropped;
    row_markers_kept += o.row_markers_kept;
    row_markers_dropped += o.row_markers_dropped;
    row_tombstones_kept += o.row_tombstones_kept;
    row_tombstones_dropped += o.row_tombstones_dropped;
    range_tombstones_kept += o.range_tombstones_kept;
    range_tombstones_dropped += o.range_tombstones_dropped;
    partition_tombstones_kept += o.partition_tombstones_kept;
    partition_tombstones_dropped += o.partition_tombstones_dropped;
    rows_dropped += o.rows_dropped;
    ambiguous_collections_at_t += o.ambiguous_collections_at_t;
    ambiguous_collection_overwrites_at_t += o.ambiguous_collection_overwrites_at_t;
    ambiguous_collection_deletes_at_t += o.ambiguous_collection_deletes_at_t;
    return *this;
}

// Filters one collection (or non-frozen UDT) cell. Every member is filtered on
// its own timestamp; the collection-level tombstone is filtered on the
// timestamp of the statement it belongs to, which is not always its own, and
// which cannot always be recovered from the bytes. The rule and both of its
// ambiguous cases are described above filter_at_timestamp() in t_filter.hh.
// Returns nothing when neither the tombstone nor any member is at or below T,
// in which case the whole cell is dropped.
static future<std::optional<collection_mutation>> filter_collection(collection_mutation_view cmv, api::timestamp_type t, filter_stats& st) {
    auto tomb = cmv.tomb();
    // The views below point into `cmv`, which the caller guarantees is not
    // mutated and is not LSA-managed, so they stay valid across the yields.
    utils::chunked_vector<std::pair<managed_bytes_view, atomic_cell_view>> kept;
    bool member_just_above_tomb = false;
    size_t walked = 0;
    for (auto&& kv : cmv) {
        if (tomb && tomb.timestamp != api::max_timestamp && kv.second.timestamp() == tomb.timestamp + 1) {
            member_just_above_tomb = true;
        }
        // A dead member keeps its timestamp too, so live and dead members are
        // filtered the same way.
        if (kv.second.timestamp() <= t) {
            kept.emplace_back(kv.first, kv.second);
            ++st.cells_kept;
        } else {
            ++st.cells_dropped;
        }
        if (++walked % collection_members_per_yield == 0) {
            co_await coroutine::maybe_yield();
        }
    }

    if (tomb) {
        // Overwriting a non-frozen collection or UDT ("SET c = ...") writes the
        // tombstone one microsecond *before* the statement's timestamp and the
        // new members at it (update_parameters::make_tombstone_just_before(),
        // cql3/maps.cc, sets.cc, lists.cc, user_types.cc). The tombstone is
        // therefore part of the statement above it, and filtering it on its own
        // timestamp would keep the delete while dropping the data it was
        // written with, emptying a collection that no statement ever emptied.
        //
        // Two shapes are indistinguishable from an overwrite in the bytes and
        // are therefore decided the same way, which is wrong for them; both are
        // stated limits, pending a decision on whether to carry statement
        // boundaries through the ledger. See t_filter.hh.
        //  - an overwrite with an empty or null value writes what a plain
        //    "DELETE c" writes, and is kept as a delete;
        //  - a BATCH holding "DELETE c" at X and a member write at X + 1 merges
        //    into one cell (eventual_consistency::batch_executor.cc), and the
        //    delete is dropped with the member.
        // One microsecond either side of the tombstone the two readings agree,
        // so only a T exactly on it is ambiguous.
        if (tomb.timestamp == t) {
            ++st.ambiguous_collections_at_t;
            if (member_just_above_tomb) {
                ++st.ambiguous_collection_overwrites_at_t;
            } else {
                ++st.ambiguous_collection_deletes_at_t;
            }
        }
        const auto statement_timestamp = member_just_above_tomb ? tomb.timestamp + 1 : tomb.timestamp;
        if (statement_timestamp <= t) {
            ++st.collection_tombstones_kept;
        } else {
            ++st.collection_tombstones_dropped;
            tomb = tombstone();
        }
    }

    collection_mutation_writer writer(tomb);
    walked = 0;
    for (auto& kv : kept) {
        writer.push_back(kv.first, kv.second);
        if (++walked % collection_members_per_yield == 0) {
            co_await coroutine::maybe_yield();
        }
    }
    if (writer.empty()) {
        co_return std::nullopt;
    }
    co_return std::move(writer).finish();
}

// Filters the cells of one row (static or regular) from `in` into `out`.
static future<> filter_cells(const schema& s, column_kind kind, const row& in, row& out, api::timestamp_type t, filter_stats& st) {
    // for_each_cell() is synchronous, so collect the cells first and then walk
    // them with preemption points. The pointers stay valid because `in` is not
    // mutated for the duration of the filter.
    std::vector<std::pair<column_id, const atomic_cell_or_collection*>> cells;
    cells.reserve(in.size());
    in.for_each_cell([&](column_id id, const atomic_cell_or_collection& cell) {
        cells.emplace_back(id, &cell);
    });

    for (auto& [id, cell] : cells) {
        const column_definition& cdef = s.column_at(kind, id);
        if (cdef.is_counter()) {
            // Counters are refused by the restore path before replay starts;
            // if one reaches the filter anyway, fail rather than return a
            // value that was never written.
            throw counter_cells_unsupported(
                    format("pitr: the T filter cannot filter the counter column {}.{}.{}", s.ks_name(), s.cf_name(), cdef.name_as_text()));
        }
        if (cdef.is_atomic()) {
            // Covers live and dead (deleted) cells alike. A kept cell is
            // copied verbatim, so its TTL and expiry are the original ones:
            // the filter rewinds the write history, not the clock.
            if (cell->as_atomic_cell(cdef).timestamp() <= t) {
                out.append_cell(id, cell->copy(*cdef.type));
                ++st.cells_kept;
            } else {
                ++st.cells_dropped;
            }
        } else if (auto filtered = co_await filter_collection(cell->as_collection_mutation(), t, st)) {
            out.append_cell(id, atomic_cell_or_collection(std::move(*filtered)));
        }
        co_await coroutine::maybe_yield();
    }
}

// Filters a row tombstone. The regular and the shadowable half are compared
// with T separately, so a row_tombstone is never kept or dropped as one unit
// (see the rules above row_tombstone in mutation/mutation_partition.hh).
//
// The invariant that the shadowable tombstone is >= the regular one is
// preserved: dropping the shadowable half falls back to the (possibly also
// dropped) regular one, and if the regular half is above T then so is the
// shadowable one, which is >= it.
static row_tombstone filter_row_tombstone(row_tombstone rt, api::timestamp_type t, filter_stats& st) {
    if (!rt) {
        return rt;
    }
    tombstone regular = rt.regular();
    if (regular) {
        if (regular.timestamp <= t) {
            ++st.row_tombstones_kept;
        } else {
            ++st.row_tombstones_dropped;
            regular = tombstone();
        }
    }
    shadowable_tombstone shadowable = rt.shadowable();
    if (rt.is_shadowable()) {
        // Only count the shadowable half when it carries information of its
        // own, i.e. when it is strictly above the regular tombstone.
        if (shadowable.tomb().timestamp <= t) {
            ++st.row_tombstones_kept;
        } else {
            ++st.row_tombstones_dropped;
            shadowable = shadowable_tombstone(regular);
        }
    } else {
        shadowable = shadowable_tombstone(regular);
    }
    return row_tombstone(regular, shadowable);
}

future<filter_result> filter_at_timestamp(const mutation& m, api::timestamp_type t) {
    const schema& s = *m.schema();
    const mutation_partition& in = m.partition();

    filter_result res{mutation(m.schema(), m.decorated_key()), {}};
    mutation_partition& out = res.m.partition();
    filter_stats& st = res.stats;

    out.set_static_row_continuous(in.static_row_continuous());

    if (auto pt = in.partition_tombstone(); pt) {
        if (pt.timestamp <= t) {
            out.apply(pt);
            ++st.partition_tombstones_kept;
        } else {
            ++st.partition_tombstones_dropped;
        }
    }

    {
        row static_cells;
        co_await filter_cells(s, column_kind::static_column, in.static_row().get(), static_cells, t, st);
        if (!static_cells.empty()) {
            out.static_row().maybe_create() = std::move(static_cells);
        }
    }

    // A range tombstone is one tombstone over a range of clustering keys, so
    // it is filtered as a unit.
    for (const auto& e : in.row_tombstones()) {
        const range_tombstone& rt = e.tombstone();
        if (rt.tomb.timestamp <= t) {
            out.apply_row_tombstone(s, range_tombstone(rt));
            ++st.range_tombstones_kept;
        } else {
            ++st.range_tombstones_dropped;
        }
        co_await coroutine::maybe_yield();
    }

    for (const rows_entry& e : in.clustered_rows()) {
        const deletable_row& src = e.row();

        auto marker = src.marker();
        if (!marker.is_missing()) {
            if (marker.timestamp() <= t) {
                ++st.row_markers_kept;
            } else {
                ++st.row_markers_dropped;
                marker = row_marker();
            }
        }

        auto tomb = filter_row_tombstone(src.deleted_at(), t, st);

        row cells;
        co_await filter_cells(s, column_kind::regular_column, src.cells(), cells, t, st);

        if (marker.is_missing() && !tomb && cells.empty() && !e.dummy()) {
            if (!src.empty()) {
                ++st.rows_dropped;
            }
            co_await coroutine::maybe_yield();
            continue;
        }

        // The source is walked in ascending key order, so appending is enough.
        // The per-entry cached range tombstone is derived from the range
        // tombstone list (which is filtered above) and is not carried over.
        deletable_row& dst = out.append_clustered_row(s, e.position(), e.dummy(), e.continuous());
        if (!marker.is_missing()) {
            dst.apply(marker);
        }
        if (tomb) {
            // Applies the filtered marker's shadowing, exactly as the write
            // path would for these two writes alone.
            dst.apply(tomb);
        }
        dst.cells() = std::move(cells);

        co_await coroutine::maybe_yield();
    }

    co_return std::move(res);
}

} // namespace db::pitr

auto fmt::formatter<db::pitr::filter_stats>::format(const db::pitr::filter_stats& st, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(),
            "{{cells {}/{}, collection tombstones {}/{}, markers {}/{}, row tombstones {}/{}, "
            "range tombstones {}/{}, partition tombstones {}/{}, rows dropped {}, "
            "ambiguous collections at T {} ({} overwrite, {} delete)}}",
            st.cells_kept, st.cells_dropped, st.collection_tombstones_kept, st.collection_tombstones_dropped, st.row_markers_kept, st.row_markers_dropped,
            st.row_tombstones_kept, st.row_tombstones_dropped, st.range_tombstones_kept, st.range_tombstones_dropped, st.partition_tombstones_kept,
            st.partition_tombstones_dropped, st.rows_dropped, st.ambiguous_collections_at_t, st.ambiguous_collection_overwrites_at_t,
            st.ambiguous_collection_deletes_at_t);
}
