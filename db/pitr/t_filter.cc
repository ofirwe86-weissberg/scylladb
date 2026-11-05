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

bool is_member_just_above_tombstone(tombstone tomb, api::timestamp_type member_timestamp) {
    return tomb && tomb.timestamp != api::max_timestamp && member_timestamp == tomb.timestamp + 1;
}

api::timestamp_type collection_statement_timestamp(tombstone tomb, bool member_just_above_tombstone) {
    return member_just_above_tombstone ? tomb.timestamp + 1 : tomb.timestamp;
}

// Returns nothing when nothing in the cell is at or below `t`, so that the
// caller drops the whole cell.
static future<std::optional<collection_mutation>> filter_collection(collection_mutation_view cmv, api::timestamp_type t, filter_stats& st) {
    auto tomb = cmv.tomb();
    // The views below point into `cmv`, which the caller guarantees is not
    // mutated and is not LSA-managed, so they stay valid across the yields.
    utils::chunked_vector<std::pair<managed_bytes_view, atomic_cell_view>> kept;
    bool member_just_above_tomb = false;
    for (auto&& kv : cmv) {
        if (is_member_just_above_tombstone(tomb, kv.second.timestamp())) {
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
        co_await coroutine::maybe_yield();
    }

    if (tomb) {
        // A collection overwrite writes its tombstone one microsecond before
        // its members (update_parameters::make_tombstone_just_before()), so the
        // tombstone belongs to the statement above it. Filtering it on its own
        // timestamp would empty a collection no statement emptied. Some shapes
        // look the same in the bytes, and only a `t` exactly on the tombstone
        // tells them apart: see the limits in t_filter.hh.
        if (tomb.timestamp == t) {
            ++st.ambiguous_collections_at_t;
            if (member_just_above_tomb) {
                ++st.ambiguous_collection_overwrites_at_t;
            } else {
                ++st.ambiguous_collection_deletes_at_t;
            }
        }
        const auto statement_timestamp = collection_statement_timestamp(tomb, member_just_above_tomb);
        if (statement_timestamp <= t) {
            ++st.collection_tombstones_kept;
        } else {
            ++st.collection_tombstones_dropped;
            tomb = tombstone();
        }
    }

    collection_mutation_writer writer(tomb);
    for (auto& kv : kept) {
        writer.push_back(kv.first, kv.second);
        co_await coroutine::maybe_yield();
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

// The two halves are compared with `t` apart, never as one unit (see the rules
// above row_tombstone in mutation/mutation_partition.hh). That keeps shadowable
// >= regular: dropping the shadowable half falls back to the regular one, and a
// regular half above `t` implies a shadowable one above it too.
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
