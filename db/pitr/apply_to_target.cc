/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/apply_to_target.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include "locator/abstract_replication_strategy.hh"
#include "mutation/converting_mutation_partition_applier.hh"
#include "mutation/mutation_partition_view.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "replica/database.hh"
#include "schema/schema.hh"
#include "utils/log.hh"
#include "validation.hh"

namespace db::pitr {

static logging::logger alogger("pitr_apply_to_target");

const column_mapping& shard_mapping_cache::get(table_schema_version version, const column_mapping& source) {
    return _mappings.try_emplace(version, source).first->second;
}

future<> shard_mapping_cache::stop() {
    return make_ready_future<>();
}

namespace {

// What one shard made of one entry.
struct entry_outcome {
    filter_stats stats;
    bool applied = false;
};

// Forwards to the converting applier, refusing every cell it would skip in silence: one whose column the target lacks or
// holds with an incompatible type. A cell written above T is not refused, since the filter would drop it anyway: a column
// added after T (REF-9) is expected to be missing from the target, so the decision is per cell, not per entry.
class checking_converting_applier : public mutation_partition_view_virtual_visitor {
    // Why the target cannot hold one column of the mapping, or nothing when it can.
    using verdict = std::optional<sstring>;

    converting_mutation_partition_applier _inner;
    const column_mapping& _mapping;
    const schema& _target;
    const db::replay_position _position;
    const api::timestamp_type _t;
    // One entry can hold many cells of one column, and the verdict costs a lookup by name, so keep it per column.
    std::vector<std::optional<verdict>> _static_verdicts;
    std::vector<std::optional<verdict>> _regular_verdicts;
    // What this visitor dropped before the filter ran, in the filter's own units, so the two add up.
    filter_stats _dropped;

public:
    checking_converting_applier(const column_mapping& mapping, const schema& target, mutation_partition& into, db::replay_position pos, api::timestamp_type t)
        : _inner(mapping, target, into)
        , _mapping(mapping)
        , _target(target)
        , _position(pos)
        , _t(t)
        , _static_verdicts(mapping.n_static())
        , _regular_verdicts(mapping.columns().size() - mapping.n_static()) {
    }

    const filter_stats& dropped_above_t() const {
        return _dropped;
    }

    void accept_partition_tombstone(tombstone t) override {
        _inner.accept_partition_tombstone(t);
    }
    void accept_static_cell(column_id id, atomic_cell cell) override {
        if (!accept_atomic(column_kind::static_column, id, atomic_cell_view(cell))) {
            return;
        }
        _inner.accept_static_cell(id, atomic_cell_view(cell));
    }
    void accept_static_cell(column_id id, collection_mutation collection) override {
        if (!accept_collection(column_kind::static_column, id, collection_mutation_view(collection))) {
            return;
        }
        _inner.accept_static_cell(id, collection_mutation_view(collection));
    }
    stop_iteration accept_row_tombstone(range_tombstone rt) override {
        _inner.accept_row_tombstone(rt);
        return stop_iteration::no;
    }
    stop_iteration accept_row(position_in_partition_view key, row_tombstone deleted_at, row_marker rm, is_dummy dummy, is_continuous continuous) override {
        _inner.accept_row(key, deleted_at, rm, dummy, continuous);
        return stop_iteration::no;
    }
    void accept_row_cell(column_id id, atomic_cell cell) override {
        if (!accept_atomic(column_kind::regular_column, id, atomic_cell_view(cell))) {
            return;
        }
        _inner.accept_row_cell(id, atomic_cell_view(cell));
    }
    void accept_row_cell(column_id id, collection_mutation collection) override {
        if (!accept_collection(column_kind::regular_column, id, collection_mutation_view(collection))) {
            return;
        }
        _inner.accept_row_cell(id, collection_mutation_view(collection));
    }

private:
    static sstring column_name(const column_mapping_entry& col) {
        return sstring(reinterpret_cast<const char*>(col.name().data()), col.name().size());
    }

    // The two tests converting_mutation_partition_applier applies before dropping a cell (:19, :38). Its third, a cell at
    // or below the target's dropped_at, is not one: a live column carries dropped_at exactly when it was dropped and
    // re-added, and then the applier hides the same cells the source hides, so the answer at T is right.
    verdict compute_verdict(const column_mapping_entry& col, column_kind kind) const {
        const column_definition* def = _target.get_column_definition(col.name());
        if (!def) {
            return fmt::format("target {}.{} has no column {}", _target.ks_name(), _target.cf_name(), column_name(col));
        }
        if (!::is_compatible(def->kind, kind) || !def->type->is_value_compatible_with(*col.type())) {
            return fmt::format("target {}.{} holds column {} as {} {}, which cannot hold the {} {}", _target.ks_name(), _target.cf_name(), column_name(col),
                    to_sstring(def->kind), def->type->name(), to_sstring(kind), col.type_name());
        }
        return std::nullopt;
    }

    const verdict& verdict_for(column_kind kind, column_id id) {
        const bool is_static = kind == column_kind::static_column;
        // Through the mapping first, which checks the id: an id past it, which corruption that passes the CRC can
        // produce, then raises an internal error rather than reading past the cache.
        const column_mapping_entry& col = is_static ? _mapping.static_column_at(id) : _mapping.regular_column_at(id);
        auto& slot = (is_static ? _static_verdicts : _regular_verdicts)[id];
        if (!slot) {
            slot = compute_verdict(col, kind);
        }
        return *slot;
    }

    // True when the cell goes on to the converting applier.
    bool accept_atomic(column_kind kind, column_id id, atomic_cell_view cell) {
        const auto& why = verdict_for(kind, id);
        if (!why) {
            return true;
        }
        if (cell.timestamp() > _t) {
            ++_dropped.cells_dropped;
            return false;
        }
        throw target_schema_mismatch(fmt::format("{}, written at or below T at {}", *why, _position));
    }

    bool accept_collection(column_kind kind, column_id id, collection_mutation_view collection) {
        const auto& why = verdict_for(kind, id);
        if (!why) {
            return true;
        }
        // One item at or below T means the restore would lose data; above T the whole column is the filter's to drop. The
        // tombstone goes by the statement it came with, as the filter has it, so an overwrite above T is not a delete at T.
        const auto tomb = collection.tomb();
        uint64_t members = 0;
        bool member_just_above_tomb = false;
        for (const auto& [key, c] : collection) {
            if (c.timestamp() <= _t) {
                throw target_schema_mismatch(fmt::format("{}, written at or below T at {}", *why, _position));
            }
            member_just_above_tomb = member_just_above_tomb || is_member_just_above_tombstone(tomb, c.timestamp());
            ++members;
        }
        // The shapes the filter cannot tell apart, counted as it counts them, so the result's not-verified lines are the
        // same whether the target holds the column or not.
        if (tomb && tomb.timestamp == _t) {
            ++_dropped.ambiguous_collections_at_t;
            if (member_just_above_tomb) {
                ++_dropped.ambiguous_collection_overwrites_at_t;
            } else {
                ++_dropped.ambiguous_collection_deletes_at_t;
            }
        }
        if (tomb && collection_statement_timestamp(tomb, member_just_above_tomb) <= _t) {
            throw target_schema_mismatch(fmt::format("{}, deleted at or below T at {}", *why, _position));
        }
        _dropped.cells_dropped += members;
        if (tomb) {
            ++_dropped.collection_tombstones_dropped;
        }
        return false;
    }
};

} // namespace

future<> target_applier::operator()(const replay_entry& e) {
    auto& table = _db.local().find_column_family(_target);
    const schema_ptr target_schema = table.schema();
    const auto token = e.mutation.token(*target_schema);
    const auto shards = table.get_effective_replication_map()->shard_for_writes(*target_schema, token);
    if (shards.empty()) {
        // The replica owns the key, since the restore refuses anything but RF=1 on one node, so nothing may fall out here.
        throw target_schema_mismatch(fmt::format("no shard owns token {} of the entry at {} in target {}", token, e.position, _target));
    }

    // Stats of the first shard only: every shard converts and filters the same bytes against the same target schema, so
    // counting each would multiply one entry's cells by the number of owners.
    std::optional<entry_outcome> counted;
    // A named local, passed by reference, and not a temporary argument to invoke_on: this is a coroutine lambda, so its
    // captures live in the closure, and invoke_on for the shard it is called from invokes the functor inline and lets it
    // go when it returns the unresolved future. The closure then dies at the end of that expression while the body is
    // suspended in the filter, and `this` and `e` dangle. A cross-shard call keeps the functor in its submitted task, so
    // only the owning shard that is also the replay's own shard lost its writes, which is what #103 was (silently: the
    // outcome it returns is a value, so the counters still looked right).
    auto apply_on_owner = [this, &e](replica::database& db) -> future<entry_outcome> {
        auto& cf = db.find_column_family(_target);
        const schema_ptr s = cf.schema();
        if (const auto err = validation::is_cql_key_invalid(*s, e.mutation.key()); err) {
            throw target_schema_mismatch(fmt::format("entry at {} has a key invalid for target {}.{}: {}", e.position, s->ks_name(), s->cf_name(), *err));
        }
        // The reader's mapping lives on the shard that reads; this shard converts with its own copy.
        const column_mapping& mapping = _mappings.local().get(e.mutation.schema_version(), e.mapping);
        mutation m(s, e.mutation.decorated_key(*s));
        checking_converting_applier v(mapping, *s, m.partition(), e.position, _t);
        e.mutation.partition().accept(mapping, v);

        auto filtered = co_await filter_at_timestamp(m, _t);
        // Cells of a column the target lacks that were above T anyway: dropped here, counted like the filter's own.
        filtered.stats += v.dropped_above_t();
        // An entry whose every write is above T is counted like any other: what it dropped is part of the result.
        if (filtered.m.partition().empty()) {
            co_return entry_outcome{.stats = filtered.stats, .applied = false};
        }
        co_await db.apply_in_memory(filtered.m, cf, db::rp_handle(), db::no_timeout);
        co_return entry_outcome{.stats = filtered.stats, .applied = true};
    };
    for (const auto shard : shards) {
        const auto outcome = co_await _db.invoke_on(shard, std::ref(apply_on_owner));
        if (!counted) {
            counted = outcome;
        }
    }

    const auto& fs = counted->stats;
    if (counted->applied) {
        ++_stats.entries_applied;
    } else {
        ++_stats.entries_empty_after_filter;
    }
    _stats.cells_applied += fs.total_kept();
    _stats.cells_dropped_after_t += fs.total_dropped();
    // ambiguous_collections_at_t is the whole count; the overwrite and delete counters classify the same items.
    _stats.cells_not_verified += fs.ambiguous_collections_at_t;
    alogger.trace("applied entry at {} to {}: {} kept, {} dropped", e.position, _target, fs.total_kept(), fs.total_dropped());
}

} // namespace db::pitr
