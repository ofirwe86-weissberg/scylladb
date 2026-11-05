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

// Forwards to the converting applier, refusing every cell it would skip in silence. The tests behind it: a column the
// target lacks, one whose type cannot hold the source's values, and one the target has dropped.
class checking_converting_applier : public mutation_partition_view_virtual_visitor {
    converting_mutation_partition_applier _inner;
    const column_mapping& _mapping;
    const schema& _target;
    const db::replay_position _position;

public:
    checking_converting_applier(const column_mapping& mapping, const schema& target, mutation_partition& into, db::replay_position pos)
        : _inner(mapping, target, into)
        , _mapping(mapping)
        , _target(target)
        , _position(pos) {
    }

    void accept_partition_tombstone(tombstone t) override {
        _inner.accept_partition_tombstone(t);
    }
    void accept_static_cell(column_id id, atomic_cell cell) override {
        check(_mapping.static_column_at(id), column_kind::static_column);
        _inner.accept_static_cell(id, atomic_cell_view(cell));
    }
    void accept_static_cell(column_id id, collection_mutation collection) override {
        check(_mapping.static_column_at(id), column_kind::static_column);
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
        check(_mapping.regular_column_at(id), column_kind::regular_column);
        _inner.accept_row_cell(id, atomic_cell_view(cell));
    }
    void accept_row_cell(column_id id, collection_mutation collection) override {
        check(_mapping.regular_column_at(id), column_kind::regular_column);
        _inner.accept_row_cell(id, collection_mutation_view(collection));
    }

private:
    static sstring column_name(const column_mapping_entry& col) {
        return sstring(reinterpret_cast<const char*>(col.name().data()), col.name().size());
    }

    // The same three tests converting_mutation_partition_applier applies before dropping a cell, in the same order.
    void check(const column_mapping_entry& col, column_kind kind) const {
        const column_definition* def = _target.get_column_definition(col.name());
        if (!def) {
            throw target_schema_mismatch(
                    fmt::format("target {}.{} has no column {} written at {}", _target.ks_name(), _target.cf_name(), column_name(col), _position));
        }
        if (!::is_compatible(def->kind, kind) || !def->type->is_value_compatible_with(*col.type())) {
            throw target_schema_mismatch(fmt::format("target {}.{} holds column {} as {} {}, which cannot hold the {} {} written at {}", _target.ks_name(),
                    _target.cf_name(), column_name(col), to_sstring(def->kind), def->type->name(), to_sstring(kind), col.type_name(), _position));
        }
        if (def->dropped_at() != api::missing_timestamp) {
            throw target_schema_mismatch(fmt::format("target {}.{} dropped column {} at {}, so a cell written at {} may be lost", _target.ks_name(),
                    _target.cf_name(), column_name(col), def->dropped_at(), _position));
        }
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
    for (const auto shard : shards) {
        const auto outcome = co_await _db.invoke_on(shard, [this, &e](replica::database& db) -> future<entry_outcome> {
            auto& cf = db.find_column_family(_target);
            const schema_ptr s = cf.schema();
            if (const auto err = validation::is_cql_key_invalid(*s, e.mutation.key()); err) {
                throw target_schema_mismatch(fmt::format("entry at {} has a key invalid for target {}.{}: {}", e.position, s->ks_name(), s->cf_name(), *err));
            }
            // The reader's mapping lives on the shard that reads; this shard converts with its own copy.
            const column_mapping& mapping = _mappings.local().get(e.mutation.schema_version(), e.mapping);
            mutation m(s, e.mutation.decorated_key(*s));
            checking_converting_applier v(mapping, *s, m.partition(), e.position);
            e.mutation.partition().accept(mapping, v);

            auto filtered = co_await filter_at_timestamp(m, _t);
            // An entry whose every write is above T is counted like any other: what it dropped is part of the result.
            if (filtered.m.partition().empty()) {
                co_return entry_outcome{.stats = filtered.stats, .applied = false};
            }
            co_await db.apply_in_memory(filtered.m, cf, db::rp_handle(), db::no_timeout);
            co_return entry_outcome{.stats = filtered.stats, .applied = true};
        });
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
