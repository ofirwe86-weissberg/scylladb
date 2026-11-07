/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>
#include <unordered_set>
#include <utility>

#include <seastar/core/sstring.hh>

#include "schema/schema_fwd.hh"
#include "seastarx.hh"

// What a caller needs to name a run and keep two of them apart: the loader's
// header includes this and not `replay_to.hh`, which would pull a JSON parser
// into every translation unit that sees the loader.

namespace db::pitr {

// The endpoint's parameters as they arrived. T stays text: a malformed one is
// ordinary user input and comes back as a refusal, not a bad request. No list
// of sstables: a replay takes them from the backup itself.
struct replay_to_params {
    // The same three the restore endpoint takes.
    sstring endpoint;
    sstring bucket;
    sstring prefix;
    // The target the backup is replayed into, which the user created.
    sstring keyspace;
    sstring table;
    // The backup, as its snapshot was named; recorded in the mark.
    sstring snapshot;
    sstring t;
};

// The targets a replay is running into on this node, so a second call on one is
// refused rather than writing over the first one's work (P62). One instance, on
// shard 0, where every replay's checks and mark run.
class replay_in_flight {
    std::unordered_set<::table_id> _targets;

public:
    // Released on every path out of the run, a failure and a shutdown too.
    class guard {
        replay_in_flight* _owner = nullptr;
        ::table_id _target;

    public:
        guard() = default;
        guard(replay_in_flight& owner, ::table_id target) noexcept
            : _owner(&owner)
            , _target(target) {
        }
        guard(const guard&) = delete;
        guard& operator=(const guard&) = delete;
        guard(guard&& o) noexcept
            : _owner(std::exchange(o._owner, nullptr))
            , _target(o._target) {
        }
        guard& operator=(guard&&) = delete;
        ~guard();
    };

    // `nullopt` when a replay is already running into this target.
    std::optional<guard> try_take(::table_id target);
    bool holds(::table_id target) const {
        return _targets.contains(target);
    }
};

// What a shard keeps for each target a replay is running into, so that two
// replays into two tables do not share one slot: one would replace the other's
// hold, and the first to finish would release what the other still needs.
template <typename Hold>
class per_target {
    std::unordered_map<::table_id, Hold> _held;

public:
    void add(::table_id target, Hold hold) {
        _held.insert_or_assign(target, std::move(hold));
    }
    // Only this target's, which is the whole point.
    void erase(::table_id target) {
        _held.erase(target);
    }
    const Hold* find(::table_id target) const {
        auto it = _held.find(target);
        return it == _held.end() ? nullptr : &it->second;
    }
    size_t size() const {
        return _held.size();
    }
};

} // namespace db::pitr
