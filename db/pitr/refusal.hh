/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string_view>

#include <fmt/format.h>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace db::pitr {

// One code per condition a restore refuses for. The strings leave the node in
// the restore's result and are matched by tests and tooling, so an existing one
// keeps its spelling; a new check adds a code of its own rather than reusing
// one.
enum class refusal_reason {
    // Carries no interval: nothing was looked up, so nothing is known about
    // what could have been restored.
    t_invalid,
    // T is above what the backup's chains cover. Carries the interval.
    t_past_coverage,
    // A chain the bound file names has no entries at all, so nothing proves it
    // ever shipped (PM-50).
    protection_records_missing,
    // A chain's entries are there but do not hold together: one will not
    // decode, or its body does not match its key, or the links between them are
    // broken.
    protection_records_damaged,
    // The backup's bound is below the first segment the chain shipped, so the
    // commitlog between the two was never shipped (D27).
    backup_before_shipping,
    // The call named no object-storage location, or one this node does not
    // know: nothing can be read, so nothing is (P51).
    store_location_missing,
    // The target table the call names does not exist. The user creates it.
    target_missing,
    // The target holds data. A replay writes the backup and the history over
    // it, and could not tell its own writes from what was there.
    target_not_empty,
    // The target's schema differs from the backup's in more than its name, its
    // id and its compaction options (P52).
    target_schema_differs,
    // Auto-compaction is on for the target, so a compaction could purge a
    // tombstone before the writes it covers are replayed.
    target_auto_compaction_enabled,
    // A replay already ran, or started to run, into this target (P62). Carries
    // where the mark that says so lives.
    target_already_replayed,
    // A file the backup must carry is not under the prefix: the wrong prefix,
    // or a backup taken with shipping off. Carries the file.
    backup_file_missing,
    // The bound file is there but its strict reader refuses it.
    bound_file_damaged,
};

// Every enumerator, so that a test can check each one has a code of its own.
inline constexpr std::array<refusal_reason, 13> all_refusal_reasons{
        refusal_reason::t_invalid,
        refusal_reason::t_past_coverage,
        refusal_reason::protection_records_missing,
        refusal_reason::protection_records_damaged,
        refusal_reason::backup_before_shipping,
        refusal_reason::store_location_missing,
        refusal_reason::target_missing,
        refusal_reason::target_not_empty,
        refusal_reason::target_schema_differs,
        refusal_reason::target_auto_compaction_enabled,
        refusal_reason::target_already_replayed,
        refusal_reason::backup_file_missing,
        refusal_reason::bound_file_damaged,
};

std::string_view reason_code(refusal_reason) noexcept;

// What a restore could have been given instead, microseconds since the UNIX
// epoch, UTC. It is data and not only words in the message, so that the message
// can be rendered from it and the two cannot disagree.
struct restorable_interval {
    int64_t from_us = 0;
    int64_t through_us = 0;

    bool operator==(const restorable_interval&) const = default;
};

// Why a restore refused. The restore ends in a failed state carrying this, and
// nothing has changed by the time it is built.
struct refusal {
    refusal_reason reason;
    // The reason and the next action, as the user reads it.
    sstring message;
    // What in particular was at fault, when naming it helps. May be empty, and
    // never carries the next action.
    sstring detail;
    // Set by the range checks only. A message that carries an interval states
    // no example time: a reader that pulls the times out of the text cannot
    // tell an example from a bound.
    std::optional<restorable_interval> interval;

    std::string_view code() const noexcept {
        return reason_code(reason);
    }
};

} // namespace db::pitr

template <>
struct fmt::formatter<db::pitr::refusal> : fmt::formatter<string_view> {
    auto format(const db::pitr::refusal&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
