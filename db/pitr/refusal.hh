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
};

// Every enumerator, so that a test can check each one has a code of its own.
inline constexpr std::array<refusal_reason, 5> all_refusal_reasons{
        refusal_reason::t_invalid,
        refusal_reason::t_past_coverage,
        refusal_reason::protection_records_missing,
        refusal_reason::protection_records_damaged,
        refusal_reason::backup_before_shipping,
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
