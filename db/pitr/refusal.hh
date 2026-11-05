/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

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
    t_invalid,
};

std::string_view reason_code(refusal_reason) noexcept;

// Why a restore refused. The restore ends in a failed state carrying this, and
// nothing has changed by the time it is built.
struct refusal {
    refusal_reason reason;
    // The reason and the next action, as the user reads it.
    sstring message;
    // What in particular was at fault, when naming it helps. May be empty, and
    // never carries the next action.
    sstring detail;

    std::string_view code() const noexcept {
        return reason_code(reason);
    }
};

} // namespace db::pitr

template <>
struct fmt::formatter<db::pitr::refusal> : fmt::formatter<string_view> {
    auto format(const db::pitr::refusal&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
