/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/refusal.hh"

namespace db::pitr {

std::string_view reason_code(refusal_reason reason) noexcept {
    switch (reason) {
    case refusal_reason::t_invalid:
        return "t_invalid";
    case refusal_reason::t_past_coverage:
        return "t_past_coverage";
    case refusal_reason::protection_records_missing:
        return "protection_records_missing";
    case refusal_reason::protection_records_damaged:
        return "protection_records_damaged";
    case refusal_reason::backup_before_shipping:
        return "backup_before_shipping";
    }
    return "unknown";
}

} // namespace db::pitr

auto fmt::formatter<db::pitr::refusal>::format(const db::pitr::refusal& r, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    if (r.detail.empty()) {
        return fmt::format_to(ctx.out(), "{}: {}", r.code(), r.message);
    }
    return fmt::format_to(ctx.out(), "{}: {} ({})", r.code(), r.message, r.detail);
}
