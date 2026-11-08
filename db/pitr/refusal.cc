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
    case refusal_reason::store_location_missing:
        return "store_location_missing";
    case refusal_reason::target_missing:
        return "target_missing";
    case refusal_reason::target_not_empty:
        return "target_not_empty";
    case refusal_reason::target_schema_differs:
        return "target_schema_differs";
    case refusal_reason::target_auto_compaction_enabled:
        return "target_auto_compaction_enabled";
    case refusal_reason::target_already_replayed:
        return "target_already_replayed";
    case refusal_reason::backup_file_missing:
        return "backup_file_missing";
    case refusal_reason::bound_file_damaged:
        return "bound_file_damaged";
    case refusal_reason::backup_names_another_table:
        return "backup_names_another_table";
    case refusal_reason::replay_already_running:
        return "replay_already_running";
    case refusal_reason::target_uses_tablets:
        return "target_uses_tablets";
    case refusal_reason::backup_manifest_damaged:
        return "backup_manifest_damaged";
    case refusal_reason::backup_file_too_large:
        return "backup_file_too_large";
    case refusal_reason::shipped_object_missing:
        return "shipped_object_missing";
    case refusal_reason::shipped_object_damaged:
        return "shipped_object_damaged";
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
