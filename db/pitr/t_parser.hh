/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <expected>
#include <string_view>

#include "db/pitr/refusal.hh"
#include "mutation/timestamp.hh"

namespace db::pitr {

// The one form T may take, and an example of it, both shown to the user.
inline constexpr std::string_view t_form = "YYYY-MM-DDThh:mm:ss[.ffffff]Z";
inline constexpr std::string_view t_example = "2026-11-04T14:05:00.000000Z";

// A valid T is 20 to 27 characters; anything longer is refused unexamined, so
// that the work here is bounded whatever the user sends.
inline constexpr size_t max_t_length = 32;

// Parses T, the time a restore rewinds to: a UTC ISO-8601 time ending in an
// upper-case `Z`, down to the microsecond. The fraction is optional and digits
// missing below the second are zero, so `.5Z` is 500000 microseconds. Nothing
// else is accepted: no other zone or offset, no local time, no other
// separators, no surrounding space, and a seventh fractional digit is refused
// rather than rounded.
//
// Returns microseconds since the epoch, directly comparable with a write's
// timestamp. Years 0000 to 9999 are accepted, including times before the epoch,
// which come back negative; both ends are far inside api::timestamp_type.
//
// A malformed T is ordinary user input, so it comes back as a refusal rather
// than an exception.
std::expected<api::timestamp_type, refusal> parse_t(std::string_view text);

} // namespace db::pitr
