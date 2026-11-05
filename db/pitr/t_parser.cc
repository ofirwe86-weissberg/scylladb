/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/t_parser.hh"

#include <chrono>
#include <optional>

#include <seastar/core/format.hh>

namespace db::pitr {

// The length of the part every T has: YYYY-MM-DDThh:mm:ss.
static constexpr size_t seconds_length = 19;
static constexpr size_t max_fraction_digits = 6;

static std::unexpected<refusal> refuse(sstring detail) {
    return std::unexpected(refusal{
            .reason = refusal_reason::t_invalid,
            .message = seastar::format("T must be a UTC time of the form {}, for example {}; retry with T in that form.", t_form, t_example),
            .detail = std::move(detail),
    });
}

static bool is_digit(char c) {
    return c >= '0' && c <= '9';
}

// Reads exactly `n` digits at `pos`, which `text` is already known to hold. A
// sign or a space is a refusal here, where a library parser would skip or
// accept it.
static std::optional<int> digits_at(std::string_view text, size_t pos, size_t n) {
    int value = 0;
    for (size_t i = 0; i < n; ++i) {
        const char c = text[pos + i];
        if (!is_digit(c)) {
            return std::nullopt;
        }
        value = value * 10 + (c - '0');
    }
    return value;
}

std::expected<api::timestamp_type, refusal> parse_t(std::string_view text) {
    if (text.size() > max_t_length) {
        return refuse(seastar::format("T is {} characters long, longer than any time of this form", text.size()));
    }
    if (text.size() < seconds_length + 1) {
        return refuse("T is shorter than a whole second of this form");
    }
    if (text[4] != '-' || text[7] != '-' || text[10] != 'T' || text[13] != ':' || text[16] != ':') {
        return refuse("T does not carry the separators of this form, with an upper-case T between the date and the time");
    }
    if (text.back() != 'Z') {
        return refuse("T does not end in an upper-case Z; only UTC is accepted, so an offset such as +00:00 is not");
    }

    const auto year = digits_at(text, 0, 4);
    const auto month = digits_at(text, 5, 2);
    const auto day = digits_at(text, 8, 2);
    const auto hour = digits_at(text, 11, 2);
    const auto minute = digits_at(text, 14, 2);
    const auto second = digits_at(text, 17, 2);
    if (!year || !month || !day || !hour || !minute || !second) {
        return refuse("every field of T must be decimal digits, the year in four of them");
    }
    // A leap second is written :60, and the end of a day is 24:00:00 in some
    // forms; neither is a microsecond this calendar has, so neither is coerced
    // into the one next to it.
    if (*hour > 23 || *minute > 59 || *second > 59) {
        return refuse(seastar::format("{:02}:{:02}:{:02} is not a time of day", *hour, *minute, *second));
    }
    const std::chrono::year_month_day date{std::chrono::year{*year}, std::chrono::month{unsigned(*month)}, std::chrono::day{unsigned(*day)}};
    if (!date.ok()) {
        return refuse(seastar::format("{:04}-{:02}-{:02} is not a date", *year, *month, *day));
    }

    int fraction = 0;
    // Everything between the seconds and the final Z, which is the fraction
    // when there is one.
    const auto rest = text.substr(seconds_length, text.size() - seconds_length - 1);
    if (!rest.empty()) {
        if (rest[0] != '.') {
            return refuse("T has something other than a fraction of a second between its seconds and its Z");
        }
        const auto digits = rest.substr(1);
        if (digits.empty() || digits.size() > max_fraction_digits) {
            return refuse(seastar::format(
                    "the fraction of a second must be 1 to {} digits; T is to the microsecond and is never rounded to it", max_fraction_digits));
        }
        const auto value = digits_at(digits, 0, digits.size());
        if (!value) {
            return refuse("the fraction of a second must be decimal digits");
        }
        // Digits missing below the second are zero, so .5 is half a second.
        fraction = *value;
        for (size_t i = digits.size(); i < max_fraction_digits; ++i) {
            fraction *= 10;
        }
    }

    // Years 0000 to 9999 reach at most 2.6e17 microseconds either way, well
    // inside api::timestamp_type.
    const std::chrono::microseconds since_epoch{std::chrono::sys_days{date}.time_since_epoch()};
    const auto t =
            since_epoch + std::chrono::hours{*hour} + std::chrono::minutes{*minute} + std::chrono::seconds{*second} + std::chrono::microseconds{fraction};
    return t.count();
}

} // namespace db::pitr
