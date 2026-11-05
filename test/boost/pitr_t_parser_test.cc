/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE pitr_t_parser

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <random>
#include <regex>
#include <string>

#include <fmt/format.h>

#include "db/pitr/t_parser.hh"

using namespace db::pitr;

namespace {

// The expected values below are independent of the parser: each is the output
// of `date -u -d ... +%s` times a million, or a figure arrived at by hand.
constexpr api::timestamp_type example_us = 1793801100000000;         // 2026-11-04T14:05:00Z
constexpr api::timestamp_type day_end_us = 1793836799999999;         // 2026-11-04T23:59:59.999999Z
constexpr api::timestamp_type leap_day_us = 1709164800000000;        // 2024-02-29T00:00:00Z
constexpr api::timestamp_type year_9999_end_us = 253402300799999999; // 9999-12-31T23:59:59.999999Z
constexpr api::timestamp_type year_0_start_us = -62167219200000000;  // 0000-01-01T00:00:00Z

void require_refused(std::string_view t) {
    const auto parsed = parse_t(t);
    BOOST_REQUIRE_MESSAGE(!parsed, fmt::format("'{}' was accepted as {}", t, parsed.value_or(0)));
    BOOST_REQUIRE_EQUAL(parsed.error().code(), "t_invalid");
}

void require_accepted(std::string_view t, api::timestamp_type expected) {
    const auto parsed = parse_t(t);
    const std::string why = parsed ? std::string{} : std::string{parsed.error().detail.c_str()};
    BOOST_REQUIRE_MESSAGE(parsed, fmt::format("'{}' was refused: {}", t, why));
    BOOST_REQUIRE_EQUAL(*parsed, expected);
}

// An oracle for the fuzz test, written from the required form rather than from
// the parser: a regular expression plus the std calendar's own date check.
bool looks_valid(const std::string& t) {
    static const std::regex form{R"(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$)"};
    if (!std::regex_match(t, form)) {
        return false;
    }
    const std::chrono::year_month_day date{std::chrono::year{std::stoi(t.substr(0, 4))}, std::chrono::month{unsigned(std::stoi(t.substr(5, 2)))},
            std::chrono::day{unsigned(std::stoi(t.substr(8, 2)))}};
    return date.ok() && std::stoi(t.substr(11, 2)) <= 23 && std::stoi(t.substr(14, 2)) <= 59 && std::stoi(t.substr(17, 2)) <= 59;
}

std::string iso_of(api::timestamp_type us) {
    const auto whole = std::chrono::floor<std::chrono::days>(std::chrono::microseconds{us});
    const std::chrono::year_month_day date{std::chrono::sys_days{whole}};
    const std::chrono::hh_mm_ss time{std::chrono::microseconds{us} - whole};
    return fmt::format("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z", int(date.year()), unsigned(date.month()), unsigned(date.day()), time.hours().count(),
            time.minutes().count(), time.seconds().count(), time.subseconds().count());
}

} // namespace

BOOST_AUTO_TEST_CASE(accepts_the_example_form) {
    require_accepted("2026-11-04T14:05:00.000000Z", example_us);
}

BOOST_AUTO_TEST_CASE(accepts_a_form_with_no_fraction) {
    require_accepted("2026-11-04T14:05:00Z", example_us);
}

BOOST_AUTO_TEST_CASE(missing_digits_below_the_second_mean_zero) {
    require_accepted("2026-11-04T14:05:00.5Z", example_us + 500000);
    require_accepted("2026-11-04T14:05:00.12Z", example_us + 120000);
    require_accepted("2026-11-04T14:05:00.123Z", example_us + 123000);
    require_accepted("2026-11-04T14:05:00.1234Z", example_us + 123400);
    require_accepted("2026-11-04T14:05:00.12345Z", example_us + 123450);
    require_accepted("2026-11-04T14:05:00.123456Z", example_us + 123456);
    require_accepted("2026-11-04T14:05:00.000001Z", example_us + 1);
    require_accepted("2026-11-04T14:05:00.499999Z", example_us + 499999);
}

BOOST_AUTO_TEST_CASE(accepts_the_epoch) {
    require_accepted("1970-01-01T00:00:00Z", 0);
    require_accepted("1970-01-01T00:00:00.000000Z", 0);
}

BOOST_AUTO_TEST_CASE(accepts_a_time_before_the_epoch) {
    // A write timestamp is signed, and a T below every write simply selects
    // nothing, so there is no reason to refuse a date the calendar has.
    require_accepted("1969-12-31T23:59:59.999999Z", -1);
    require_accepted("0000-01-01T00:00:00Z", year_0_start_us);
}

BOOST_AUTO_TEST_CASE(accepts_a_leap_day) {
    require_accepted("2024-02-29T00:00:00Z", leap_day_us);
}

BOOST_AUTO_TEST_CASE(accepts_a_leap_day_of_a_century_divisible_by_400) {
    // The half of the rule a check for "a multiple of four, but not of a
    // hundred" gets wrong; 1900 and 2100 are the other half.
    require_accepted("2000-02-29T00:00:00Z", 951782400000000);
    require_accepted("2400-02-29T00:00:00Z", 13574563200000000);
    require_accepted("0000-02-29T00:00:00Z", -62162121600000000);
    require_refused("1900-02-29T00:00:00Z");
    require_refused("2100-02-29T00:00:00Z");
}

BOOST_AUTO_TEST_CASE(accepts_the_last_microsecond_of_a_day) {
    require_accepted("2026-11-04T23:59:59.999999Z", day_end_us);
}

BOOST_AUTO_TEST_CASE(accepts_year_9999) {
    require_accepted("9999-12-31T23:59:59.999999Z", year_9999_end_us);
}

BOOST_AUTO_TEST_CASE(neighbouring_times_map_one_microsecond_apart) {
    const char* const pairs[][2] = {
            {"2026-11-04T14:04:59.999999Z", "2026-11-04T14:05:00.000000Z"},
            {"2026-11-04T23:59:59.999999Z", "2026-11-05T00:00:00.000000Z"},
            {"2026-12-31T23:59:59.999999Z", "2027-01-01T00:00:00.000000Z"},
            {"2024-02-28T23:59:59.999999Z", "2024-02-29T00:00:00.000000Z"},
            {"1969-12-31T23:59:59.999999Z", "1970-01-01T00:00:00.000000Z"},
    };
    for (const auto& [below, above] : pairs) {
        const auto a = parse_t(below);
        const auto b = parse_t(above);
        BOOST_REQUIRE(a && b);
        BOOST_REQUIRE_EQUAL(*b - *a, 1);
    }
}

BOOST_AUTO_TEST_CASE(the_extremes_stay_inside_a_write_timestamp) {
    require_accepted("0000-01-01T00:00:00Z", year_0_start_us);
    require_accepted("9999-12-31T23:59:59.999999Z", year_9999_end_us);
    BOOST_REQUIRE_GT(year_0_start_us, api::min_timestamp);
    BOOST_REQUIRE_LT(year_9999_end_us, api::max_timestamp);
}

BOOST_AUTO_TEST_CASE(refuses_a_missing_or_wrong_zone) {
    require_refused("2026-11-04T14:05:00");
    require_refused("2026-11-04T14:05:00.123456");
    require_refused("2026-11-04T14:05:00+00:00");
    require_refused("2026-11-04T14:05:00+02:00");
    require_refused("2026-11-04T14:05:00-05:00");
    require_refused("2026-11-04T14:05:00UTC");
    require_refused("2026-11-04T14:05:00z");
}

BOOST_AUTO_TEST_CASE(refuses_a_local_time_form) {
    require_refused("2026-11-04 14:05:00");
    require_refused("2026-11-04 14:05:00Z");
    require_refused("2026-11-04t14:05:00Z");
    require_refused("2026/11/04T14:05:00Z");
    require_refused("2026-11-04T14.05.00Z");
}

BOOST_AUTO_TEST_CASE(refuses_more_precision_than_a_microsecond) {
    // Rounding would answer a question the user did not ask.
    require_refused("2026-11-04T14:05:00.1234567Z");
    require_refused("2026-11-04T14:05:00.0000001Z");
    require_refused("2026-11-04T14:05:00.000000000Z");
}

BOOST_AUTO_TEST_CASE(refuses_a_fraction_with_no_digits) {
    require_refused("2026-11-04T14:05:00.Z");
    require_refused("2026-11-04T14:05:00.");
    require_refused("2026-11-04T14:05:00,5Z");
}

BOOST_AUTO_TEST_CASE(refuses_an_impossible_date) {
    require_refused("2026-13-01T14:05:00Z");
    require_refused("2026-00-01T14:05:00Z");
    require_refused("2026-02-30T14:05:00Z");
    require_refused("2026-02-29T14:05:00Z"); // 2026 is not a leap year
    require_refused("1900-02-29T14:05:00Z"); // nor is 1900, a century that is not a multiple of 400
    require_refused("2026-11-00T14:05:00Z");
    require_refused("2026-11-31T14:05:00Z");
    require_refused("2026-11-32T14:05:00Z");
}

BOOST_AUTO_TEST_CASE(refuses_a_leap_second_and_the_end_of_day_hour) {
    require_refused("2026-12-31T23:59:60Z");
    require_refused("2026-11-04T14:05:60Z");
    require_refused("2026-11-04T14:60:00Z");
    require_refused("2026-11-04T24:00:00Z");
    require_refused("2026-11-04T25:00:00Z");
}

BOOST_AUTO_TEST_CASE(refuses_surrounding_or_inner_space) {
    require_refused(" 2026-11-04T14:05:00Z");
    require_refused("2026-11-04T14:05:00Z ");
    require_refused("\t2026-11-04T14:05:00Z");
    require_refused("2026-11-04T14:05:00Z\n");
    require_refused("2026-11-04T14:05: 0Z");
}

BOOST_AUTO_TEST_CASE(refuses_something_that_is_not_a_time_at_all) {
    require_refused("");
    require_refused(" ");
    require_refused("now");
    require_refused("1730000000");
    require_refused("1730000000000000");
    require_refused("2026-11-04");
    require_refused("2026-11-04Z");
    require_refused("2026-11-04T14:05Z");
    require_refused("14:05:00Z");
}

BOOST_AUTO_TEST_CASE(refuses_trailing_junk_and_signs) {
    require_refused("2026-11-04T14:05:00ZZ");
    require_refused("2026-11-04T14:05:00Z junk");
    require_refused("2026-11-04T14:05:00.000000Z;");
    require_refused("+2026-11-04T14:05:00Z");
    require_refused("-2026-11-04T14:05:00Z");
    require_refused("2026-11-04T+4:05:00Z");
    require_refused("2026-11-04T14:05:00.+00001Z");
}

BOOST_AUTO_TEST_CASE(refuses_a_year_that_is_not_four_digits) {
    require_refused("226-11-04T14:05:00Z");
    require_refused("12026-11-04T14:05:00Z");
    require_refused("26-11-04T14:05:00Z");
}

BOOST_AUTO_TEST_CASE(refuses_an_overlong_input_without_examining_it) {
    // A T well formed up to its length, padded with fraction digits past the
    // bound. The detail has to be the length's own, so that dropping the bound
    // and letting a later check refuse it does not pass.
    const std::string padded = "2026-11-04T14:05:00." + std::string(max_t_length + 1 - 21, '0') + "Z";
    BOOST_REQUIRE_EQUAL(padded.size(), max_t_length + 1);
    const auto parsed = parse_t(padded);
    BOOST_REQUIRE(!parsed);
    BOOST_REQUIRE_EQUAL(parsed.error().code(), "t_invalid");
    const std::string detail{parsed.error().detail.c_str()};
    BOOST_REQUIRE_MESSAGE(detail.find(fmt::format("{} characters", padded.size())) != std::string::npos, detail);

    require_refused(std::string(max_t_length + 1, '0'));
    require_refused("2026-11-04T14:05:00.000000Z" + std::string(1000, ' '));
}

BOOST_AUTO_TEST_CASE(the_refusal_names_the_required_form_with_an_example) {
    const auto parsed = parse_t("2026-11-04T14:05:00");
    BOOST_REQUIRE(!parsed);
    // QA reads the code and finds an example of the form in the message.
    BOOST_REQUIRE_EQUAL(parsed.error().code(), "t_invalid");
    const std::string message{parsed.error().message.c_str()};
    BOOST_REQUIRE(message.find(std::string{t_form}) != std::string::npos);
    static const std::regex an_example{R"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z)"};
    BOOST_REQUIRE(std::regex_search(message, an_example));
    BOOST_REQUIRE(!parsed.error().detail.empty());
}

BOOST_AUTO_TEST_CASE(a_random_valid_time_round_trips) {
    constexpr unsigned seed = 20261104;
    BOOST_TEST_MESSAGE(fmt::format("seed {}", seed));
    std::mt19937 engine{seed};
    std::uniform_int_distribution<api::timestamp_type> us{year_0_start_us, year_9999_end_us};
    for (int i = 0; i < 2000; ++i) {
        const auto expected = us(engine);
        const auto text = iso_of(expected);
        BOOST_TEST_CONTEXT(text) {
            require_accepted(text, expected);
        }
    }
}

BOOST_AUTO_TEST_CASE(a_random_string_is_accepted_only_if_the_form_allows_it) {
    constexpr unsigned seed = 20261105;
    BOOST_TEST_MESSAGE(fmt::format("seed {}", seed));
    std::mt19937 engine{seed};
    // The alphabet of the form, plus the characters most likely to be let
    // through by mistake.
    const std::string alphabet = "0123456789-T:.Z z+t/,";
    std::uniform_int_distribution<size_t> length{0, max_t_length + 2};
    std::uniform_int_distribution<size_t> letter{0, alphabet.size() - 1};
    const std::string example{t_example};
    for (int i = 0; i < 4000; ++i) {
        std::string text;
        if (i % 2) {
            // Half the cases are the example with one character replaced, so
            // that near misses are tried as often as noise.
            text = example;
            text[letter(engine) % text.size()] = alphabet[letter(engine)];
        } else {
            text.resize(length(engine));
            for (char& c : text) {
                c = alphabet[letter(engine)];
            }
        }
        BOOST_TEST_CONTEXT(text) {
            const auto parsed = parse_t(text);
            BOOST_REQUIRE_EQUAL(parsed.has_value(), looks_valid(text));
            if (parsed) {
                BOOST_REQUIRE_EQUAL(*parse_t(iso_of(*parsed)), *parsed);
            }
        }
    }
}
