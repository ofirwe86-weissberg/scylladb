/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE pitr_ledger_entry

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <limits>
#include <random>

#include <fmt/format.h>

#include "db/pitr/ledger_entry.hh"
#include "utils/crc.hh"

using namespace db::pitr;

namespace {

// Printed on every random case so a failure can be replayed.
constexpr uint64_t random_seed = 0x5e9ed9e701ull;

const utils::UUID test_host_id{"a1b2c3d4-0000-4000-8000-00000000000f"};
const chain_id test_chain{utils::UUID("11112222-0000-4000-8000-00000000000a")};
constexpr int64_t test_boot_epoch = 1762300000000LL;

chain_key_parts test_chain_key(uint32_t shard) {
    return chain_key_parts{"pitr/site-a", test_host_id, shard, test_boot_epoch, test_boot_epoch + 1000, test_chain};
}

segment_entry make_entry() {
    segment_entry e;
    e.chain = chain_id(utils::UUID(0x0123456789abcdefLL, 0x7766554433221100LL));
    e.sequence = 42;
    e.boot_epoch_us = 1762300000000000LL;
    e.segment_id = 1762300123456789ULL;
    e.prev_segment_id = 1762300123456788ULL;
    e.used_size = 33554432;
    e.segment_crc32 = 0xdeadbeef;
    e.upload_time_us = 1762300200000001LL;
    e.covered_through_us = 1762300199000000LL;
    e.chain_start_us = 1762300001000000LL;
    e.host_id = test_host_id;
    e.shard = 5;
    e.file_name = "CommitLog-2-1762300123456789.log";
    return e;
}

// Patches a field and repairs the trailing checksum, so a test about a value
// no writer emits is not answered by the checksum instead.
bytes resealed(bytes b) {
    auto body = b.size() - sizeof(uint32_t);
    utils::crc32 crc;
    crc.process(reinterpret_cast<const uint8_t*>(b.data()), body);
    seastar::write_be<uint32_t>(reinterpret_cast<char*>(b.data()) + body, crc.get());
    return b;
}

} // namespace

BOOST_AUTO_TEST_CASE(round_trip_keeps_every_field) {
    auto e = make_entry();
    BOOST_REQUIRE(decode_segment_entry(encode(e)) == e);
}

BOOST_AUTO_TEST_CASE(round_trip_at_extreme_values) {
    segment_entry e;
    e.chain = chain_id(utils::UUID(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()));
    e.sequence = std::numeric_limits<uint64_t>::max();
    e.boot_epoch_us = std::numeric_limits<int64_t>::max();
    e.segment_id = std::numeric_limits<uint64_t>::max();
    e.prev_segment_id = 0; // the first entry of a chain
    e.used_size = std::numeric_limits<uint64_t>::max();
    e.segment_crc32 = std::numeric_limits<uint32_t>::max();
    e.upload_time_us = std::numeric_limits<int64_t>::max();
    e.covered_through_us = std::numeric_limits<int64_t>::max(); // coverage right up to the upload
    e.chain_start_us = std::numeric_limits<int64_t>::max();
    e.host_id = test_host_id;
    e.shard = 7;
    e.file_name = sstring(max_file_name_size, 'x');
    auto encoded = encode(e);
    BOOST_REQUIRE_EQUAL(encoded.size(), max_entry_encoded_size);
    BOOST_REQUIRE(decode_segment_entry(encoded) == e);

    // The shortest entry a writer can emit.
    e.file_name = "a";
    encoded = encode(e);
    BOOST_REQUIRE_EQUAL(encoded.size(), min_entry_encoded_size);
    BOOST_REQUIRE(decode_segment_entry(encoded) == e);
}

BOOST_AUTO_TEST_CASE(round_trip_of_random_entries) {
    BOOST_TEST_MESSAGE(fmt::format("seed {:#x}", random_seed));
    std::mt19937_64 rnd(random_seed);
    for (int i = 0; i < 200; ++i) {
        segment_entry e;
        // The low bit is set so neither id can come out null, which no writer
        // emits and decode refuses.
        e.chain = chain_id(utils::UUID(int64_t(rnd() | 1), int64_t(rnd())));
        e.host_id = utils::UUID(int64_t(rnd() | 1), int64_t(rnd()));
        e.shard = uint32_t(rnd());
        e.sequence = rnd();
        e.boot_epoch_us = int64_t(rnd() >> 1);
        e.segment_id = rnd() | 1; // 0 is not a shipped segment
        e.prev_segment_id = rnd();
        e.used_size = rnd() | 1;
        e.segment_crc32 = uint32_t(rnd());
        e.covered_through_us = int64_t(rnd() >> 2);
        e.upload_time_us = e.covered_through_us + int64_t(rnd() >> 2);
        e.chain_start_us = int64_t(rnd() >> 2);
        e.file_name = fmt::format("CommitLog-2-{}.log", rnd());
        BOOST_REQUIRE_MESSAGE(decode_segment_entry(encode(e)) == e, fmt::format("iteration {}, seed {:#x}", i, random_seed));
    }
}

BOOST_AUTO_TEST_CASE(a_single_changed_byte_anywhere_is_detected) {
    auto e = make_entry();
    auto good = encode(e);
    for (size_t i = 0; i < good.size(); ++i) {
        for (uint8_t mask : {uint8_t(0x01), uint8_t(0x80)}) {
            auto damaged = good;
            damaged[i] = bytes::value_type(damaged[i] ^ mask);
            BOOST_REQUIRE_MESSAGE(damaged != good, fmt::format("byte {} unchanged", i));
            BOOST_REQUIRE_THROW(decode_segment_entry(damaged), ledger_format_error);
        }
    }
}

BOOST_AUTO_TEST_CASE(truncation_at_every_length_is_refused) {
    auto good = encode(make_entry());
    for (size_t len = 0; len < good.size(); ++len) {
        BOOST_REQUIRE_THROW(decode_segment_entry(bytes_view(good.data(), len)), ledger_format_error);
    }
}

// Resealed, so the entry's own exact-length check refuses it rather than the
// checksum, which an attacker or a careless tool would have recomputed anyway.
BOOST_AUTO_TEST_CASE(trailing_bytes_are_refused) {
    auto good = encode(make_entry());
    for (size_t extra = 1; extra <= 3; ++extra) {
        auto padded = good + bytes(bytes::initialized_later(), extra);
        std::fill(padded.begin() + good.size(), padded.end(), bytes::value_type(0));
        BOOST_REQUIRE_THROW(decode_segment_entry(resealed(padded)), ledger_format_error);
    }
}

BOOST_AUTO_TEST_CASE(an_empty_object_is_refused) {
    BOOST_REQUIRE_THROW(decode_segment_entry(bytes_view()), ledger_format_error);
}

BOOST_AUTO_TEST_CASE(a_newer_version_is_refused_as_an_unknown_version) {
    auto b = encode(make_entry());
    seastar::write_be<uint16_t>(reinterpret_cast<char*>(b.data()) + 8, ledger_format_version_1 + 1);
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_unknown_version);
    seastar::write_be<uint16_t>(reinterpret_cast<char*>(b.data()) + 8, std::numeric_limits<uint16_t>::max());
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_unknown_version);
}

BOOST_AUTO_TEST_CASE(version_zero_is_refused_but_not_as_a_newer_version) {
    auto b = encode(make_entry());
    seastar::write_be<uint16_t>(reinterpret_cast<char*>(b.data()) + 8, 0);
    b = resealed(b);
    BOOST_REQUIRE_THROW(decode_segment_entry(b), ledger_format_error);
    try {
        decode_segment_entry(b);
        BOOST_FAIL("version 0 accepted");
    } catch (const ledger_unknown_version&) {
        BOOST_FAIL("version 0 reported as a newer format");
    } catch (const ledger_format_error&) {
    }
}

BOOST_AUTO_TEST_CASE(a_foreign_object_is_refused_on_the_magic) {
    auto b = encode(make_entry());
    b[0] = 'X';
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
}

BOOST_AUTO_TEST_CASE(an_unknown_entry_kind_is_refused_as_an_unknown_version) {
    for (uint8_t kind : {uint8_t(object_kind_head + 1), uint8_t(0xff)}) {
        auto b = encode(make_entry());
        b[10] = bytes::value_type(kind); // a future event entry, which this reader must not skip
        BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_unknown_version);
    }
}

// Kind 0 is never assigned, so an intact 0 is damage: telling the user to
// upgrade would send them after a version that will never exist.
BOOST_AUTO_TEST_CASE(kind_zero_is_refused_but_not_as_an_unknown_version) {
    auto b = encode(make_entry());
    b[10] = 0;
    b = resealed(b);
    BOOST_REQUIRE_THROW(decode_segment_entry(b), ledger_format_error);
    try {
        decode_segment_entry(b);
        BOOST_FAIL("kind 0 accepted");
    } catch (const ledger_unknown_version&) {
        BOOST_FAIL("kind 0 reported as an unknown version");
    } catch (const ledger_format_error&) {
    }
}

// `head` is a kind this reader knows, so asking for a segment entry and getting
// one is a format error, not something to report as needing an upgrade.
BOOST_AUTO_TEST_CASE(a_head_object_is_not_read_as_a_segment_entry) {
    auto b = encode(make_entry());
    b[10] = bytes::value_type(object_kind_head);
    b = resealed(b);
    BOOST_REQUIRE_THROW(decode_segment_entry(b), ledger_format_error);
    try {
        decode_segment_entry(b);
        BOOST_FAIL("a head object was read as a segment entry");
    } catch (const ledger_unknown_version&) {
        BOOST_FAIL("a known kind was reported as an unknown version");
    } catch (const ledger_format_error&) {
    }
}

// The decode order is part of the format: the checksum comes before the
// version, so a reader never tells the user to upgrade over damage.
BOOST_AUTO_TEST_CASE(a_flipped_version_bit_is_reported_as_damage_not_as_an_unknown_version) {
    auto good = encode(make_entry());
    for (uint8_t mask : {uint8_t(0x01), uint8_t(0x02), uint8_t(0x80)}) {
        auto damaged = good;
        damaged[9] = bytes::value_type(damaged[9] ^ mask); // the version's low byte, checksum left alone
        try {
            decode_segment_entry(damaged);
            BOOST_FAIL("a flipped version bit was accepted");
        } catch (const ledger_unknown_version&) {
            BOOST_FAIL(fmt::format("mask {:#04x}: damage reported as an unknown version", mask));
        } catch (const ledger_format_error&) {
        }
    }
}

// The other side of the same order: an intact object that a newer writer made
// bigger, or gave a newer version or kind, must say so rather than complain
// about its size, which is checked only after the version and the kind.
BOOST_AUTO_TEST_CASE(an_intact_larger_newer_object_is_reported_as_an_unknown_version) {
    auto grown = [](bytes b, size_t extra, size_t off, uint16_t value) {
        b = b.substr(0, b.size() - sizeof(uint32_t)) + bytes(bytes::initialized_later(), extra) + bytes(bytes::initialized_later(), sizeof(uint32_t));
        std::fill(b.begin() + b.size() - extra - sizeof(uint32_t), b.begin() + b.size() - sizeof(uint32_t), bytes::value_type(0));
        seastar::write_be<uint16_t>(reinterpret_cast<char*>(b.data()) + off, value);
        return resealed(b);
    };
    // Larger than any version 1 segment entry, but a legal ledger object.
    auto newer_version = grown(encode(make_entry()), max_entry_encoded_size, 8, ledger_format_version_1 + 1);
    BOOST_REQUIRE_GT(newer_version.size(), max_entry_encoded_size);
    BOOST_REQUIRE_THROW(decode_segment_entry(newer_version), ledger_unknown_version);

    auto newer_kind = grown(encode(make_entry()), max_entry_encoded_size, 10, uint16_t((object_kind_head + 1) << 8)); // an unassigned kind, flags 0
    BOOST_REQUIRE_THROW(decode_segment_entry(newer_kind), ledger_unknown_version);
}

// The cap is never raised, so even an intact object of a newer version is
// refused on its size alone: the version is read only after the cap.
BOOST_AUTO_TEST_CASE(an_object_past_the_general_maximum_is_refused) {
    auto b = encode(make_entry());
    b = b.substr(0, b.size() - sizeof(uint32_t)) + bytes(bytes::initialized_later(), max_ledger_object_size) +
        bytes(bytes::initialized_later(), sizeof(uint32_t));
    std::fill(b.begin() + min_entry_encoded_size, b.end(), bytes::value_type(0));
    seastar::write_be<uint16_t>(reinterpret_cast<char*>(b.data()) + 8, ledger_format_version_1 + 1);
    b = resealed(b);
    BOOST_REQUIRE_GT(b.size(), max_ledger_object_size);
    try {
        decode_segment_entry(b);
        BOOST_FAIL("an object past the general maximum was accepted");
    } catch (const ledger_unknown_version&) {
        BOOST_FAIL("an object past the general maximum was reported as an unknown version");
    } catch (const ledger_format_error&) {
    }
}

BOOST_AUTO_TEST_CASE(an_unknown_must_understand_flag_bit_is_refused) {
    for (int bit = 0; bit < 4; ++bit) {
        auto b = encode(make_entry());
        b[11] = bytes::value_type(1 << bit);
        BOOST_REQUIRE_MESSAGE(uint8_t(1 << bit) & must_understand_flags_mask, fmt::format("bit {} is not must-understand", bit));
        BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
    }
}

// The other half: a hint from a newer writer must not cost this reader the
// entry, so an advisory bit it does not know is ignored.
BOOST_AUTO_TEST_CASE(an_unknown_advisory_flag_bit_is_ignored) {
    auto e = make_entry();
    for (int bit = 4; bit < 8; ++bit) {
        auto b = encode(e);
        b[11] = bytes::value_type(1 << bit);
        BOOST_REQUIRE_MESSAGE(uint8_t(1 << bit) & advisory_flags_mask, fmt::format("bit {} is not advisory", bit));
        BOOST_REQUIRE(decode_segment_entry(resealed(b)) == e);
    }
    // Both halves at once is still a refusal.
    auto b = encode(e);
    b[11] = bytes::value_type(0x11);
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
}

BOOST_AUTO_TEST_CASE(values_no_writer_emits_are_refused_on_decode) {
    auto zero_at = [](size_t off) {
        auto b = encode(make_entry());
        seastar::write_be<uint64_t>(reinterpret_cast<char*>(b.data()) + off, 0);
        return resealed(b);
    };
    BOOST_REQUIRE_THROW(decode_segment_entry(zero_at(44)), ledger_format_error); // segment id
    BOOST_REQUIRE_THROW(decode_segment_entry(zero_at(60)), ledger_format_error); // used size

    auto negative_epoch = encode(make_entry());
    seastar::write_be<int64_t>(reinterpret_cast<char*>(negative_epoch.data()) + 36, -1);
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(negative_epoch)), ledger_format_error);

    // A name the entry could carry but a fetch could not use.
    for (char c : {'/', '\0', '\x1f', '\x7f', char(0x80), char(0xff)}) {
        auto b = encode(make_entry());
        reinterpret_cast<char*>(b.data())[118] = c;
        BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
    }
    // '.' and '..' would name the chain's directory or its parent.
    for (std::string_view dots : {".", ".."}) {
        auto e = make_entry();
        e.file_name = sstring(dots.size(), 'x');
        auto b = encode(e);
        std::copy(dots.begin(), dots.end(), reinterpret_cast<char*>(b.data()) + 118);
        BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
    }
}

// Both times come from the wall clock, which can step backwards, so a fast seal
// and upload can record coverage above the upload's time. Refusing that would
// stop shipping on valid data, so the entry carries it.
BOOST_AUTO_TEST_CASE(coverage_past_the_upload_time_is_accepted) {
    auto e = make_entry();
    e.covered_through_us = e.upload_time_us + 1;
    BOOST_REQUIRE(decode_segment_entry(encode(e)) == e);

    // A step of the size this host shows, 153 ms, on an entry sealed and
    // uploaded inside it.
    e = make_entry();
    e.covered_through_us = e.upload_time_us + 153'000;
    BOOST_REQUIRE(decode_segment_entry(encode(e)) == e);

    e = make_entry();
    e.covered_through_us = e.upload_time_us;
    BOOST_REQUIRE(decode_segment_entry(encode(e)) == e);
}

BOOST_AUTO_TEST_CASE(a_negative_upload_or_coverage_time_is_refused) {
    auto e = make_entry();
    e.upload_time_us = -1;
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
    e = make_entry();
    e.covered_through_us = -1;
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);

    // And on the way in, with the checksum repaired so the field answers. The
    // other time is left at 0, so only the `< 0` check can refuse each case.
    for (size_t off : {size_t(72), size_t(80)}) {
        auto b = encode(make_entry());
        seastar::write_be<int64_t>(reinterpret_cast<char*>(b.data()) + 72, 0);
        seastar::write_be<int64_t>(reinterpret_cast<char*>(b.data()) + 80, 0);
        seastar::write_be<int64_t>(reinterpret_cast<char*>(b.data()) + off, -1);
        BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
    }
}

// The chain start is in the entry's key too, and no checksum covers a key, so
// the body carries it and the chain reader compares the two.
BOOST_AUTO_TEST_CASE(the_chain_start_round_trips_at_its_extremes) {
    for (int64_t start : {int64_t(0), int64_t(1), std::numeric_limits<int64_t>::max()}) {
        auto e = make_entry();
        e.chain_start_us = start;
        auto decoded = decode_segment_entry(encode(e));
        BOOST_REQUIRE_EQUAL(decoded.chain_start_us, start);
        BOOST_REQUIRE(decoded == e);
    }

    // Nothing ties it to the other wall-clock times, which can step, so a start
    // above the boot epoch or the upload is carried and not refused.
    auto e = make_entry();
    e.chain_start_us = e.upload_time_us + 1;
    BOOST_REQUIRE(decode_segment_entry(encode(e)) == e);
    e.chain_start_us = e.boot_epoch_us - 1;
    BOOST_REQUIRE(decode_segment_entry(encode(e)) == e);
}

BOOST_AUTO_TEST_CASE(a_negative_chain_start_is_refused) {
    auto e = make_entry();
    e.chain_start_us = -1;
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
    e.chain_start_us = std::numeric_limits<int64_t>::min();
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);

    // And on the way in, with the checksum repaired so the field answers and
    // not the checksum. Every other time is left at 0, so only the chain
    // start's own check can refuse these.
    for (int64_t start : {int64_t(-1), std::numeric_limits<int64_t>::min()}) {
        auto b = encode(make_entry());
        auto* p = reinterpret_cast<char*>(b.data());
        seastar::write_be<int64_t>(p + 36, 0); // boot epoch
        seastar::write_be<int64_t>(p + 72, 0); // upload time
        seastar::write_be<int64_t>(p + 80, 0); // covered through
        seastar::write_be<int64_t>(p + 88, start);
        BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
    }
}

// The host and the shard are in the key too, and nothing checksums a key, so a
// chain moved whole under another host or shard must not be believed: the body
// carries both and the chain reader compares them.
BOOST_AUTO_TEST_CASE(the_host_id_and_shard_round_trip_at_their_extremes) {
    BOOST_TEST_MESSAGE(fmt::format("seed {:#x}", random_seed));
    std::mt19937_64 rnd(random_seed);
    auto host = utils::UUID(int64_t(rnd() | 1), int64_t(rnd()));
    for (uint32_t shard : {0u, 1u, std::numeric_limits<uint32_t>::max()}) {
        auto e = make_entry();
        e.host_id = host;
        e.shard = shard;
        auto decoded = decode_segment_entry(encode(e));
        BOOST_REQUIRE_EQUAL(decoded.host_id, host);
        BOOST_REQUIRE_EQUAL(decoded.shard, shard);
        BOOST_REQUIRE(decoded == e);
    }
}

// A null id is what a zeroed or unwritten object holds, the same reasoning as
// segment id 0, so it is refused on both sides.
BOOST_AUTO_TEST_CASE(a_null_chain_id_is_refused) {
    auto e = make_entry();
    e.chain = chain_id(utils::UUID());
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);

    auto b = encode(make_entry());
    auto* p = reinterpret_cast<char*>(b.data());
    seastar::write_be<int64_t>(p + 12, 0); // chain msb
    seastar::write_be<int64_t>(p + 20, 0); // chain lsb
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);

    // Only both halves zero is null: either half alone is a valid id.
    for (auto off : {12, 20}) {
        auto half = encode(make_entry());
        seastar::write_be<int64_t>(reinterpret_cast<char*>(half.data()) + off, 0);
        BOOST_REQUIRE_NO_THROW(decode_segment_entry(resealed(half)));
    }
}

BOOST_AUTO_TEST_CASE(a_null_host_id_is_refused) {
    auto e = make_entry();
    e.host_id = utils::UUID();
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);

    auto b = encode(make_entry());
    auto* p = reinterpret_cast<char*>(b.data());
    seastar::write_be<int64_t>(p + 96, 0);  // host id msb
    seastar::write_be<int64_t>(p + 104, 0); // host id lsb
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);

    // Only both halves zero is null: either half alone is a valid id.
    for (auto off : {96, 104}) {
        auto half = encode(make_entry());
        seastar::write_be<int64_t>(reinterpret_cast<char*>(half.data()) + off, 0);
        BOOST_REQUIRE_NO_THROW(decode_segment_entry(resealed(half)));
    }
}

// The declared name length fixes the whole size, so one byte either side must
// be refused on the size and not read as a shorter name or trailing padding.
BOOST_AUTO_TEST_CASE(one_byte_short_or_long_is_refused_on_the_exact_size) {
    auto good = encode(make_entry());
    BOOST_REQUIRE_EQUAL(good.size(), 122 + make_entry().file_name.size());

    // Resealed, so the exact-length check answers rather than the checksum.
    auto refused_on_the_length = [](bytes b) {
        try {
            decode_segment_entry(resealed(std::move(b)));
        } catch (const ledger_format_error& e) {
            return std::string_view(e.what()).find("bytes, but holds") != std::string_view::npos;
        }
        return false;
    };
    BOOST_REQUIRE(refused_on_the_length(good.substr(0, good.size() - 1)));
    auto padded = good + bytes(bytes::initialized_later(), 1);
    padded.back() = bytes::value_type(0);
    BOOST_REQUIRE(refused_on_the_length(padded));
}

BOOST_AUTO_TEST_CASE(encode_refuses_an_entry_it_could_not_prove) {
    auto e = make_entry();
    e.file_name = "";
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
    e.file_name = sstring(max_file_name_size + 1, 'x');
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
    e.file_name = "dir/CommitLog-2-1.log";
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
    e.file_name = sstring("Commit\nLog");
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
    e.file_name = ".";
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
    e.file_name = "..";
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
    e.file_name = sstring("Commit\x7fLog");
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
    e.file_name = sstring("Commit\xc3\xa9Log");
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);

    e = make_entry();
    e.boot_epoch_us = -1;
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);

    e = make_entry();
    e.segment_id = 0;
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
    e = make_entry();
    e.used_size = 0;
    BOOST_REQUIRE_THROW(encode(e), ledger_format_error);
}

// Pins version 1 byte for byte. An accidental layout, order or checksum change
// fails here, which is the point: restore reads objects this node will never
// rewrite.
BOOST_AUTO_TEST_CASE(golden_vector_pins_version_1) {
    segment_entry e;
    e.chain = chain_id(utils::UUID(0x0102030405060708LL, 0x090a0b0c0d0e0f10LL));
    e.sequence = 1;
    e.boot_epoch_us = 2;
    e.segment_id = 3;
    e.prev_segment_id = 0;
    e.used_size = 4;
    e.segment_crc32 = 0x05060708;
    e.upload_time_us = 9;
    e.covered_through_us = 7;
    e.chain_start_us = 11;
    e.host_id = utils::UUID(0x1112131415161718LL, 0x191a1b1c1d1e1f20LL);
    e.shard = 19;
    e.file_name = "ab";

    static const uint8_t expected[] = {
            'P',
            'I',
            'T',
            'R',
            'L',
            'D',
            'G',
            'R', // magic
            0x00,
            0x01, // version 1
            0x01, // kind: segment
            0x00, // flags
            0x01,
            0x02,
            0x03,
            0x04,
            0x05,
            0x06,
            0x07,
            0x08, // chain msb
            0x09,
            0x0a,
            0x0b,
            0x0c,
            0x0d,
            0x0e,
            0x0f,
            0x10, // chain lsb
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x01, // sequence
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x02, // boot epoch
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x03, // segment id
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00, // previous segment id
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x04, // used size
            0x05,
            0x06,
            0x07,
            0x08, // segment crc32
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x09, // upload time
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x07, // covered through
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x0b, // chain start
            0x11,
            0x12,
            0x13,
            0x14,
            0x15,
            0x16,
            0x17,
            0x18, // host id msb
            0x19,
            0x1a,
            0x1b,
            0x1c,
            0x1d,
            0x1e,
            0x1f,
            0x20, // host id lsb
            0x00,
            0x00,
            0x00,
            0x13, // shard
            0x00,
            0x02, // name length
            'a',
            'b', // name
            0x9c,
            0xa6,
            0xb4,
            0xbc, // entry crc32
    };

    auto encoded = encode(e);
    BOOST_REQUIRE_EQUAL(encoded.size(), sizeof(expected));
    BOOST_REQUIRE_EQUAL_COLLECTIONS(reinterpret_cast<const uint8_t*>(encoded.data()), reinterpret_cast<const uint8_t*>(encoded.data()) + encoded.size(),
            std::begin(expected), std::end(expected));
    BOOST_REQUIRE(decode_segment_entry(bytes_view(reinterpret_cast<const bytes::value_type*>(expected), sizeof(expected))) == e);
}

BOOST_AUTO_TEST_CASE(entry_key_round_trips) {
    for (uint32_t shard : {0u, 1u, 63u, std::numeric_limits<uint32_t>::max()}) {
        for (uint64_t seq : {uint64_t(0), uint64_t(1), uint64_t(1) << 40, std::numeric_limits<uint64_t>::max()}) {
            auto chain = test_chain_key(shard);
            auto parts = parse_entry_object_key(entry_object_key(chain, seq));
            BOOST_REQUIRE(parts.chain == chain);
            BOOST_REQUIRE_EQUAL(parts.sequence, seq);
        }
    }
    // Both times at their extremes still round-trip.
    for (int64_t t : {int64_t(0), int64_t(1), std::numeric_limits<int64_t>::max()}) {
        auto chain = test_chain_key(3);
        chain.boot_epoch_us = t;
        chain.chain_start_us = t;
        BOOST_REQUIRE(parse_entry_object_key(entry_object_key(chain, 5)).chain == chain);
    }
}

BOOST_AUTO_TEST_CASE(chain_prefix_and_head_key_round_trip) {
    auto chain = test_chain_key(3);
    auto prefix = chain_key_prefix(chain);
    BOOST_REQUIRE(parse_chain_key_prefix(prefix) == chain);

    auto head = head_object_key(chain);
    BOOST_REQUIRE_EQUAL(head, prefix + "head");
    BOOST_REQUIRE(parse_head_object_key(head) == chain);

    // Every key of one chain sits under the chain's prefix, and the head and the
    // entries are told apart by the suffix a listing filters on.
    BOOST_REQUIRE(entry_object_key(chain, 7).starts_with(prefix));
    BOOST_REQUIRE(segment_object_key(chain, "CommitLog-2-9.log").starts_with(prefix));
    BOOST_REQUIRE_THROW(parse_entry_object_key(head), ledger_format_error);
}

// A restart keeps sequence numbers but gets a new chain, and
// its entries must not land on the old chain's.
BOOST_AUTO_TEST_CASE(a_new_chain_never_reuses_an_older_chains_keys) {
    auto older = test_chain_key(3);
    auto new_chain_same_boot = older;
    new_chain_same_boot.chain = chain_id(utils::UUID(1, 2));
    auto later_boot = new_chain_same_boot;
    later_boot.boot_epoch_us = older.boot_epoch_us + 1000;
    BOOST_REQUIRE(entry_object_key(older, 0) != entry_object_key(new_chain_same_boot, 0));
    BOOST_REQUIRE(entry_object_key(older, 0) != entry_object_key(later_boot, 0));
    BOOST_REQUIRE(entry_object_key(older, 0) < entry_object_key(later_boot, 0));
}

// An outage ends a chain and the next one starts in the same boot, so the start
// time, not the chain's uuid, has to put them in order.
BOOST_AUTO_TEST_CASE(chains_of_one_boot_sort_by_their_start_time) {
    auto first = test_chain_key(3);
    first.chain_start_us = 9;
    // A uuid that sorts below the first chain's, so only the start time can
    // give the right order.
    first.chain = chain_id(utils::UUID("ffffffff-0000-4000-8000-00000000000f"));
    auto second = first;
    second.chain_start_us = 10;
    second.chain = chain_id(utils::UUID("00000000-0000-4000-8000-000000000001"));
    BOOST_REQUIRE_LT(chain_key_prefix(first), chain_key_prefix(second));
    BOOST_REQUIRE_LT(entry_object_key(first, 0), entry_object_key(second, 0));
    // And the boot epoch still outranks the start time.
    auto next_boot = first;
    next_boot.boot_epoch_us = first.boot_epoch_us + 1;
    next_boot.chain_start_us = 0;
    BOOST_REQUIRE_LT(chain_key_prefix(second), chain_key_prefix(next_boot));
}

// A file name reused after a clock step must not overwrite another chain's
// segment, and segments must stay out of the entries' listing.
BOOST_AUTO_TEST_CASE(segment_keys_are_scoped_to_their_chain) {
    auto chain = test_chain_key(3);
    auto other = chain;
    other.chain_start_us = chain.chain_start_us + 1;
    auto a = segment_object_key(chain, "CommitLog-2-7.log");
    BOOST_REQUIRE(a != segment_object_key(other, "CommitLog-2-7.log"));
    BOOST_REQUIRE(a.ends_with("/segments/CommitLog-2-7.log"));
    BOOST_REQUIRE(!a.ends_with(entry_key_suffix));
    BOOST_REQUIRE_THROW(parse_entry_object_key(a), ledger_format_error);
    // The same name rule as the entry's, so a key can never leave the chain.
    for (sstring name : {sstring(""), sstring("."), sstring(".."), sstring("a/b"), sstring("a\x7f"), sstring(max_file_name_size + 1, 'x')}) {
        BOOST_REQUIRE_THROW(segment_object_key(chain, name), ledger_format_error);
    }
}

BOOST_AUTO_TEST_CASE(entry_keys_sort_by_sequence) {
    std::vector<uint64_t> sequences{0, 1, 2, 9, 10, 99, 100, 1ull << 32, std::numeric_limits<uint64_t>::max() - 1, std::numeric_limits<uint64_t>::max()};
    auto chain = test_chain_key(7);
    for (size_t i = 1; i < sequences.size(); ++i) {
        auto lower = entry_object_key(chain, sequences[i - 1]);
        auto higher = entry_object_key(chain, sequences[i]);
        BOOST_REQUIRE_MESSAGE(lower < higher, fmt::format("'{}' should sort below '{}'", lower, higher));
    }
    // The shard component keeps its order too, so a listing of one prefix
    // groups shards in order.
    BOOST_REQUIRE(shard_key_prefix("p", test_host_id, 9) < shard_key_prefix("p", test_host_id, 10));
    BOOST_REQUIRE(shard_key_prefix("p", test_host_id, 10) < shard_key_prefix("p", test_host_id, std::numeric_limits<uint32_t>::max()));
}

BOOST_AUTO_TEST_CASE(keys_that_were_not_built_here_are_refused) {
    BOOST_REQUIRE_NO_THROW(parse_entry_object_key(entry_object_key(test_chain_key(3), 11)));

    const char* chain_dir = "00000001762300000000-00000001762300001000-11112222-0000-4000-8000-00000000000a";
    auto key = [&](std::string_view prefix, std::string_view host, std::string_view shard, std::string_view chain, std::string_view name) {
        return fmt::format("{}/{}/{}/{}/{}", prefix, host, shard, chain, name);
    };
    const sstring host = "a1b2c3d4-0000-4000-8000-00000000000f";
    const sstring upper_host = "A1B2C3D4-0000-4000-8000-00000000000F";
    const sstring seq = "00000000000000000011.entry";

    const sstring bad[] = {
            "",
            key("pitr", host, "0000000003", chain_dir, "head"),             // the head, not an entry
            key("pitr", host, "0000000003", chain_dir, "11.entry"),         // sequence not padded
            key("pitr", host, "3", chain_dir, seq),                         // shard not padded
            key("pitr", "not-a-uuid", "0000000003", chain_dir, seq),        // host id
            key("pitr", upper_host, "0000000003", chain_dir, seq),          // host id not canonical
            fmt::format("{}/{}/{}/{}", host, "0000000003", chain_dir, seq), // no prefix
            key("/pitr", host, "0000000003", chain_dir, seq),               // leading '/'
            key("pitr/", host, "0000000003", chain_dir, seq),               // trailing '/' in the prefix
            key("pitr//site-a", host, "0000000003", chain_dir, seq),        // empty component
            key("pitr/./site-a", host, "0000000003", chain_dir, seq),       // '.' component
            key("pitr/../site-a", host, "0000000003", chain_dir, seq),
            key(".", host, "0000000003", chain_dir, seq),
            key("..", host, "0000000003", chain_dir, seq),
            key("pitr", host, "0000000003", chain_dir, "x/" + seq), // a component below the chain
            key("pitr", host, "0000000003", chain_dir, "0000000000000000001x.entry"),
            key("pitr", host, "0000000003", chain_dir, " 0000000000000000011.entry"),
            key("pitr", host, "9999999999", chain_dir, seq),                                                                 // shard past uint32
            key("pitr", host, "0000000003", "11112222-0000-4000-8000-00000000000a", seq),                                    // no times
            key("pitr", host, "0000000003", "00000001762300000000-11112222-0000-4000-8000-00000000000a", seq),               // no start time
            key("pitr", host, "0000000003", "1762300000000-00000001762300001000-11112222-0000-4000-8000-00000000000a", seq), // epoch not padded
            key("pitr", host, "0000000003", "00000001762300000000-1762300001000-11112222-0000-4000-8000-00000000000a", seq), // start not padded
            key("pitr", host, "0000000003", "00000001762300000000-00000001762300001000-not-a-uuid", seq),
            key("pitr", host, "0000000003", "00000001762300000000-00000001762300001000-11112222-0000-4000-8000-00000000000A", seq), // not canonical
            // Past INT64_MAX but a legal 20-digit number, so the range check
            // answers rather than the digit parse.
            key("pitr", host, "0000000003", "09223372036854775808-00000001762300001000-11112222-0000-4000-8000-00000000000a", seq),
            key("pitr", host, "0000000003", "00000001762300000000-09223372036854775808-11112222-0000-4000-8000-00000000000a", seq),
            key("pitr", host, "0000000003", "99999999999999999999-00000001762300001000-11112222-0000-4000-8000-00000000000a", seq),
    };
    for (auto& k : bad) {
        BOOST_REQUIRE_THROW(parse_entry_object_key(k), ledger_format_error);
    }
    // The head and the chain prefix hold to the same rules.
    BOOST_REQUIRE_THROW(parse_head_object_key(key("pitr/", host, "0000000003", chain_dir, "head")), ledger_format_error);
    BOOST_REQUIRE_THROW(parse_head_object_key(entry_object_key(test_chain_key(3), 11)), ledger_format_error);
    BOOST_REQUIRE_NO_THROW(parse_chain_key_prefix(key("pitr", host, "0000000003", chain_dir, "")));
    BOOST_REQUIRE_THROW(parse_chain_key_prefix(key("pitr", host, "0000000003", chain_dir, "segments/")), ledger_format_error);
    BOOST_REQUIRE_THROW(parse_chain_key_prefix(fmt::format("pitr/{}/0000000003/{}", host, chain_dir)), ledger_format_error); // no trailing '/'
}

BOOST_AUTO_TEST_CASE(an_unusable_prefix_is_refused_by_the_builders) {
    for (sstring prefix : {sstring(""), sstring("/"), sstring("pitr/"), sstring("/pitr"), sstring("pitr//site-a"), sstring("."), sstring(".."),
                 sstring("pitr/./site-a"), sstring("pitr/../site-a")}) {
        auto chain = test_chain_key(0);
        chain.prefix = prefix;
        BOOST_REQUIRE_THROW(entry_object_key(chain, 0), ledger_format_error);
        BOOST_REQUIRE_THROW(head_object_key(chain), ledger_format_error);
        BOOST_REQUIRE_THROW(segment_object_key(chain, "CommitLog-2-1.log"), ledger_format_error);
        BOOST_REQUIRE_THROW(shard_key_prefix(prefix, test_host_id, 0), ledger_format_error);
    }
}

BOOST_AUTO_TEST_CASE(a_time_with_no_chain_directory_is_refused) {
    auto chain = test_chain_key(0);
    chain.boot_epoch_us = -1;
    BOOST_REQUIRE_THROW(chain_key_prefix(chain), ledger_format_error);
    chain = test_chain_key(0);
    chain.chain_start_us = -1;
    BOOST_REQUIRE_THROW(chain_key_prefix(chain), ledger_format_error);
    chain = test_chain_key(0);
    chain.boot_epoch_us = 0;
    chain.chain_start_us = 0;
    BOOST_REQUIRE_NO_THROW(chain_key_prefix(chain));
}
