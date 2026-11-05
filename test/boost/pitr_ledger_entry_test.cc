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
    e.boot_epoch_us = std::numeric_limits<int64_t>::min();
    e.segment_id = std::numeric_limits<uint64_t>::max();
    e.prev_segment_id = 0; // the first entry of a chain
    e.used_size = std::numeric_limits<uint64_t>::max();
    e.segment_crc32 = std::numeric_limits<uint32_t>::max();
    e.upload_time_us = std::numeric_limits<int64_t>::max();
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
        e.chain = chain_id(utils::UUID(int64_t(rnd()), int64_t(rnd())));
        e.sequence = rnd();
        e.boot_epoch_us = int64_t(rnd());
        e.segment_id = rnd() | 1; // 0 is not a shipped segment
        e.prev_segment_id = rnd();
        e.used_size = rnd() | 1;
        e.segment_crc32 = uint32_t(rnd());
        e.upload_time_us = int64_t(rnd());
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

BOOST_AUTO_TEST_CASE(trailing_bytes_are_refused) {
    auto good = encode(make_entry());
    for (size_t extra = 1; extra <= 3; ++extra) {
        auto padded = good + bytes(bytes::initialized_later(), extra);
        BOOST_REQUIRE_THROW(decode_segment_entry(padded), ledger_format_error);
    }
}

BOOST_AUTO_TEST_CASE(an_empty_object_is_refused) {
    BOOST_REQUIRE_THROW(decode_segment_entry(bytes_view()), ledger_format_error);
}

BOOST_AUTO_TEST_CASE(an_oversized_object_is_refused) {
    auto big = bytes(bytes::initialized_later(), max_entry_encoded_size + 1);
    BOOST_REQUIRE_THROW(decode_segment_entry(big), ledger_format_error);
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

BOOST_AUTO_TEST_CASE(an_unknown_entry_kind_is_refused) {
    auto b = encode(make_entry());
    b[10] = 2; // a future event entry
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
}

BOOST_AUTO_TEST_CASE(a_used_reserved_byte_is_refused) {
    auto b = encode(make_entry());
    b[11] = 1;
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

    auto b = encode(make_entry());
    reinterpret_cast<char*>(b.data())[82] = '/'; // a name that could leave the prefix
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
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
            0x00, // reserved
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
            0x02, // name length
            'a',
            'b', // name
            0x56,
            0x7d,
            0xd0,
            0x15, // entry crc32
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
            auto key = entry_object_key("pitr/site-a", test_host_id, shard, seq);
            auto parts = parse_entry_object_key(key);
            BOOST_REQUIRE_EQUAL(parts.prefix, "pitr/site-a");
            BOOST_REQUIRE(parts.host_id == test_host_id);
            BOOST_REQUIRE_EQUAL(parts.shard, shard);
            BOOST_REQUIRE_EQUAL(parts.sequence, seq);
        }
    }
}

BOOST_AUTO_TEST_CASE(entry_keys_sort_by_sequence) {
    std::vector<uint64_t> sequences{0, 1, 2, 9, 10, 99, 100, 1ull << 32, std::numeric_limits<uint64_t>::max() - 1, std::numeric_limits<uint64_t>::max()};
    for (size_t i = 1; i < sequences.size(); ++i) {
        auto lower = entry_object_key("p", test_host_id, 7, sequences[i - 1]);
        auto higher = entry_object_key("p", test_host_id, 7, sequences[i]);
        BOOST_REQUIRE_MESSAGE(lower < higher, fmt::format("'{}' should sort below '{}'", lower, higher));
    }
    // The shard component keeps its order too, so a listing of one prefix
    // groups shards in order.
    BOOST_REQUIRE(shard_key_prefix("p", test_host_id, 9) < shard_key_prefix("p", test_host_id, 10));
    BOOST_REQUIRE(shard_key_prefix("p", test_host_id, 10) < shard_key_prefix("p", test_host_id, std::numeric_limits<uint32_t>::max()));
}

BOOST_AUTO_TEST_CASE(keys_that_were_not_built_here_are_refused) {
    auto good = entry_object_key("pitr/site-a", test_host_id, 3, 11);
    BOOST_REQUIRE_NO_THROW(parse_entry_object_key(good));

    const char* bad[] = {
            "",
            "pitr/site-a/a1b2c3d4-0000-4000-8000-00000000000f/0000000003/head",
            "pitr/site-a/a1b2c3d4-0000-4000-8000-00000000000f/0000000003/11.entry",          // sequence not padded
            "pitr/site-a/a1b2c3d4-0000-4000-8000-00000000000f/3/00000000000000000011.entry", // shard not padded
            "pitr/site-a/not-a-uuid/0000000003/00000000000000000011.entry",
            "a1b2c3d4-0000-4000-8000-00000000000f/0000000003/00000000000000000011.entry", // no prefix
            "pitr/site-a/a1b2c3d4-0000-4000-8000-00000000000f/0000000003/00000000000000000011.entry/x",
            "pitr/site-a/a1b2c3d4-0000-4000-8000-00000000000f/0000000003/0000000000000000001x.entry",
            "pitr/site-a/a1b2c3d4-0000-4000-8000-00000000000f/0000000003/ 0000000000000000011.entry",
            "pitr/site-a/a1b2c3d4-0000-4000-8000-00000000000f/9999999999/00000000000000000011.entry", // shard past uint32
    };
    for (auto* k : bad) {
        BOOST_REQUIRE_THROW(parse_entry_object_key(k), ledger_format_error);
    }
}

BOOST_AUTO_TEST_CASE(an_unusable_prefix_is_refused) {
    BOOST_REQUIRE_THROW(entry_object_key("", test_host_id, 0, 0), ledger_format_error);
    BOOST_REQUIRE_THROW(entry_object_key("pitr/", test_host_id, 0, 0), ledger_format_error);
}
