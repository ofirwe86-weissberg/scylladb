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
    e.boot_epoch_us = std::numeric_limits<int64_t>::max();
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
        e.boot_epoch_us = int64_t(rnd() >> 1);
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

BOOST_AUTO_TEST_CASE(an_unknown_entry_kind_is_refused_as_an_unknown_version) {
    auto b = encode(make_entry());
    b[10] = 2; // a future event entry, which this reader must not skip
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_unknown_version);
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

    auto newer_kind = grown(encode(make_entry()), max_entry_encoded_size, 10, 0x0200); // kind 2, flags 0
    BOOST_REQUIRE_THROW(decode_segment_entry(newer_kind), ledger_unknown_version);
}

BOOST_AUTO_TEST_CASE(an_object_past_the_general_maximum_is_refused) {
    auto b = encode(make_entry());
    b = b.substr(0, b.size() - sizeof(uint32_t)) + bytes(bytes::initialized_later(), max_ledger_object_size) +
        bytes(bytes::initialized_later(), sizeof(uint32_t));
    std::fill(b.begin() + min_entry_encoded_size, b.end(), bytes::value_type(0));
    BOOST_REQUIRE_GT(b.size(), max_ledger_object_size);
    BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
}

BOOST_AUTO_TEST_CASE(an_unknown_flag_bit_is_refused) {
    for (int bit = 0; bit < 8; ++bit) {
        auto b = encode(make_entry());
        b[11] = bytes::value_type(1 << bit);
        BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
    }
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
        reinterpret_cast<char*>(b.data())[82] = c;
        BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
    }
    // '.' and '..' would name the chain's directory or its parent.
    for (std::string_view dots : {".", ".."}) {
        auto e = make_entry();
        e.file_name = sstring(dots.size(), 'x');
        auto b = encode(e);
        std::copy(dots.begin(), dots.end(), reinterpret_cast<char*>(b.data()) + 82);
        BOOST_REQUIRE_THROW(decode_segment_entry(resealed(b)), ledger_format_error);
    }
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
            auto key = entry_object_key("pitr/site-a", test_host_id, shard, test_boot_epoch, test_chain, seq);
            auto parts = parse_entry_object_key(key);
            BOOST_REQUIRE_EQUAL(parts.chain.prefix, "pitr/site-a");
            BOOST_REQUIRE(parts.chain.host_id == test_host_id);
            BOOST_REQUIRE_EQUAL(parts.chain.shard, shard);
            BOOST_REQUIRE_EQUAL(parts.chain.boot_epoch_us, test_boot_epoch);
            BOOST_REQUIRE(parts.chain.chain == test_chain);
            BOOST_REQUIRE_EQUAL(parts.sequence, seq);
        }
    }
}

BOOST_AUTO_TEST_CASE(chain_prefix_and_head_key_round_trip) {
    auto chain_prefix = chain_key_prefix("pitr/site-a", test_host_id, 3, test_boot_epoch, test_chain);
    auto expected = chain_key_parts{"pitr/site-a", test_host_id, 3, test_boot_epoch, test_chain};
    BOOST_REQUIRE(parse_chain_key_prefix(chain_prefix) == expected);

    auto head = head_object_key("pitr/site-a", test_host_id, 3, test_boot_epoch, test_chain);
    BOOST_REQUIRE_EQUAL(head, chain_prefix + "head");
    BOOST_REQUIRE(parse_head_object_key(head) == expected);

    // Every key of one chain sits under the chain's prefix, and the head and the
    // entries are told apart by the suffix a listing filters on.
    BOOST_REQUIRE(entry_object_key("pitr/site-a", test_host_id, 3, test_boot_epoch, test_chain, 7).starts_with(chain_prefix));
    BOOST_REQUIRE(segment_object_key("pitr/site-a", test_host_id, 3, test_boot_epoch, test_chain, "CommitLog-2-9.log").starts_with(chain_prefix));
    BOOST_REQUIRE_THROW(parse_entry_object_key(head), ledger_format_error);
}

// What M1 is about: a restart keeps sequence numbers but gets a new chain, and
// its entries must not land on the old chain's.
BOOST_AUTO_TEST_CASE(a_new_chain_never_reuses_an_older_chains_keys) {
    auto older = entry_object_key("p", test_host_id, 3, 1000, test_chain, 0);
    auto same_boot_new_chain = entry_object_key("p", test_host_id, 3, 1000, chain_id(utils::UUID(1, 2)), 0);
    auto later_boot = entry_object_key("p", test_host_id, 3, 2000, chain_id(utils::UUID(1, 2)), 0);
    BOOST_REQUIRE(older != same_boot_new_chain);
    BOOST_REQUIRE(older != later_boot);
    // The boot epoch leads the chain's name, so chains list in the order they ran.
    BOOST_REQUIRE(older < later_boot);
    BOOST_REQUIRE(chain_key_prefix("p", test_host_id, 3, 999, test_chain) < chain_key_prefix("p", test_host_id, 3, 1000, test_chain));
}

// M3: a file name reused after a clock step must not overwrite another chain's
// segment, and segments must stay out of the entries' listing.
BOOST_AUTO_TEST_CASE(segment_keys_are_scoped_to_their_chain) {
    auto a = segment_object_key("p", test_host_id, 3, 1000, test_chain, "CommitLog-2-7.log");
    auto b = segment_object_key("p", test_host_id, 3, 2000, chain_id(utils::UUID(1, 2)), "CommitLog-2-7.log");
    BOOST_REQUIRE(a != b);
    BOOST_REQUIRE(a.ends_with("/segments/CommitLog-2-7.log"));
    BOOST_REQUIRE(!a.ends_with(entry_key_suffix));
    BOOST_REQUIRE_THROW(parse_entry_object_key(a), ledger_format_error);
    // The same name rule as the entry's, so a key can never leave the chain.
    for (sstring name : {sstring(""), sstring("."), sstring(".."), sstring("a/b"), sstring("a\x7f"), sstring(max_file_name_size + 1, 'x')}) {
        BOOST_REQUIRE_THROW(segment_object_key("p", test_host_id, 3, 1000, test_chain, name), ledger_format_error);
    }
}

BOOST_AUTO_TEST_CASE(entry_keys_sort_by_sequence) {
    std::vector<uint64_t> sequences{0, 1, 2, 9, 10, 99, 100, 1ull << 32, std::numeric_limits<uint64_t>::max() - 1, std::numeric_limits<uint64_t>::max()};
    for (size_t i = 1; i < sequences.size(); ++i) {
        auto lower = entry_object_key("p", test_host_id, 7, test_boot_epoch, test_chain, sequences[i - 1]);
        auto higher = entry_object_key("p", test_host_id, 7, test_boot_epoch, test_chain, sequences[i]);
        BOOST_REQUIRE_MESSAGE(lower < higher, fmt::format("'{}' should sort below '{}'", lower, higher));
    }
    // The shard component keeps its order too, so a listing of one prefix
    // groups shards in order.
    BOOST_REQUIRE(shard_key_prefix("p", test_host_id, 9) < shard_key_prefix("p", test_host_id, 10));
    BOOST_REQUIRE(shard_key_prefix("p", test_host_id, 10) < shard_key_prefix("p", test_host_id, std::numeric_limits<uint32_t>::max()));
}

BOOST_AUTO_TEST_CASE(keys_that_were_not_built_here_are_refused) {
    auto good = entry_object_key("pitr/site-a", test_host_id, 3, test_boot_epoch, test_chain, 11);
    BOOST_REQUIRE_NO_THROW(parse_entry_object_key(good));

    const char* chain_dir = "00000001762300000000-11112222-0000-4000-8000-00000000000a";
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
            key("pitr", host, "0000000003", chain_dir, "x/" + seq),         // a component below the chain
            key("pitr", host, "0000000003", chain_dir, "0000000000000000001x.entry"),
            key("pitr", host, "0000000003", chain_dir, " 0000000000000000011.entry"),
            key("pitr", host, "9999999999", chain_dir, seq),                                            // shard past uint32
            key("pitr", host, "0000000003", "11112222-0000-4000-8000-00000000000a", seq),               // no boot epoch
            key("pitr", host, "0000000003", "1762300000000-11112222-0000-4000-8000-00000000000a", seq), // epoch not padded
            key("pitr", host, "0000000003", "00000001762300000000-not-a-uuid", seq),
            key("pitr", host, "0000000003", "00000001762300000000-11112222-0000-4000-8000-00000000000A", seq), // chain not canonical
            key("pitr", host, "0000000003", "99999999999999999999-11112222-0000-4000-8000-00000000000a", seq), // epoch past int64
    };
    for (auto& k : bad) {
        BOOST_REQUIRE_THROW(parse_entry_object_key(k), ledger_format_error);
    }
    // The head and the chain prefix hold to the same rules.
    BOOST_REQUIRE_THROW(parse_head_object_key(key("pitr/", host, "0000000003", chain_dir, "head")), ledger_format_error);
    BOOST_REQUIRE_THROW(parse_head_object_key(good), ledger_format_error);
    BOOST_REQUIRE_NO_THROW(parse_chain_key_prefix(key("pitr", host, "0000000003", chain_dir, "")));
    BOOST_REQUIRE_THROW(parse_chain_key_prefix(key("pitr", host, "0000000003", chain_dir, "segments/")), ledger_format_error);
    BOOST_REQUIRE_THROW(parse_chain_key_prefix(fmt::format("pitr/{}/0000000003/{}", host, chain_dir)), ledger_format_error); // no trailing '/'
}

BOOST_AUTO_TEST_CASE(an_unusable_prefix_is_refused_by_the_builders) {
    for (sstring prefix : {sstring(""), sstring("/"), sstring("pitr/"), sstring("/pitr"), sstring("pitr//site-a")}) {
        BOOST_REQUIRE_THROW(entry_object_key(prefix, test_host_id, 0, test_boot_epoch, test_chain, 0), ledger_format_error);
        BOOST_REQUIRE_THROW(head_object_key(prefix, test_host_id, 0, test_boot_epoch, test_chain), ledger_format_error);
        BOOST_REQUIRE_THROW(shard_key_prefix(prefix, test_host_id, 0), ledger_format_error);
    }
}

BOOST_AUTO_TEST_CASE(a_boot_epoch_with_no_chain_directory_is_refused) {
    BOOST_REQUIRE_THROW(chain_key_prefix("p", test_host_id, 0, -1, test_chain), ledger_format_error);
    BOOST_REQUIRE_NO_THROW(chain_key_prefix("p", test_host_id, 0, 0, test_chain));
}
