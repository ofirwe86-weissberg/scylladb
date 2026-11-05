/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/ledger_entry.hh"

#include <algorithm>
#include <charconv>
#include <limits>

#include <fmt/format.h>
#include <seastar/core/byteorder.hh>
#include <seastar/core/format.hh>

#include "utils/crc.hh"

namespace db::pitr {

namespace {

// 8 bytes, so an object that is not a ledger entry at all is refused before
// anything else is believed.
constexpr char entry_magic[8] = {'P', 'I', 'T', 'R', 'L', 'D', 'G', 'R'};
// Only segments for now. Events (TRUNCATE, schema) become further kinds, and
// refusing an unknown kind keeps an older reader from skipping one.
constexpr uint8_t entry_kind_segment = 1;

// Offsets of version 1. Writing them out beats arithmetic in two places, and a
// change here fails the golden-vector test.
constexpr size_t off_magic = 0;
constexpr size_t off_version = 8;
constexpr size_t off_kind = 10;
constexpr size_t off_reserved = 11;
constexpr size_t off_chain_msb = 12;
constexpr size_t off_chain_lsb = 20;
constexpr size_t off_sequence = 28;
constexpr size_t off_boot_epoch = 36;
constexpr size_t off_segment_id = 44;
constexpr size_t off_prev_segment_id = 52;
constexpr size_t off_used_size = 60;
constexpr size_t off_segment_crc = 68;
constexpr size_t off_upload_time = 72;
constexpr size_t off_file_name_size = 80;
constexpr size_t off_file_name = 82;

uint32_t checksum(const char* p, size_t size) {
    utils::crc32 crc;
    crc.process(reinterpret_cast<const uint8_t*>(p), size);
    return crc.get();
}

bool valid_file_name(std::string_view name) {
    if (name.empty() || name.size() > max_file_name_size) {
        return false;
    }
    // A separator would let an entry point a fetch outside the prefix, and a
    // control character would corrupt every message naming the object.
    return std::all_of(name.begin(), name.end(), [](char c) {
        return c != '/' && static_cast<unsigned char>(c) >= 0x20 && static_cast<unsigned char>(c) < 0x7f;
    });
}

// Exactly `digits` decimal digits, nothing else: no sign, no space, no short
// form, so a key that would not round-trip is refused.
uint64_t parse_fixed_digits(std::string_view s, size_t digits, std::string_view what) {
    if (s.size() != digits || !std::all_of(s.begin(), s.end(), [](char c) {
            return c >= '0' && c <= '9';
        })) {
        throw ledger_format_error(fmt::format("pitr ledger: {} is not {} decimal digits: '{}'", what, digits, s));
    }
    uint64_t v = 0;
    auto res = std::from_chars(s.data(), s.data() + s.size(), v);
    if (res.ec != std::errc() || res.ptr != s.data() + s.size()) {
        throw ledger_format_error(fmt::format("pitr ledger: {} does not fit: '{}'", what, s));
    }
    return v;
}

} // namespace

bytes encode(const segment_entry& e) {
    if (!valid_file_name(e.file_name)) {
        throw ledger_format_error(fmt::format("pitr ledger: segment file name is empty, too long, or holds '/' or a control character: '{}'", e.file_name));
    }
    if (e.segment_id == 0) {
        throw ledger_format_error("pitr ledger: segment id 0 is never a shipped segment");
    }
    if (e.used_size == 0) {
        throw ledger_format_error("pitr ledger: used size 0 is never a shipped segment");
    }

    bytes buf(bytes::initialized_later(), off_file_name + e.file_name.size() + sizeof(uint32_t));
    auto* p = reinterpret_cast<char*>(buf.data());
    std::copy(std::begin(entry_magic), std::end(entry_magic), p + off_magic);
    seastar::write_be<uint16_t>(p + off_version, ledger_format_version_1);
    seastar::write_be<uint8_t>(p + off_kind, entry_kind_segment);
    seastar::write_be<uint8_t>(p + off_reserved, 0);
    seastar::write_be<int64_t>(p + off_chain_msb, e.chain.uuid().get_most_significant_bits());
    seastar::write_be<int64_t>(p + off_chain_lsb, e.chain.uuid().get_least_significant_bits());
    seastar::write_be<uint64_t>(p + off_sequence, e.sequence);
    seastar::write_be<int64_t>(p + off_boot_epoch, e.boot_epoch_us);
    seastar::write_be<uint64_t>(p + off_segment_id, e.segment_id);
    seastar::write_be<uint64_t>(p + off_prev_segment_id, e.prev_segment_id);
    seastar::write_be<uint64_t>(p + off_used_size, e.used_size);
    seastar::write_be<uint32_t>(p + off_segment_crc, e.segment_crc32);
    seastar::write_be<int64_t>(p + off_upload_time, e.upload_time_us);
    seastar::write_be<uint16_t>(p + off_file_name_size, e.file_name.size());
    std::copy(e.file_name.begin(), e.file_name.end(), p + off_file_name);
    auto body_size = off_file_name + e.file_name.size();
    seastar::write_be<uint32_t>(p + body_size, checksum(p, body_size));
    return buf;
}

segment_entry decode_segment_entry(bytes_view in) {
    if (in.size() < min_entry_encoded_size || in.size() > max_entry_encoded_size) {
        throw ledger_format_error(fmt::format("pitr ledger: entry of {} bytes is outside [{}, {}]", in.size(), min_entry_encoded_size, max_entry_encoded_size));
    }
    const auto* p = reinterpret_cast<const char*>(in.data());
    if (!std::equal(std::begin(entry_magic), std::end(entry_magic), p + off_magic)) {
        throw ledger_format_error("pitr ledger: entry does not start with the ledger magic");
    }
    auto version = seastar::read_be<uint16_t>(p + off_version);
    if (version > ledger_format_version_1) {
        throw ledger_unknown_version(
                fmt::format("pitr ledger: entry format version {} is newer than {}, this node cannot read it", version, ledger_format_version_1));
    }
    if (version != ledger_format_version_1) {
        throw ledger_format_error(fmt::format("pitr ledger: entry format version {} is not a version any writer emits", version));
    }
    // The declared name length fixes the whole size, so one comparison refuses
    // both a truncated object and one with bytes appended.
    auto name_size = seastar::read_be<uint16_t>(p + off_file_name_size);
    auto body_size = off_file_name + size_t(name_size);
    auto total_size = body_size + sizeof(uint32_t);
    if (total_size != in.size()) {
        throw ledger_format_error(fmt::format("pitr ledger: entry declares a {}-byte name, so {} bytes, but holds {}", name_size, total_size, in.size()));
    }
    auto stored_crc = seastar::read_be<uint32_t>(p + body_size);
    auto computed_crc = checksum(p, body_size);
    if (stored_crc != computed_crc) {
        throw ledger_format_error(fmt::format("pitr ledger: entry checksum is {:#010x}, computed {:#010x}", stored_crc, computed_crc));
    }
    auto kind = seastar::read_be<uint8_t>(p + off_kind);
    if (kind != entry_kind_segment) {
        throw ledger_format_error(fmt::format("pitr ledger: entry kind {} is not a segment entry", kind));
    }
    if (seastar::read_be<uint8_t>(p + off_reserved) != 0) {
        throw ledger_format_error("pitr ledger: reserved byte is not 0");
    }

    segment_entry e;
    e.chain = chain_id(utils::UUID(seastar::read_be<int64_t>(p + off_chain_msb), seastar::read_be<int64_t>(p + off_chain_lsb)));
    e.sequence = seastar::read_be<uint64_t>(p + off_sequence);
    e.boot_epoch_us = seastar::read_be<int64_t>(p + off_boot_epoch);
    e.segment_id = seastar::read_be<uint64_t>(p + off_segment_id);
    e.prev_segment_id = seastar::read_be<uint64_t>(p + off_prev_segment_id);
    e.used_size = seastar::read_be<uint64_t>(p + off_used_size);
    e.segment_crc32 = seastar::read_be<uint32_t>(p + off_segment_crc);
    e.upload_time_us = seastar::read_be<int64_t>(p + off_upload_time);
    e.file_name = sstring(p + off_file_name, name_size);

    // A checksum that matches still leaves values no writer emits, which would
    // make the chain check believe a segment it can never fetch.
    if (e.segment_id == 0) {
        throw ledger_format_error("pitr ledger: entry holds segment id 0");
    }
    if (e.used_size == 0) {
        throw ledger_format_error("pitr ledger: entry holds used size 0");
    }
    if (!valid_file_name(e.file_name)) {
        throw ledger_format_error(fmt::format("pitr ledger: entry holds an unusable segment file name: '{}'", e.file_name));
    }
    return e;
}

sstring shard_key_prefix(std::string_view prefix, const utils::UUID& host_id, uint32_t shard) {
    if (prefix.empty() || prefix.back() == '/') {
        throw ledger_format_error(fmt::format("pitr ledger: object prefix is empty or ends in '/': '{}'", prefix));
    }
    return fmt::format("{}/{}/{:0{}}/", prefix, host_id, shard, shard_key_digits);
}

sstring entry_object_key(std::string_view prefix, const utils::UUID& host_id, uint32_t shard, uint64_t sequence) {
    return fmt::format("{}{:0{}}{}", shard_key_prefix(prefix, host_id, shard), sequence, sequence_key_digits, entry_key_suffix);
}

entry_key_parts parse_entry_object_key(std::string_view key) {
    auto fail = [&](std::string_view why) {
        throw ledger_format_error(fmt::format("pitr ledger: '{}' is not an entry key: {}", key, why));
    };
    auto suffix = std::string_view(entry_key_suffix);
    if (!key.ends_with(suffix)) {
        fail("it does not end in .entry");
    }
    key.remove_suffix(suffix.size());
    auto seq_start = key.rfind('/');
    if (seq_start == std::string_view::npos) {
        fail("it has no shard component");
    }
    auto sequence = parse_fixed_digits(key.substr(seq_start + 1), sequence_key_digits, "sequence");
    key = key.substr(0, seq_start);
    auto shard_start = key.rfind('/');
    if (shard_start == std::string_view::npos) {
        fail("it has no host id component");
    }
    auto shard = parse_fixed_digits(key.substr(shard_start + 1), shard_key_digits, "shard");
    if (shard > std::numeric_limits<uint32_t>::max()) {
        fail("its shard is out of range");
    }
    key = key.substr(0, shard_start);
    auto host_start = key.rfind('/');
    if (host_start == std::string_view::npos) {
        fail("it has no prefix before the host id");
    }
    auto host_text = key.substr(host_start + 1);
    utils::UUID host_id;
    try {
        host_id = utils::UUID(host_text);
    } catch (...) {
        fail("its host id is not a UUID");
    }
    // `utils::UUID` accepts forms it does not print, and an unrelated object
    // must not parse as an entry, so only the canonical text is taken.
    if (fmt::to_string(host_id) != host_text) {
        fail("its host id is not in canonical form");
    }
    auto prefix = key.substr(0, host_start);
    if (prefix.empty()) {
        fail("its prefix is empty");
    }
    return entry_key_parts{sstring(prefix), host_id, uint32_t(shard), sequence};
}

} // namespace db::pitr
