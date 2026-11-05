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
// The highest kind this reader knows. An unknown number may be a kind a newer
// writer added, which an older reader must never skip.
constexpr uint8_t highest_known_kind = object_kind_head;

// Offsets of version 1. Writing them out beats arithmetic in two places, and a
// change here fails the golden-vector test.
constexpr size_t off_magic = 0;
constexpr size_t off_version = 8;
constexpr size_t off_kind = 10;
constexpr size_t off_flags = 11;
constexpr size_t off_chain_msb = 12;
constexpr size_t off_chain_lsb = 20;
constexpr size_t off_sequence = 28;
constexpr size_t off_boot_epoch = 36;
constexpr size_t off_segment_id = 44;
constexpr size_t off_prev_segment_id = 52;
constexpr size_t off_used_size = 60;
constexpr size_t off_segment_crc = 68;
constexpr size_t off_upload_time = 72;
constexpr size_t off_covered_through = 80;
constexpr size_t off_chain_start = 88;
constexpr size_t off_host_id_msb = 96;
constexpr size_t off_host_id_lsb = 104;
constexpr size_t off_shard = 112;
constexpr size_t off_file_name_size = 116;
constexpr size_t off_file_name = 118;
// The extension area's length follows the name, so its offset depends on the
// name's size: `off_file_name + name_size`, then its bytes, then the checksum.
constexpr size_t extension_size_bytes = sizeof(uint16_t);

uint32_t checksum(const char* p, size_t size) {
    utils::crc32 crc;
    crc.process(reinterpret_cast<const uint8_t*>(p), size);
    return crc.get();
}

bool valid_file_name(std::string_view name) {
    if (name.empty() || name.size() > max_file_name_size) {
        return false;
    }
    // '.' and '..' would name the chain's own directory or its parent once a
    // fetch joins them to a path.
    if (name == "." || name == "..") {
        return false;
    }
    // A separator would let an entry point a fetch outside the prefix, and
    // anything outside printable ASCII, including 0x7f and non-ASCII bytes,
    // would corrupt every message naming the object.
    return std::all_of(name.begin(), name.end(), [](char c) {
        return c != '/' && static_cast<unsigned char>(c) >= 0x20 && static_cast<unsigned char>(c) < 0x7f;
    });
}

// The same rule in the builders and in the parsers, so a key either round-trips
// or is refused on both sides.
void check_prefix(std::string_view prefix) {
    if (prefix.empty() || prefix.front() == '/' || prefix.back() == '/' || prefix.find("//") != std::string_view::npos) {
        throw ledger_format_error(fmt::format("pitr ledger: object prefix is empty, starts or ends in '/', or holds an empty component: '{}'", prefix));
    }
    // '.' or '..' in the prefix would give the same objects a second key, and
    // one that climbs out of the prefix.
    for (size_t at = 0; at <= prefix.size();) {
        auto end = std::min(prefix.find('/', at), prefix.size());
        auto component = prefix.substr(at, end - at);
        if (component == "." || component == "..") {
            throw ledger_format_error(fmt::format("pitr ledger: object prefix holds a '{}' component: '{}'", component, prefix));
        }
        at = end + 1;
    }
}

void check_time(int64_t us, std::string_view what) {
    if (us < 0) {
        throw ledger_format_error(fmt::format("pitr ledger: {} {} is before the UNIX epoch, so it would not sort", what, us));
    }
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
        throw ledger_format_error(fmt::format(
                "pitr ledger: segment file name is empty, too long, '.' or '..', or holds '/' or a character outside printable ASCII: '{}'", e.file_name));
    }
    check_time(e.boot_epoch_us, "boot epoch");
    check_time(e.upload_time_us, "upload time");
    check_time(e.covered_through_us, "covered-through time");
    check_time(e.chain_start_us, "chain start");
    if (e.segment_id == 0) {
        throw ledger_format_error("pitr ledger: segment id 0 is never a shipped segment");
    }
    if (e.used_size == 0) {
        throw ledger_format_error("pitr ledger: used size 0 is never a shipped segment");
    }
    // A null id is what a zeroed or unwritten object holds, so it is refused
    // here for the same reason as segment id 0.
    if (e.chain.uuid().is_null()) {
        throw ledger_format_error("pitr ledger: the null uuid is never a chain id");
    }
    if (e.host_id.is_null()) {
        throw ledger_format_error("pitr ledger: the null uuid is never a host id");
    }

    bytes buf(bytes::initialized_later(), off_file_name + e.file_name.size() + extension_size_bytes + sizeof(uint32_t));
    auto* p = reinterpret_cast<char*>(buf.data());
    std::copy(std::begin(entry_magic), std::end(entry_magic), p + off_magic);
    seastar::write_be<uint16_t>(p + off_version, ledger_format_version_1);
    seastar::write_be<uint8_t>(p + off_kind, object_kind_segment_entry);
    seastar::write_be<uint8_t>(p + off_flags, 0);
    seastar::write_be<int64_t>(p + off_chain_msb, e.chain.uuid().get_most_significant_bits());
    seastar::write_be<int64_t>(p + off_chain_lsb, e.chain.uuid().get_least_significant_bits());
    seastar::write_be<uint64_t>(p + off_sequence, e.sequence);
    seastar::write_be<int64_t>(p + off_boot_epoch, e.boot_epoch_us);
    seastar::write_be<uint64_t>(p + off_segment_id, e.segment_id);
    seastar::write_be<uint64_t>(p + off_prev_segment_id, e.prev_segment_id);
    seastar::write_be<uint64_t>(p + off_used_size, e.used_size);
    seastar::write_be<uint32_t>(p + off_segment_crc, e.segment_crc32);
    seastar::write_be<int64_t>(p + off_upload_time, e.upload_time_us);
    seastar::write_be<int64_t>(p + off_covered_through, e.covered_through_us);
    seastar::write_be<int64_t>(p + off_chain_start, e.chain_start_us);
    seastar::write_be<int64_t>(p + off_host_id_msb, e.host_id.get_most_significant_bits());
    seastar::write_be<int64_t>(p + off_host_id_lsb, e.host_id.get_least_significant_bits());
    seastar::write_be<uint32_t>(p + off_shard, e.shard);
    seastar::write_be<uint16_t>(p + off_file_name_size, e.file_name.size());
    std::copy(e.file_name.begin(), e.file_name.end(), p + off_file_name);
    // Version 1 has nothing to put in the extension area, but the length field
    // is always there, so a later writer can fill it without a new version.
    seastar::write_be<uint16_t>(p + off_file_name + e.file_name.size(), 0);
    auto body_size = off_file_name + e.file_name.size() + extension_size_bytes;
    seastar::write_be<uint32_t>(p + body_size, checksum(p, body_size));
    return buf;
}

segment_entry decode_segment_entry(bytes_view in) {
    // 1. The size every object of every kind has, then the magic, so an object
    //    that is not part of the ledger is refused before anything is believed.
    if (in.size() < min_ledger_object_size) {
        throw ledger_format_error(fmt::format("pitr ledger: object of {} bytes is shorter than the {}-byte common header", in.size(), min_ledger_object_size));
    }
    const auto* p = reinterpret_cast<const char*>(in.data());
    if (!std::equal(std::begin(entry_magic), std::end(entry_magic), p + off_magic)) {
        throw ledger_format_error("pitr ledger: object does not start with the ledger magic");
    }
    // 2. The checksum over the whole object, bounded first. Before the version,
    //    so damage is never reported as a format this node cannot read.
    if (in.size() > max_ledger_object_size) {
        throw ledger_format_error(fmt::format("pitr ledger: object of {} bytes is larger than the {}-byte maximum", in.size(), max_ledger_object_size));
    }
    auto body_size = in.size() - sizeof(uint32_t);
    auto stored_crc = seastar::read_be<uint32_t>(p + body_size);
    auto computed_crc = checksum(p, body_size);
    if (stored_crc != computed_crc) {
        throw ledger_format_error(fmt::format("pitr ledger: object checksum is {:#010x}, computed {:#010x}", stored_crc, computed_crc));
    }
    // 3. The version, 4. the kind, both of which a newer writer may have moved
    //    beyond this reader, and 5. the flags.
    auto version = seastar::read_be<uint16_t>(p + off_version);
    if (version > ledger_format_version_1) {
        throw ledger_unknown_version(
                fmt::format("pitr ledger: object format version {} is newer than {}, this node cannot read it", version, ledger_format_version_1));
    }
    if (version != ledger_format_version_1) {
        throw ledger_format_error(fmt::format("pitr ledger: object format version {} is not a version any writer emits", version));
    }
    auto kind = seastar::read_be<uint8_t>(p + off_kind);
    // Kind 0 is never assigned, so an intact 0 is damage and not a newer
    // writer's work, the same reasoning as version 0.
    if (kind == 0) {
        throw ledger_format_error("pitr ledger: object kind 0 is not a kind any writer emits");
    }
    if (kind > highest_known_kind) {
        throw ledger_unknown_version(fmt::format("pitr ledger: object kind {} is not a kind this node knows", kind));
    }
    if (kind != object_kind_segment_entry) {
        throw ledger_format_error(fmt::format("pitr ledger: object kind {} is not a segment entry", kind));
    }
    // An advisory bit this reader does not know is ignored on purpose; a
    // must-understand bit changes how the object reads, so it is refused.
    auto flags = seastar::read_be<uint8_t>(p + off_flags);
    if (flags & must_understand_flags_mask) {
        throw ledger_format_error(
                fmt::format("pitr ledger: object sets must-understand flags {:#04x}, none of which this node knows", flags & must_understand_flags_mask));
    }
    // 6. Only now the segment entry's own size and bounds. The declared name
    //    and extension lengths together fix the whole size, so one comparison
    //    refuses both a truncated object and one with bytes appended. The
    //    general maximum above already caps the total.
    if (in.size() < min_entry_encoded_size) {
        throw ledger_format_error(fmt::format("pitr ledger: segment entry of {} bytes is shorter than the {}-byte minimum", in.size(), min_entry_encoded_size));
    }
    auto name_size = seastar::read_be<uint16_t>(p + off_file_name_size);
    // The extension length sits behind the name, so the name has to fit before
    // it can be read at all.
    auto least_size = off_file_name + size_t(name_size) + extension_size_bytes + sizeof(uint32_t);
    if (least_size > in.size()) {
        throw ledger_format_error(
                fmt::format("pitr ledger: entry declares a {}-byte name, so at least {} bytes, but holds {}", name_size, least_size, in.size()));
    }
    // Skipped, not parsed: the records inside are advisory, and a version 1
    // reader knows no tag. The checksum already covered them.
    auto extension_size = seastar::read_be<uint16_t>(p + off_file_name + size_t(name_size));
    if (least_size + size_t(extension_size) != in.size()) {
        throw ledger_format_error(fmt::format("pitr ledger: entry declares a {}-byte name and a {}-byte extension, so {} bytes, but holds {}", name_size,
                extension_size, least_size + size_t(extension_size), in.size()));
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
    // The two times are not compared with each other: both come from the wall
    // clock, which can step backwards, so a fast seal and upload can record a
    // coverage time above the upload's. Only the coverage time is used.
    e.covered_through_us = seastar::read_be<int64_t>(p + off_covered_through);
    // Not compared with the times above either, for the same reason, and not
    // compared with the key here: that is the chain reader's job, which holds
    // both.
    e.chain_start_us = seastar::read_be<int64_t>(p + off_chain_start);
    // The rest of the key the chain reader compares, here for the same reason:
    // nothing checksums a key, so a chain moved whole under another host or
    // shard would otherwise be believed.
    e.host_id = utils::UUID(seastar::read_be<int64_t>(p + off_host_id_msb), seastar::read_be<int64_t>(p + off_host_id_lsb));
    e.shard = seastar::read_be<uint32_t>(p + off_shard);
    e.file_name = sstring(p + off_file_name, name_size);

    // A checksum that matches still leaves values no writer emits, which would
    // make the chain check believe a segment it can never fetch.
    if (e.segment_id == 0) {
        throw ledger_format_error("pitr ledger: entry holds segment id 0");
    }
    if (e.used_size == 0) {
        throw ledger_format_error("pitr ledger: entry holds used size 0");
    }
    if (e.chain.uuid().is_null()) {
        throw ledger_format_error("pitr ledger: entry holds the null uuid as its chain id");
    }
    if (e.host_id.is_null()) {
        throw ledger_format_error("pitr ledger: entry holds the null uuid as its host id");
    }
    if (e.boot_epoch_us < 0) {
        throw ledger_format_error(fmt::format("pitr ledger: entry holds boot epoch {}, which has no chain directory", e.boot_epoch_us));
    }
    if (e.upload_time_us < 0 || e.covered_through_us < 0) {
        throw ledger_format_error(fmt::format(
                "pitr ledger: entry holds upload time {} and covered-through {}, one of them before the UNIX epoch", e.upload_time_us, e.covered_through_us));
    }
    if (e.chain_start_us < 0) {
        throw ledger_format_error(fmt::format("pitr ledger: entry holds chain start {}, which has no chain directory", e.chain_start_us));
    }
    if (!valid_file_name(e.file_name)) {
        throw ledger_format_error(fmt::format("pitr ledger: entry holds an unusable segment file name: '{}'", e.file_name));
    }
    return e;
}

sstring shard_key_prefix(std::string_view prefix, const utils::UUID& host_id, uint32_t shard) {
    check_prefix(prefix);
    return fmt::format("{}/{}/{:0{}}/", prefix, host_id, shard, shard_key_digits);
}

sstring chain_key_prefix(const chain_key_parts& c) {
    check_time(c.boot_epoch_us, "boot epoch");
    check_time(c.chain_start_us, "chain start");
    return fmt::format("{}{:0{}}-{:0{}}-{}/", shard_key_prefix(c.prefix, c.host_id, c.shard), c.boot_epoch_us, boot_epoch_key_digits, c.chain_start_us,
            chain_start_key_digits, c.chain.uuid());
}

sstring entry_object_key(const chain_key_parts& c, uint64_t sequence) {
    return fmt::format("{}{:0{}}{}", chain_key_prefix(c), sequence, sequence_key_digits, entry_key_suffix);
}

sstring head_object_key(const chain_key_parts& c) {
    return chain_key_prefix(c) + head_key_name;
}

sstring segment_object_key(const chain_key_parts& c, std::string_view file_name) {
    if (!valid_file_name(file_name)) {
        throw ledger_format_error(fmt::format("pitr ledger: '{}' is not a usable segment file name", file_name));
    }
    return fmt::format("{}{}/{}", chain_key_prefix(c), segments_key_component, file_name);
}

namespace {

// Takes the components right to left, which is what lets the prefix itself hold
// '/'. `key` is left holding the prefix.
chain_key_parts parse_chain_components(std::string_view& key, std::string_view whole) {
    auto fail = [&](std::string_view why) {
        throw ledger_format_error(fmt::format("pitr ledger: '{}' is not a ledger key: {}", whole, why));
    };
    auto next_component = [&](std::string_view what) {
        auto start = key.rfind('/');
        if (start == std::string_view::npos) {
            fail(what);
        }
        auto component = key.substr(start + 1);
        key = key.substr(0, start);
        return component;
    };

    auto chain_text = next_component("it has no shard component before the chain");
    auto times_size = boot_epoch_key_digits + 1 + chain_start_key_digits + 1;
    if (chain_text.size() <= times_size || chain_text[boot_epoch_key_digits] != '-' || chain_text[times_size - 1] != '-') {
        fail("its chain does not name a boot epoch and a start time");
    }
    auto boot_epoch = parse_fixed_digits(chain_text.substr(0, boot_epoch_key_digits), boot_epoch_key_digits, "boot epoch");
    auto chain_start = parse_fixed_digits(chain_text.substr(boot_epoch_key_digits + 1, chain_start_key_digits), chain_start_key_digits, "chain start");
    if (boot_epoch > uint64_t(std::numeric_limits<int64_t>::max())) {
        fail("its boot epoch is out of range");
    }
    if (chain_start > uint64_t(std::numeric_limits<int64_t>::max())) {
        fail("its chain start is out of range");
    }
    auto chain_uuid_text = chain_text.substr(times_size);
    utils::UUID chain_uuid;
    try {
        chain_uuid = utils::UUID(chain_uuid_text);
    } catch (...) {
        fail("its chain id is not a UUID");
    }
    if (fmt::to_string(chain_uuid) != chain_uuid_text) {
        fail("its chain id is not in canonical form");
    }

    auto shard = parse_fixed_digits(next_component("it has no host id component"), shard_key_digits, "shard");
    if (shard > std::numeric_limits<uint32_t>::max()) {
        fail("its shard is out of range");
    }

    auto host_text = next_component("it has no prefix before the host id");
    utils::UUID host_id;
    try {
        host_id = utils::UUID(host_text);
    } catch (...) {
        fail("its host id is not a UUID");
    }
    // `utils::UUID` accepts forms it does not print, and an unrelated object
    // must not parse as part of the ledger, so only the canonical text is taken.
    if (fmt::to_string(host_id) != host_text) {
        fail("its host id is not in canonical form");
    }
    // The same rule the builders keep, so a key either round-trips or is
    // refused: a prefix that is empty, bounded by '/' or holds an empty
    // component would make a second key for the same entry.
    check_prefix(key);
    return chain_key_parts{sstring(key), host_id, uint32_t(shard), int64_t(boot_epoch), int64_t(chain_start), chain_id(chain_uuid)};
}

} // namespace

chain_key_parts parse_chain_key_prefix(std::string_view key) {
    if (!key.ends_with('/')) {
        throw ledger_format_error(fmt::format("pitr ledger: '{}' is not a chain prefix: it does not end in '/'", key));
    }
    auto rest = key.substr(0, key.size() - 1);
    return parse_chain_components(rest, key);
}

entry_key_parts parse_entry_object_key(std::string_view key) {
    auto suffix = std::string_view(entry_key_suffix);
    if (!key.ends_with(suffix)) {
        throw ledger_format_error(fmt::format("pitr ledger: '{}' is not an entry key: it does not end in {}", key, entry_key_suffix));
    }
    auto rest = key.substr(0, key.size() - suffix.size());
    auto seq_start = rest.rfind('/');
    if (seq_start == std::string_view::npos) {
        throw ledger_format_error(fmt::format("pitr ledger: '{}' is not an entry key: it has no chain component", key));
    }
    auto sequence = parse_fixed_digits(rest.substr(seq_start + 1), sequence_key_digits, "sequence");
    rest = rest.substr(0, seq_start);
    return entry_key_parts{parse_chain_components(rest, key), sequence};
}

chain_key_parts parse_head_object_key(std::string_view key) {
    auto name = fmt::format("/{}", head_key_name);
    if (!key.ends_with(std::string_view(name))) {
        throw ledger_format_error(fmt::format("pitr ledger: '{}' is not a head key: it does not end in /{}", key, head_key_name));
    }
    auto rest = key.substr(0, key.size() - name.size());
    return parse_chain_components(rest, key);
}

} // namespace db::pitr
