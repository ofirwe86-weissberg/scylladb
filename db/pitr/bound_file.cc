/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/bound_file.hh"

#include <algorithm>
#include <charconv>
#include <string_view>

#include <fmt/format.h>

#include "utils/crc.hh"

namespace db::pitr {

namespace {

constexpr std::string_view format_key = "format";
constexpr std::string_view format_value = "pitr-bound";
constexpr std::string_view crc_key = "crc32";
constexpr std::string_view none_value = "none";

[[noreturn]] void refuse(sstring what) {
    throw bound_file_format_error(std::move(what));
}

uint32_t checksum(std::string_view body) {
    utils::crc32 crc;
    crc.process(reinterpret_cast<const uint8_t*>(body.data()), body.size());
    return crc.get();
}

void check_time(int64_t us, std::string_view what) {
    if (us < 0) {
        refuse(fmt::format("pitr bound file: {} {} is before the UNIX epoch", what, us));
    }
}

void check_id(const utils::UUID& id, std::string_view what) {
    if (id.is_null()) {
        refuse(fmt::format("pitr bound file: the null uuid is never a {}", what));
    }
}

void check_floor(const std::optional<uint64_t>& floor) {
    // 0 is no segment's id, so it is never a stand-in for "none" or "unknown":
    // a restore comparing a bound against it would pass every chain.
    if (floor && *floor == 0) {
        refuse("pitr bound file: a chain's floor segment id is never 0; a chain with no floor records 'none'");
    }
}

// Walks the lines of a newline-terminated body without copying or collecting
// them, so nothing is allocated in proportion to the line count.
class line_cursor {
    std::string_view _rest;
    size_t _line = 0;

public:
    explicit line_cursor(std::string_view body) noexcept
        : _rest(body) {
    }

    bool at_end() const noexcept {
        return _rest.empty();
    }

    size_t lines_read() const noexcept {
        return _line;
    }

    // The next line's value. Positional: the key has to be the one the format
    // puts here, which refuses an unknown key, a duplicate, a missing one and a
    // reordering alike.
    std::string_view value_of(std::string_view key) {
        if (_rest.empty()) {
            refuse(fmt::format("pitr bound file: ends before the '{}' line", key));
        }
        ++_line;
        // The body always ends in '\n', so every line here is terminated.
        auto end = _rest.find('\n');
        auto line = _rest.substr(0, end);
        _rest.remove_prefix(end + 1);
        if (line.empty()) {
            refuse(fmt::format("pitr bound file: line {} is empty", _line));
        }
        auto eq = line.find('=');
        if (eq == std::string_view::npos) {
            refuse(fmt::format("pitr bound file: line {} is not key=value: '{}'", _line, line));
        }
        if (line.substr(0, eq) != key) {
            refuse(fmt::format("pitr bound file: line {} is '{}', expected key '{}'", _line, line.substr(0, eq), key));
        }
        return line.substr(eq + 1);
    }
};

template <typename T>
T parse_decimal(std::string_view s, std::string_view what) {
    // No sign, no space and no leading zero, so a value has exactly one
    // spelling and a file either round-trips or is refused.
    bool plain = !s.empty() && std::all_of(s.begin(), s.end(), [](char c) {
        return c >= '0' && c <= '9';
    }) && (s.size() == 1 || s.front() != '0');
    if (!plain) {
        refuse(fmt::format("pitr bound file: {} is not a plain decimal number: '{}'", what, s));
    }
    T v = 0;
    auto res = std::from_chars(s.data(), s.data() + s.size(), v);
    if (res.ec != std::errc() || res.ptr != s.data() + s.size()) {
        refuse(fmt::format("pitr bound file: {} does not fit its field: '{}'", what, s));
    }
    return v;
}

utils::UUID parse_uuid(std::string_view s, std::string_view what) {
    utils::UUID id;
    try {
        id = utils::UUID(s);
    } catch (...) {
        refuse(fmt::format("pitr bound file: {} is not a uuid: '{}'", what, s));
    }
    // UUID's own parser erases dashes and takes upper case, so the only way to
    // keep one spelling per id, and the checksum meaningful, is to compare the
    // canonical form with what was read.
    if (fmt::format("{}", id) != s) {
        refuse(fmt::format("pitr bound file: {} is not a canonical uuid: '{}'", what, s));
    }
    check_id(id, what);
    return id;
}

} // namespace

sstring encode(const bound_file& f) {
    check_time(f.capture_time_us, "capture time");
    check_id(f.schema_version.uuid(), "schema version");
    if (f.shards.empty()) {
        refuse("pitr bound file: a snapshot always covers at least one shard");
    }
    // Before a byte is formatted, so a count no replay position could name
    // never builds a file no reader would take.
    if (f.shards.size() > max_shards_in_a_bound_file) {
        refuse(fmt::format("pitr bound file: {} shards is more than the {} a replay position can name", f.shards.size(), max_shards_in_a_bound_file));
    }

    fmt::memory_buffer out;
    fmt::format_to(std::back_inserter(out), "{}={}\nversion={}\ncapture_time_us={}\nschema_version={}\nshard_count={}\n", format_key, format_value,
            bound_file_version_1, f.capture_time_us, f.schema_version.uuid(), f.shards.size());
    for (size_t shard = 0; shard < f.shards.size(); ++shard) {
        const auto& b = f.shards[shard];
        if (b.shard != shard) {
            refuse(fmt::format("pitr bound file: row {} is shard {}, so a shard would be missing or recorded twice", shard, b.shard));
        }
        fmt::format_to(std::back_inserter(out), "shard={}\nbound_segment_id={}\nbound_position={}\n", b.shard, b.bound.id, b.bound.pos);
        if (!b.chain) {
            fmt::format_to(std::back_inserter(out), "chain={}\n", none_value);
            continue;
        }
        const auto& c = *b.chain;
        check_time(c.boot_epoch_us, "chain boot epoch");
        check_time(c.chain_start_us, "chain start");
        check_id(c.host_id, "host id");
        check_id(c.chain.uuid(), "chain id");
        check_floor(c.floor_segment_id);
        fmt::format_to(std::back_inserter(out), "chain=present\nchain_host_id={}\nchain_boot_epoch_us={}\nchain_start_us={}\nchain_id={}\n", c.host_id,
                c.boot_epoch_us, c.chain_start_us, c.chain.uuid());
        if (c.floor_segment_id) {
            fmt::format_to(std::back_inserter(out), "chain_floor_segment_id={}\n", *c.floor_segment_id);
        } else {
            fmt::format_to(std::back_inserter(out), "chain_floor_segment_id={}\n", none_value);
        }
    }
    fmt::format_to(std::back_inserter(out), "{}={:08x}\n", crc_key, checksum(std::string_view(out.data(), out.size())));
    // The static_assert in the header makes this unreachable for any shard
    // count a replay position can name; it stays because the cap is what every
    // reader relies on.
    if (out.size() > max_bound_file_size) {
        refuse(fmt::format("pitr bound file: {} shards need {} bytes, over the {}-byte maximum", f.shards.size(), out.size(), max_bound_file_size));
    }
    return sstring(out.data(), out.size());
}

bound_file decode_bound_file(std::string_view in) {
    // 1. The size, before a byte is read.
    if (in.size() > max_bound_file_size) {
        refuse(fmt::format("pitr bound file: {} bytes is larger than the {}-byte maximum", in.size(), max_bound_file_size));
    }
    if (in.empty() || in.back() != '\n') {
        refuse(fmt::format("pitr bound file: {} bytes, the last of which is not a newline", in.size()));
    }
    // The checksum line is the last one, so the body is everything up to the
    // newline that ends the line before it. Nothing may follow the checksum
    // line, which holds by construction: anything that does becomes the line
    // read as the checksum, and is refused as the wrong key.
    auto body_end = in.rfind('\n', in.size() - 2);
    if (body_end == std::string_view::npos) {
        refuse("pitr bound file: holds one line, so it has no body and no checksum");
    }
    auto body = in.substr(0, body_end + 1);
    auto crc_line = in.substr(body_end + 1);

    line_cursor cursor(body);
    // 2. What the file is, so one that is not a bound file at all is refused
    //    before anything in it is believed.
    if (auto f = cursor.value_of(format_key); f != format_value) {
        refuse(fmt::format("pitr bound file: format is '{}', not '{}'", f, format_value));
    }
    // 3. The checksum, before the version, so damage is never reported as a
    //    format this node cannot read.
    auto stored = line_cursor(crc_line).value_of(crc_key);
    auto computed = fmt::format("{:08x}", checksum(body));
    if (stored != computed) {
        refuse(fmt::format("pitr bound file: checksum is '{}', computed '{}'", stored, computed));
    }
    // 4. The version. A higher one is intact but unreadable here, so the remedy
    //    is an upgrade and not another backup. Version 0 was never written, so
    //    it is damage.
    auto version = parse_decimal<uint32_t>(cursor.value_of("version"), "version");
    if (version == 0) {
        refuse("pitr bound file: version 0 was never written by any Scylla");
    }
    if (version != bound_file_version_1) {
        throw bound_file_unknown_version(
                fmt::format("pitr bound file: version {} was written by a newer Scylla; this node reads version {}", version, bound_file_version_1));
    }

    bound_file f;
    f.capture_time_us = parse_decimal<int64_t>(cursor.value_of("capture_time_us"), "capture time");
    f.schema_version = table_schema_version(parse_uuid(cursor.value_of("schema_version"), "schema version"));
    auto shard_count = parse_decimal<uint32_t>(cursor.value_of("shard_count"), "shard count");
    if (shard_count == 0) {
        refuse("pitr bound file: a snapshot always covers at least one shard");
    }
    if (shard_count > max_shards_in_a_bound_file) {
        refuse(fmt::format("pitr bound file: {} shards is more than the {} a replay position can name", shard_count, max_shards_in_a_bound_file));
    }

    f.shards.reserve(shard_count);
    for (uint32_t shard = 0; shard < shard_count; ++shard) {
        shard_bound b;
        b.shard = parse_decimal<uint32_t>(cursor.value_of("shard"), "shard");
        if (b.shard != shard) {
            refuse(fmt::format("pitr bound file: row {} is shard {}, so a shard is missing, recorded twice or out of order", shard, b.shard));
        }
        b.bound.id = parse_decimal<uint64_t>(cursor.value_of("bound_segment_id"), "bound segment id");
        b.bound.pos = parse_decimal<uint32_t>(cursor.value_of("bound_position"), "bound position");
        auto chain = cursor.value_of("chain");
        if (chain == "present") {
            shard_chain c;
            c.host_id = parse_uuid(cursor.value_of("chain_host_id"), "host id");
            c.boot_epoch_us = parse_decimal<int64_t>(cursor.value_of("chain_boot_epoch_us"), "chain boot epoch");
            c.chain_start_us = parse_decimal<int64_t>(cursor.value_of("chain_start_us"), "chain start");
            c.chain = chain_id(parse_uuid(cursor.value_of("chain_id"), "chain id"));
            if (auto floor = cursor.value_of("chain_floor_segment_id"); floor != none_value) {
                c.floor_segment_id = parse_decimal<uint64_t>(floor, "chain floor segment id");
                check_floor(c.floor_segment_id);
            }
            b.chain = std::move(c);
        } else if (chain != none_value) {
            refuse(fmt::format("pitr bound file: chain is '{}', not 'present' or '{}'", chain, none_value));
        }
        f.shards.push_back(std::move(b));
    }
    // 5. Nothing may stand between the last row and the checksum.
    if (!cursor.at_end()) {
        refuse(fmt::format("pitr bound file: lines follow the last shard row, which holds {} rows", shard_count));
    }
    return f;
}

} // namespace db::pitr
