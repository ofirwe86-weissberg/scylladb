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
#include <seastar/core/format.hh>

#include "utils/crc.hh"

namespace db::pitr {

namespace {

constexpr std::string_view format_key = "format";
constexpr std::string_view format_value = "pitr-bound";
constexpr std::string_view crc_key = "crc32";

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

// The lines, each without its '\n'. A line that is not terminated, an empty
// line and anything after the last '\n' are refused, so the byte count a reader
// checksums is exactly the one the writer produced.
std::vector<std::string_view> split_lines(std::string_view in) {
    std::vector<std::string_view> lines;
    size_t at = 0;
    while (at < in.size()) {
        auto end = in.find('\n', at);
        if (end == std::string_view::npos) {
            refuse(fmt::format("pitr bound file: the last of {} bytes is not a newline", in.size()));
        }
        if (end == at) {
            refuse(fmt::format("pitr bound file: line {} is empty", lines.size() + 1));
        }
        lines.push_back(in.substr(at, end - at));
        at = end + 1;
    }
    return lines;
}

// Positional: the key has to be the one the format puts here, which is what
// refuses an unknown key, a duplicate, a missing one and a reordering alike.
std::string_view value_of(const std::vector<std::string_view>& lines, size_t at, std::string_view key) {
    if (at >= lines.size()) {
        refuse(fmt::format("pitr bound file: ends before the '{}' line", key));
    }
    auto line = lines[at];
    auto eq = line.find('=');
    if (eq == std::string_view::npos) {
        refuse(fmt::format("pitr bound file: line {} is not key=value: '{}'", at + 1, line));
    }
    if (line.substr(0, eq) != key) {
        refuse(fmt::format("pitr bound file: line {} is '{}', expected key '{}'", at + 1, line.substr(0, eq), key));
    }
    return line.substr(eq + 1);
}

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

    sstring out = seastar::format("{}={}\nversion={}\ncapture_time_us={}\nschema_version={}\nshard_count={}\n", format_key, format_value, bound_file_version_1,
            f.capture_time_us, f.schema_version.uuid(), f.shards.size());
    for (size_t shard = 0; shard < f.shards.size(); ++shard) {
        const auto& b = f.shards[shard];
        if (b.shard != shard) {
            refuse(fmt::format("pitr bound file: row {} is shard {}, so a shard would be missing or recorded twice", shard, b.shard));
        }
        out += seastar::format("shard={}\nbound_segment_id={}\nbound_position={}\n", b.shard, b.bound.id, b.bound.pos);
        if (!b.chain) {
            out += "chain=none\n";
            continue;
        }
        const auto& c = *b.chain;
        check_time(c.boot_epoch_us, "chain boot epoch");
        check_time(c.chain_start_us, "chain start");
        check_id(c.host_id, "host id");
        check_id(c.chain.uuid(), "chain id");
        out += seastar::format("chain=present\nchain_host_id={}\nchain_boot_epoch_us={}\nchain_start_us={}\nchain_id={}\nchain_floor_segment_id={}\n",
                c.host_id, c.boot_epoch_us, c.chain_start_us, c.chain.uuid(), c.floor_segment_id);
    }
    out += seastar::format("{}={:08x}\n", crc_key, checksum(out));
    // A node with more shards than the cap leaves room for would otherwise
    // write a file every reader refuses as oversize. Failing the snapshot says
    // so; see max_shards_in_a_bound_file.
    if (out.size() > max_bound_file_size) {
        refuse(fmt::format("pitr bound file: {} shards need {} bytes, over the {}-byte maximum", f.shards.size(), out.size(), max_bound_file_size));
    }
    return out;
}

bound_file decode_bound_file(std::string_view in) {
    // 1. The size, before a byte is read.
    if (in.size() > max_bound_file_size) {
        refuse(fmt::format("pitr bound file: {} bytes is larger than the {}-byte maximum", in.size(), max_bound_file_size));
    }
    auto lines = split_lines(in);
    // 2. What the file is, so one that is not a bound file at all is refused
    //    before anything in it is believed.
    if (auto f = value_of(lines, 0, format_key); f != format_value) {
        refuse(fmt::format("pitr bound file: format is '{}', not '{}'", f, format_value));
    }
    // 3. The checksum, over every byte before the last line, and before the
    //    version, so damage is never reported as a format this node cannot read.
    auto crc_line = lines.back();
    auto body_size = in.size() - (crc_line.size() + 1);
    auto stored = value_of(lines, lines.size() - 1, crc_key);
    auto computed = fmt::format("{:08x}", checksum(in.substr(0, body_size)));
    if (stored != computed) {
        refuse(fmt::format("pitr bound file: checksum is '{}', computed '{}'", stored, computed));
    }
    // 4. The version. A newer one is intact but unreadable here, so the remedy
    //    is an upgrade and not another backup.
    auto version = parse_decimal<uint32_t>(value_of(lines, 1, "version"), "version");
    if (version != bound_file_version_1) {
        throw bound_file_unknown_version(
                fmt::format("pitr bound file: version {} was written by a newer Scylla; this node reads version {}", version, bound_file_version_1));
    }

    bound_file f;
    f.capture_time_us = parse_decimal<int64_t>(value_of(lines, 2, "capture_time_us"), "capture time");
    f.schema_version = table_schema_version(parse_uuid(value_of(lines, 3, "schema_version"), "schema version"));
    auto shard_count = parse_decimal<uint32_t>(value_of(lines, 4, "shard_count"), "shard count");
    if (shard_count == 0) {
        refuse("pitr bound file: a snapshot always covers at least one shard");
    }

    // Nothing is reserved from the declared count: a count far above what the
    // file holds must cost a reader nothing before the rows refute it.
    size_t at = 5;
    for (uint32_t shard = 0; shard < shard_count; ++shard) {
        shard_bound b;
        b.shard = parse_decimal<uint32_t>(value_of(lines, at++, "shard"), "shard");
        if (b.shard != shard) {
            refuse(fmt::format("pitr bound file: row {} is shard {}, so a shard is missing, recorded twice or out of order", shard, b.shard));
        }
        b.bound.id = parse_decimal<uint64_t>(value_of(lines, at++, "bound_segment_id"), "bound segment id");
        b.bound.pos = parse_decimal<uint32_t>(value_of(lines, at++, "bound_position"), "bound position");
        auto chain = value_of(lines, at++, "chain");
        if (chain == "present") {
            shard_chain c;
            c.host_id = parse_uuid(value_of(lines, at++, "chain_host_id"), "host id");
            c.boot_epoch_us = parse_decimal<int64_t>(value_of(lines, at++, "chain_boot_epoch_us"), "chain boot epoch");
            c.chain_start_us = parse_decimal<int64_t>(value_of(lines, at++, "chain_start_us"), "chain start");
            c.chain = chain_id(parse_uuid(value_of(lines, at++, "chain_id"), "chain id"));
            c.floor_segment_id = parse_decimal<uint64_t>(value_of(lines, at++, "chain_floor_segment_id"), "chain floor segment id");
            b.chain = std::move(c);
        } else if (chain != "none") {
            refuse(fmt::format("pitr bound file: chain is '{}', not 'present' or 'none'", chain));
        }
        f.shards.push_back(std::move(b));
    }
    // 5. Nothing may follow the rows but the checksum line.
    if (at != lines.size() - 1) {
        refuse(fmt::format("pitr bound file: {} lines follow the last shard row and the checksum", lines.size() - 1 - at));
    }
    return f;
}

} // namespace db::pitr
