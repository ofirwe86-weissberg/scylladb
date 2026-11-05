/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <stdexcept>

#include <seastar/core/sstring.hh>

#include "bytes.hh"
#include "seastarx.hh"
#include "utils/UUID.hh"

namespace db::pitr {

// A chain is one unbroken run of shipped segments from one shard of one boot.
// A restart or an outage ends a chain and the next one gets a fresh id, so an
// id never spans a break in coverage.
using chain_id = utils::tagged_uuid<struct chain_id_tag>;

// The ledger is a persisted format, read by a later version on restore, so a
// reader that does not understand an entry must say so rather than guess.
class ledger_format_error : public std::runtime_error {
public:
    explicit ledger_format_error(const sstring& what)
        : std::runtime_error(what.c_str()) {
    }
};

// Separate from the above because restore reports it differently: the entry is
// intact but was written by a newer Scylla, so the remedy is an upgrade, not a
// new backup.
class ledger_unknown_version : public ledger_format_error {
public:
    explicit ledger_unknown_version(const sstring& what)
        : ledger_format_error(what) {
    }
};

// One immutable ledger object, written only after the segment's upload
// succeeded. Together with its neighbours it proves the chain has no gap:
// segment ids have gaps of their own, and a cut-off segment reads as a clean
// end, so names alone cannot prove coverage.
struct segment_entry {
    chain_id chain;
    // Dense within a chain, starting at 0; a missing number is a missing entry.
    uint64_t sequence = 0;
    // Microseconds since the UNIX epoch, UTC, at this node's boot. Orders the
    // chains of one host when their covered-through intervals are compared.
    int64_t boot_epoch_us = 0;
    // db::segment_id_type of the commitlog segment, never 0.
    uint64_t segment_id = 0;
    // The segment id of the previous entry in the chain, or 0 in the first
    // entry. Ids have gaps, so only this link shows what the chain contains.
    uint64_t prev_segment_id = 0;
    // Bytes uploaded, which is the segment's used size, never 0: a forced seal
    // of an unused segment produces no entry.
    uint64_t used_size = 0;
    // `utils::crc32` over the uploaded bytes. That is Castagnoli with an
    // initial value of 0 and no final XOR, so it is NOT standard CRC32C
    // (which starts at 0xffffffff and inverts the result). The same function
    // the commitlog uses for its own header and chunks.
    uint32_t segment_crc32 = 0;
    // Microseconds since the UNIX epoch, UTC, when the upload was recorded.
    int64_t upload_time_us = 0;
    // The segment's file name, without a directory. Restore needs it because
    // replay derives the segment's descriptor, including its base position,
    // from the name, and because it is the uploaded object's name. A name
    // with a path separator could move a fetch out of the prefix, so it is
    // refused.
    sstring file_name;

    bool operator==(const segment_entry&) const = default;
};

// Version 1 of the entry encoding. Fixed-width big-endian fields with a
// trailing `utils::crc32`, the same shape the commitlog's own segment header
// uses (`commitlog.cc`, `crc32_nbo`), so one order and one checksum function
// cover both formats and `xxd` reads the numbers left to right on a restore.
constexpr uint16_t ledger_format_version_1 = 1;

constexpr size_t max_file_name_size = 255;
// 86 bytes of fixed fields plus the name; both bounds are checked on decode so
// a damaged length can never make a reader allocate or read out of the buffer.
constexpr size_t min_entry_encoded_size = 86 + 1;
constexpr size_t max_entry_encoded_size = 86 + max_file_name_size;

// Throws `ledger_format_error` if the entry cannot be represented (an empty or
// oversized name, a name with '/' or a control character, segment id or used
// size 0). Constant-size work.
bytes encode(const segment_entry&);

// Refuses an unknown magic or version, an input outside the size bounds, a
// length that does not match the input exactly (truncated or with trailing
// bytes), a failed checksum, and a field a writer never emits. The entry's own
// checksum covers its bytes, so a changed entry is caught without reading the
// segment. Constant-size work.
segment_entry decode_segment_entry(bytes_view);

// Object keys. Entries live under `<prefix>/<host_id>/<shard>/`, one object
// each, named by sequence so a plain lexicographic listing comes out in chain
// order. Both numbers are zero padded to their full decimal width, so the
// order holds for every value a shard or a sequence can take.
constexpr size_t shard_key_digits = 10;    // uint32 max, 4294967295
constexpr size_t sequence_key_digits = 20; // uint64 max, 18446744073709551615
constexpr const char* entry_key_suffix = ".entry";

// `<prefix>/<host_id>/<shard>/`. The prefix must not be empty and must not end
// in '/'.
sstring shard_key_prefix(std::string_view prefix, const utils::UUID& host_id, uint32_t shard);

sstring entry_object_key(std::string_view prefix, const utils::UUID& host_id, uint32_t shard, uint64_t sequence);

struct entry_key_parts {
    sstring prefix;
    utils::UUID host_id;
    uint32_t shard = 0;
    uint64_t sequence = 0;

    bool operator==(const entry_key_parts&) const = default;
};

// Throws `ledger_format_error` on anything that `entry_object_key` would not
// have produced, so an unrelated object in the bucket is never read as an entry.
entry_key_parts parse_entry_object_key(std::string_view key);

} // namespace db::pitr
