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
//
// The rule a shipper must keep, and a restore relies on: every segment that
// held data is either an entry in the chain, or the chain ends there. There is
// no third case, which is why a missing sequence is a refusal and not a gap to
// be worked around.
struct segment_entry {
    chain_id chain;
    // Dense within a chain, starting at 0; a missing number is a missing entry.
    uint64_t sequence = 0;
    // Microseconds since the UNIX epoch, UTC, at this node's boot, never
    // negative. Orders the chains of one host, and names the chain's directory.
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

// Frozen for every kind and every version, because a reader of a later entry
// has to get this far before it knows what it is holding:
//
//   0   magic, 8 bytes
//   8   format version, big-endian uint16
//   10  entry kind, uint8
//   11  flags, uint8
//   12  the kind's own fields
//   last 4 bytes: `utils::crc32` over everything before them
//
// Every ledger object has it, including `head`, so any reader can tell what it
// is holding and whether it may read on, whoever wrote it.
constexpr size_t common_header_size = 12;

// Kinds. A kind number is never reused, and `head` has one now even though its
// fields arrive with the next piece, so no later kind can take the number.
constexpr uint8_t object_kind_segment_entry = 1;
constexpr uint8_t object_kind_head = 2;
// 3 and up are unassigned; events (TRUNCATE, schema) take the next numbers.

// Flags, split in half so a later writer can choose which readers it breaks:
//   bits 0-3  must-understand: an unknown bit here is refused, because it
//             changes how the object has to be read;
//   bits 4-7  advisory: an unknown bit here is ignored, so a hint this reader
//             does not know about does not cost it the object.
// No bit is defined in version 1.
constexpr uint8_t must_understand_flags_mask = 0x0f;
constexpr uint8_t advisory_flags_mask = 0xf0;

// Any kind, any version, and **never raised**: a reader has to bound its read
// before it knows what it is reading. A payload that could grow past this, such
// as a schema event's statement, goes in its own object, which the entry names
// by key and checksum.
constexpr size_t max_ledger_object_size = 4096;
constexpr size_t min_ledger_object_size = common_header_size + sizeof(uint32_t);

constexpr size_t max_file_name_size = 255;
// 82 bytes of fixed fields, the 12-byte common header among them, plus the name
// plus the 4-byte checksum.
// Both bounds are checked on decode, so a damaged length can never make a
// reader allocate or read out of the buffer.
constexpr size_t min_entry_encoded_size = 86 + 1;
constexpr size_t max_entry_encoded_size = 86 + max_file_name_size;
static_assert(max_entry_encoded_size < max_ledger_object_size);

// Throws `ledger_format_error` if the entry cannot be represented (an empty or
// oversized name, a name that is '.' or '..' or holds '/' or a character
// outside printable ASCII, a negative boot epoch, segment id or used size 0).
// Constant-size work.
bytes encode(const segment_entry&);

// The order of the checks is part of the format, because every later version
// must still be diagnosed the same way by a reader built today:
//
//   1. the magic, after the size that any object must have;
//   2. the checksum over the whole object, after the general maximum size;
//   3. the version, where a newer one throws `ledger_unknown_version`;
//   4. the kind, where an unknown number also throws `ledger_unknown_version`,
//      and a known kind that is not the one asked for throws
//      `ledger_format_error`;
//   5. the flags, where an unknown must-understand bit throws
//      `ledger_format_error` and an unknown advisory bit is ignored;
//   6. only then this kind's own size, bounds and field checks.
//
// Checking the checksum before the version is what keeps a damaged object from
// being reported as "written by a newer Scylla, upgrade": one flipped bit in
// the version field is damage, and only an intact object can claim a version.
//
// A ledger object is copied as bytes and never re-encoded, because re-encoding
// through this struct would drop an advisory flag bit the writer did not know;
// a `flags` field can join the struct later with no change to the format.
//
// The checksum is an accident check, not a defence against tampering: anyone
// who can change the bytes can recompute it. It also cannot be compared with
// the checksum an object store reports, which is standard CRC32C.
// Constant-size work.
segment_entry decode_segment_entry(bytes_view);

// Object keys.
//
//   entries:  <prefix>/<host_id>/<shard>/<chain>/<sequence>.entry
//   head:     <prefix>/<host_id>/<shard>/<chain>/head
//   segments: <prefix>/<host_id>/<shard>/<chain>/segments/<file_name>
//
// where <chain> is `<boot_epoch_us>-<chain_start_us>-<chain_uuid>`.
//
// Everything is under the chain, because sequences restart at 0 in a new chain
// and a restart or an outage starts one: a flat layout would have the new
// chain's entries overwrite the old chain's, and with them the old interval a
// restore is still entitled to (SHP-7). Segments sit in their own
// sub-directory, so a file name reused after a clock step cannot overwrite an
// older chain's segment and so they stay out of the entries' listing, which
// filters on the `.entry` suffix.
//
// The chain's name leads with the boot epoch and then the time the chain
// started, because an outage ends a chain and the next one starts in the same
// boot, so the boot epoch alone would leave those chains in UUID order. With
// both, a listing returns a host's chains in the order they ran.
//
// A listing's order is a convenience, never proof: both times come from the
// wall clock, which can step backwards, and two chains of one boot could then
// carry the same start. What proves a chain is the entries themselves, their
// sequence and their links.
//
// Every number is zero padded to its full decimal width, so the order holds for
// every value it can take.
constexpr size_t shard_key_digits = 10;       // uint32 max, 4294967295
constexpr size_t sequence_key_digits = 20;    // uint64 max, 18446744073709551615
constexpr size_t boot_epoch_key_digits = 20;  // int64 max is 19 digits
constexpr size_t chain_start_key_digits = 20; // the same
constexpr const char* entry_key_suffix = ".entry";
constexpr const char* head_key_name = "head";
constexpr const char* segments_key_component = "segments";

// The chain a restore walks: one directory holding its entries, its `head` and
// its segments. The prefix must not be empty, must not start or end in '/' and
// must hold no empty, '.' or '..' component; neither time may be negative, as
// it would not sort.
struct chain_key_parts {
    sstring prefix;
    utils::UUID host_id;
    uint32_t shard = 0;
    int64_t boot_epoch_us = 0;
    int64_t chain_start_us = 0;
    chain_id chain;

    bool operator==(const chain_key_parts&) const = default;
};

// `<prefix>/<host_id>/<shard>/`, the directory holding one shard's chains.
sstring shard_key_prefix(std::string_view prefix, const utils::UUID& host_id, uint32_t shard);

sstring chain_key_prefix(const chain_key_parts&);

sstring entry_object_key(const chain_key_parts&, uint64_t sequence);

// The one mutable object per chain: the last sequence and the covered-through
// time. Buckets with object lock are therefore unsupported.
sstring head_object_key(const chain_key_parts&);

sstring segment_object_key(const chain_key_parts&, std::string_view file_name);

struct entry_key_parts {
    chain_key_parts chain;
    uint64_t sequence = 0;

    bool operator==(const entry_key_parts&) const = default;
};

// All three throw `ledger_format_error` on anything the builders above would
// not have produced, so an unrelated object in the bucket is never read as part
// of the ledger. `parse_chain_key_prefix` takes the form with the trailing '/'.
chain_key_parts parse_chain_key_prefix(std::string_view key);
entry_key_parts parse_entry_object_key(std::string_view key);
chain_key_parts parse_head_object_key(std::string_view key);

} // namespace db::pitr
