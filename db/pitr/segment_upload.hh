/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <stdexcept>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace seastar {
class abort_source;
class file;
} // namespace seastar

namespace sstables {
class object_name;
class object_storage_client;
} // namespace sstables

namespace db::pitr {

// A segment file that does not hold the used size reported for it. Shipping a
// prefix of it would leave an object the ledger describes wrongly, so the
// upload fails instead of uploading what there is.
class segment_too_short : public std::runtime_error {
public:
    explicit segment_too_short(const seastar::sstring& what)
        : std::runtime_error(what.c_str()) {
    }
};

struct upload_result {
    // Bytes the sink accepted, which on success equals the used size.
    uint64_t bytes_uploaded = 0;
    // CRC32C of those bytes, computed chunk by chunk as they are uploaded, so
    // it covers what was sent and not a second read of the file. The same
    // primitive the commitlog checksums its own entries with (utils::crc32).
    uint32_t crc32c = 0;
};

// Bytes read, checksummed and put per iteration. Small enough to stay off the
// large-allocation path, and a multiple of the 4 KiB DMA alignment so that only
// the last chunk of an unaligned used size is a partial read.
inline constexpr size_t upload_chunk_size = 64 * 1024;

// Streams the first `used_size` bytes of `f` to `name` in `client`, and returns
// how many bytes were stored and their checksum.
//
// Everything past `used_size` is a recycled tail holding writes from an earlier
// use of the file, so the upload stops there: a chunk that reaches past it is
// trimmed before it is checksummed or put. A file holding fewer than
// `used_size` bytes throws `segment_too_short` before anything is uploaded.
//
// Takes ownership of `f` and closes it, and the sink, on every path, including
// a failed read, put, flush or close and an abort. Any failure propagates to
// the caller; the object left in the store is then whatever the store does with
// an upload that never completed, so callers must not treat the name as
// written until this resolves successfully.
//
// `used_size` of 0 stores an empty object and returns a checksum of 0.
//
// Reads in `upload_chunk_size` chunks and yields between them, so the work is
// bounded by the chunk and not by the segment, which is 32 MiB or more.
seastar::future<upload_result> upload_used_bytes(
        sstables::object_storage_client& client, sstables::object_name name, seastar::file f, uint64_t used_size, seastar::abort_source* as = nullptr);

} // namespace db::pitr
