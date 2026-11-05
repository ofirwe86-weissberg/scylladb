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
    // CRC-32 over those bytes, computed chunk by chunk as they are uploaded.
    // utils::crc32, the primitive the commitlog checksums its own entries with:
    // the Castagnoli polynomial with an initial value of 0 and no final xor, so
    // it is not the CRC32C the object stores report (that one inverts both).
    uint32_t crc32 = 0;
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
// `used_size` bytes throws `segment_too_short`, as does one that shrinks while
// being read.
//
// `f` must be a handle dedicated to this call - a fresh open_file_dma() per
// attempt, never the commitlog's own handle - because ownership passes here and
// the file is closed on every path, as is the sink.
//
// Any failure propagates to the caller, and the sink's own abort source is
// aborted before it is closed, so no store finalizes a partial object: S3
// aborts the multipart upload it started, and GCS abandons its resumable
// session, which the service expires on its own. Either way nothing is
// readable at `name`, and an earlier object there is left as it was. A retry
// with the same name replaces whatever a successful attempt stored.
//
// `used_size` of 0 stores an empty object and returns a checksum of 0.
//
// Reads in `upload_chunk_size` chunks and yields between them, so the work is
// bounded by the chunk and not by the used size, which can be up to a full
// segment.
seastar::future<upload_result> upload_used_bytes(
        sstables::object_storage_client& client, sstables::object_name name, seastar::file f, uint64_t used_size, seastar::abort_source* as = nullptr);

} // namespace db::pitr
