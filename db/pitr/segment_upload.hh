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

// A segment file that does not hold the used size reported for it. A prefix of
// it would leave an object the ledger describes wrongly, so the upload fails.
class segment_too_short : public std::runtime_error {
public:
    explicit segment_too_short(const seastar::sstring& what)
        : std::runtime_error(what.c_str()) {
    }
};

struct upload_result {
    // Bytes the sink accepted, which on success equals the used size.
    uint64_t bytes_uploaded = 0;
    // utils::crc32 of those bytes: the Castagnoli polynomial with an initial
    // value of 0 and no final xor, so not the CRC32C the stores report.
    uint32_t crc32 = 0;
};

// Bytes read, checksummed and put per iteration: off the large-allocation path,
// and a multiple of the 4 KiB DMA alignment.
inline constexpr size_t upload_chunk_size = 64 * 1024;

// Streams the first `used_size` bytes of `f` to `name` in `client`, and returns
// how many bytes were stored and their checksum. 0 stores an empty object.
//
// Everything past `used_size` is a recycled tail from an earlier use of the
// file, so a chunk reaching past it is trimmed before it is checksummed or put.
// A file holding fewer than `used_size` bytes throws `segment_too_short`, as
// does one that shrinks while being read.
//
// `f` must be a handle dedicated to this call - a fresh open_file_dma() per
// attempt, never the commitlog's own - since ownership passes here, and it is
// closed on every path, as is the sink.
//
// On failure, including an abort, the sink is aborted before it is closed, so no
// store finalizes a partial object: `name` then holds nothing, or what an
// earlier attempt left, or - when the failure raced the request that completed
// the upload - a complete new object of exactly `used_size` bytes, never a
// partial one. Only a reported success may be recorded.
//
// Reads in `upload_chunk_size` chunks and yields between them, so the work is
// bounded by the chunk, not by the used size, which can be a full segment.
seastar::future<upload_result> upload_used_bytes(
        sstables::object_storage_client& client, sstables::object_name name, seastar::file f, uint64_t used_size, seastar::abort_source* as = nullptr);

} // namespace db::pitr
