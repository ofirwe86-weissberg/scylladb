/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <stdexcept>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace seastar {
class abort_source;
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
// `f` must be a handle dedicated to this call, never the commitlog's own, since
// ownership passes here and it is closed on every path, as is the sink. It must
// read back the bytes the commitlog wrote, which the commitlog wraps on write
// and read, so an encrypted commitlog is a stated limit of segment shipping and
// is not handled here, and it must not be a checked file, whose reader turns a
// missing file or an I/O error into disk-error handling.
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

// Waits between attempts: `base_backoff` before the second attempt, twice the
// last wait before each one after it, and never more than `max_backoff`. Both
// are raised to one millisecond, since a store that fails instantly would
// otherwise be retried in a spin.
struct retry_policy {
    std::chrono::milliseconds base_backoff = std::chrono::milliseconds(100);
    std::chrono::milliseconds max_backoff = std::chrono::seconds(10);
    // Reports each failed attempt and the wait before the next one, so a caller
    // can show the error it is retrying; a normal return reports the success
    // that clears it. Nothing is reported for the attempt that ends the call.
    std::function<void(std::exception_ptr, std::chrono::milliseconds)> on_attempt_failed;
};

// Opens one attempt's handle, by the rules stated above for `f`.
using segment_file_opener = std::function<seastar::future<seastar::file>()>;

// Uploads `used_size` bytes of a sealed segment to `name`, retrying a failure
// after a backoff from `policy`, until it succeeds or `as` is aborted. A
// `used_size` of 0, or no opener, is a caller's error: a segment with no used
// bytes is not shipped at all.
//
// Every attempt opens its own handle and writes the same `name`, replacing
// whatever an earlier attempt left there, so a retry cannot pile up objects.
// The name must keep the segment's own file name, which a restore matches
// against its prefix.
//
// The count of attempts is not bounded here: a store that is down for a long
// time is one condition, watched elsewhere, which stops this loop by aborting
// `as`. The wiring therefore needs an abort source per segment rather than one
// per shard, and must not record a success for a segment whose close was
// reported as failed. After an abort no further attempt starts and the caller
// gets the exception `as` carries, but the attempt in flight still unwinds, and
// cancelling an S3 upload can itself retry a request, so the call need not
// return instantly.
//
// Permanent, so propagated as themselves without a backoff: `segment_too_short`
// and a missing segment file (ENOENT) from the opener, which a rollback may
// have deleted. Every other failure is retried.
seastar::future<upload_result> upload_with_retry(sstables::object_storage_client& client, sstables::object_name name, uint64_t used_size,
        segment_file_opener open_segment_file, retry_policy policy, seastar::abort_source& as);

} // namespace db::pitr
