/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <cstdint>
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
// also read back the same bytes the commitlog wrote: the commitlog wraps its
// files on both write and read, so a plain open_file_dma() handle is right only
// where that wrapping is the identity. An encrypted commitlog is a stated limit
// of segment shipping and is not handled here.
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

// How long upload_with_retry() waits between attempts: `base_backoff` before
// the second attempt, and twice the last wait before each one after it, never
// more than `max_backoff`. A base below one millisecond is raised to one, so
// that a store failing instantly cannot turn the retry loop into a spin.
struct retry_policy {
    std::chrono::milliseconds base_backoff = std::chrono::milliseconds(100);
    std::chrono::milliseconds max_backoff = std::chrono::seconds(10);
    // Called with each wait before it is waited out, for observation only. The
    // tests read the sequence from here instead of waiting out a long one.
    std::function<void(std::chrono::milliseconds)> on_backoff;
};

// Opens the handle for one attempt, by the rules upload_used_bytes() states for
// `f`. Called once per attempt, and the handle it returns is never reused.
using segment_file_opener = std::function<seastar::future<seastar::file>()>;

// Whether a closed segment may be shipped at all, given
// commitlog::config::segment_closed's arguments. This is upload_with_retry()'s
// entry contract rather than a check it makes: a caller must drop a segment
// this rejects without creating any object for it. A failed close leaves a used
// size the commitlog does not stand behind, and a used size of 0 has nothing to
// ship but would still put an empty object in a restore's way.
bool segment_is_shippable(uint64_t segment_id, uint64_t used_size, bool failed);

// Uploads `used_size` bytes of a sealed segment to `name`, retrying a store or
// read failure after a backoff from `policy`, until it succeeds or `as` is
// aborted. Returns what upload_used_bytes() returned for the attempt that
// succeeded.
//
// Every attempt opens its own handle through `open_segment_file` and writes the
// same `name`, replacing whatever an earlier attempt left there, so a retry
// cannot pile up objects or leave a stale one behind. The name must keep the
// segment's own file name, which a restore matches against its prefix.
//
// The count of attempts is not bounded here: a store that is down for a long
// time is one condition, watched elsewhere, and it stops this loop by aborting
// `as`. An abort ends the call at once, with the exception `as` carries, from
// inside an attempt or from inside a backoff, and is never retried.
// `segment_too_short` is permanent - a segment shorter than the used size
// reported for it cannot grow - so it propagates as itself without a retry.
seastar::future<upload_result> upload_with_retry(sstables::object_storage_client& client, sstables::object_name name, uint64_t used_size,
        segment_file_opener open_segment_file, retry_policy policy, seastar::abort_source& as);

} // namespace db::pitr
