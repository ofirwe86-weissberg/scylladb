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
#include <seastar/core/semaphore.hh>
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
    // that clears it. Nothing is reported for the attempt that ends the call,
    // and an exception out of this callback ends it with that exception.
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
// reported as failed.
//
// The abort always wins: once `as` is aborted the call fails with the exception
// it carries, even if the attempt in flight had already finished uploading -
// which a rollback makes necessary, since the segment may have been erased and
// the handle may now be reading a newer segment's bytes. `name` may then hold a
// complete object that nothing records, which is as safe as any object an
// earlier attempt left. The in-flight attempt still unwinds, and cancelling an
// S3 upload can itself retry a request, so the call need not return instantly.
// The abort can land once more in the gap between this call returning and the
// caller's continuation, so the wiring must record the success without awaiting
// anything in between, or check `as` once more itself.
//
// Permanent, so propagated as themselves without a backoff: `segment_too_short`
// and a missing segment file (ENOENT) from the opener, which a rollback may
// have deleted. Every other failure is retried. A permanent failure leaves the
// segment held, so the wiring has to break the shipped chain and release it -
// and since a rollback both aborts and deletes, an abort is reported as the
// abort even when a permanent failure happened with it, so that path is only
// taken for a segment the caller is still shipping.
seastar::future<upload_result> upload_with_retry(sstables::object_storage_client& client, sstables::object_name name, uint64_t used_size,
        segment_file_opener open_segment_file, retry_policy policy, seastar::abort_source& as);

// Uploads in flight per shard, by default. Each one holds the store client's own
// upload buffers: about 8 MiB accumulated by the GCS sink, and on S3 a part of
// at least 5 MiB with more than one part in flight. The bound therefore keeps a
// shard's upload buffers in the tens of megabytes rather than growing with the
// segment count.
//
// Provisional: the piece that wires this up picks the real default and makes it
// a db::config option, so this value only serves callers that do not choose.
inline constexpr size_t default_max_concurrent_uploads = 8;

// Bounds how many uploads run at once on this shard.
//
// Without a bound, a store that is down leaves every sealed segment of the shard
// in upload_with_retry(), each reading its segment at full speed and holding its
// buffers: at a thousand segments that is hundreds of MiB/s of reads and
// gigabytes of buffers per shard. A slot therefore covers a whole
// upload_with_retry() call, the backoff sleeps included, since a segment waiting
// to be retried is holding its place just as much as one being read.
//
// Slots are granted in the semaphore's FIFO order, so calls begin in the order
// they asked. Nothing beyond that is ordered: calls run concurrently and may
// finish in any order, and the scheduler may reorder waiters woken together, so
// a caller that needs segments recorded in order has to order them itself.
//
// The limiter must outlive every call it started, the ones still waiting for a
// slot included, since a waiter holds a reference to the semaphore. The wiring
// therefore aborts the calls and awaits them all before destroying it.
class segment_upload_limiter {
    seastar::semaphore _slots;
    size_t _limit;

public:
    // A limit of 0 would stop every upload for ever, so it is raised to 1.
    explicit segment_upload_limiter(size_t max_concurrent_uploads = default_max_concurrent_uploads);

    size_t max_concurrent_uploads() const noexcept {
        return _limit;
    }
    // Slots free right now, so `max_concurrent_uploads()` minus the uploads in
    // flight, and the calls waiting for one of them to finish.
    size_t available_slots() const noexcept {
        return _slots.available_units();
    }
    size_t waiters() const noexcept {
        return _slots.waiters();
    }

    // Takes a slot and then runs upload_with_retry() with these arguments,
    // releasing the slot however the call ends - a success, a permanent failure,
    // an abort, or an exception from the caller's own callback.
    //
    // A call that is still waiting for a slot ends on an abort of `as` like one
    // already running: with the exception `as` carries, having opened no file
    // and put nothing, since the abort wins here too.
    seastar::future<upload_result> upload(sstables::object_storage_client& client, sstables::object_name name, uint64_t used_size,
            segment_file_opener open_segment_file, retry_policy policy, seastar::abort_source& as);
};

} // namespace db::pitr
