/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/segment_upload.hh"

#include <algorithm>
#include <chrono>
#include <exception>
#include <optional>

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "sstables/object_storage_client.hh"
#include "utils/chain_abort_source.hh"
#include "utils/crc.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/log.hh"

namespace db::pitr {

static logging::logger upload_log("pitr_segment_upload");

// Streams the used bytes of an open segment file into one sink.
//
// dma_read_bulk() may hand back more than it was asked for, up to the DMA
// alignment boundary, so every chunk is trimmed to what is left of the used
// size before it is checksummed or put: past that lies the recycled tail.
static future<> stream_used_bytes(file& f, data_sink& sink, uint64_t used_size, abort_source& as, upload_result& res, utils::crc32& crc) {
    uint64_t pos = 0;
    while (pos < used_size) {
        as.check();
        const auto rem = std::min<uint64_t>(used_size - pos, upload_chunk_size);
        auto buf = co_await f.dma_read_bulk<char>(pos, rem);
        if (buf.size() > rem) {
            buf.trim(rem);
        }
        if (buf.empty()) {
            // The size check in upload_to_sink() passed, so the file shrank since.
            co_await coroutine::return_exception(segment_too_short(fmt::format("segment ended at {} of {} used bytes", pos, used_size)));
        }
        const auto n = buf.size();
        crc.process(reinterpret_cast<const uint8_t*>(buf.get()), n);
        co_await sink.put(std::move(buf));
        pos += n;
        res.bytes_uploaded += n;
        co_await coroutine::maybe_yield();
    }
    co_await sink.flush();
}

static future<upload_result> upload_to_sink(
        sstables::object_storage_client& client, sstables::object_name name, file& f, uint64_t used_size, abort_source* caller_as) {
    const auto file_size = co_await f.size();
    if (file_size < used_size) {
        co_await coroutine::return_exception(
                segment_too_short(fmt::format("segment {} holds {} bytes, fewer than the {} used bytes reported for it", name, file_size, used_size)));
    }
    if (caller_as) {
        caller_as->check();
    }
    upload_result res;
    utils::crc32 crc;
    // The sink gets an abort source of its own, chained from the caller's, so
    // that a failure anywhere in the upload can abort it: a sink that finalizes
    // on close() (GCS) must not be left to finalize a partial object.
    abort_source as;
    const auto sub = utils::chain_abort_source(as, caller_as);
    const auto name_str = name.str();
    // No attributes: what describes a shipped segment is its ledger entry.
    auto sink = client.make_upload_sink(std::move(name), sstables::object_storage_attributes{}, &as);
    std::exception_ptr ex;
    try {
        co_await stream_used_bytes(f, sink, used_size, as, res, crc);
    } catch (...) {
        ex = std::current_exception();
        as.request_abort();
    }
    try {
        co_await sink.close();
    } catch (...) {
        if (ex) {
            // Closing a sink we have just aborted is expected to report that
            // abort, so this is noise on every failed upload; the first error is
            // what the caller retries on, and nesting the two would hide it.
            upload_log.debug("closing the aborted upload sink for {} reported: {}", name_str, std::current_exception());
        } else {
            // On GCS the final request, and anything that failed in the
            // background, surface only from close(), so a close error can be
            // the upload's only error - and it leaves by the same path.
            ex = std::current_exception();
        }
    }
    if (ex) {
        // An abort landing inside a request reaches us as whatever the store
        // made of the cancelled request, so report the caller's own abort
        // instead: a retry loop has to recognise it.
        if (caller_as && caller_as->abort_requested()) {
            std::rethrow_exception(caller_as->abort_requested_exception_ptr());
        }
        std::rethrow_exception(std::move(ex));
    }
    res.crc32 = crc.get();
    co_return res;
}

future<upload_result> upload_used_bytes(sstables::object_storage_client& client, sstables::object_name name, file f, uint64_t used_size, abort_source* as) {
    const auto name_str = name.str();
    std::optional<upload_result> res;
    std::exception_ptr ex;
    try {
        res = co_await upload_to_sink(client, std::move(name), f, used_size, as);
    } catch (...) {
        ex = std::current_exception();
    }
    // Closed by hand rather than with with_closeable(), which would nest a
    // close error into the upload's own and so hide its type from a retry loop.
    try {
        co_await f.close();
    } catch (...) {
        if (ex) {
            upload_log.debug("closing the segment file for {} reported: {}", name_str, std::current_exception());
        } else {
            ex = std::current_exception();
        }
    }
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
    co_return std::move(*res);
}

bool segment_is_shippable(uint64_t segment_id, uint64_t used_size, bool failed) {
    if (failed) {
        upload_log.warn("not shipping segment {}: its close failed, so the {} used bytes reported for it may not be on disk", segment_id, used_size);
        return false;
    }
    if (used_size == 0) {
        upload_log.debug("not shipping segment {}: it holds no used bytes", segment_id);
        return false;
    }
    return true;
}

future<upload_result> upload_with_retry(sstables::object_storage_client& client, sstables::object_name name, uint64_t used_size,
        segment_file_opener open_segment_file, retry_policy policy, abort_source& as) {
    as.check();
    const auto name_str = name.str();
    // Clamped so that a store failing without a round trip still costs one
    // sleep per attempt, instead of spinning the shard.
    auto backoff = exponential_backoff_retry(std::max(policy.base_backoff, std::chrono::milliseconds(1)), policy.max_backoff);
    for (unsigned attempt = 1;; ++attempt) {
        std::exception_ptr ex;
        try {
            // A fresh handle per attempt: a failed attempt's handle is closed,
            // and its read position is of no use to the next one.
            auto f = co_await open_segment_file();
            co_return co_await upload_used_bytes(client, name, std::move(f), used_size, &as);
        } catch (const segment_too_short&) {
            throw;
        } catch (...) {
            ex = std::current_exception();
        }
        if (as.abort_requested()) {
            std::rethrow_exception(as.abort_requested_exception_ptr());
        }
        const auto wait = backoff.sleep_time();
        upload_log.warn("attempt {} to upload segment {} failed, retrying in {}: {}", attempt, name_str, wait, ex);
        if (policy.on_backoff) {
            policy.on_backoff(wait);
        }
        try {
            co_await backoff.retry(as);
        } catch (...) {
            // An aborted sleep reports the abort its own way; the caller's
            // exception is what the rest of the shipper recognises.
            if (as.abort_requested()) {
                std::rethrow_exception(as.abort_requested_exception_ptr());
            }
            throw;
        }
    }
}

} // namespace db::pitr
