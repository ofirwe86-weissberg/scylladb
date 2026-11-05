/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/segment_upload.hh"

#include <algorithm>
#include <exception>
#include <optional>

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/closeable.hh>

#include "sstables/object_storage_client.hh"
#include "utils/chain_abort_source.hh"
#include "utils/crc.hh"
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
        if (!ex) {
            throw;
        }
        // Closing a sink we have just aborted is expected to report that abort,
        // so this is noise on every failed upload; the first error is what the
        // caller retries on, and nesting the two would hide its type.
        upload_log.debug("closing the aborted upload sink for {} reported: {}", name_str, std::current_exception());
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
    // `name` stays in this coroutine's frame, which outlives the lambda, since
    // with_closeable() requires a lambda that moves without throwing and
    // object_name's move constructor does not promise that.
    co_return co_await with_closeable(std::move(f), [&client, &name, used_size, as](file& f) {
        return upload_to_sink(client, std::move(name), f, used_size, as);
    });
}

} // namespace db::pitr
