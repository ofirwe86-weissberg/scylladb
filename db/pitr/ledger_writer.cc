/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/ledger_writer.hh"

#include <algorithm>
#include <chrono>
#include <exception>
#include <utility>

#include <seastar/core/abort_source.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "bytes.hh"
#include "sstables/object_storage_client.hh"
#include "utils/chain_abort_source.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/log.hh"

namespace db::pitr {

static logging::logger writer_log("pitr_ledger_writer");

// Puts a whole ledger object in one request, bounded by
// `max_ledger_object_size`, and leaves nothing behind if it fails.
static future<> put_whole_object(sstables::object_storage_client& client, sstables::object_name name, bytes_view body, abort_source& caller_as) {
    caller_as.check();
    // The sink gets an abort source of its own, chained from the caller's, so
    // that a failed put can abort it: a sink that finalizes on close() (GCS)
    // must not be left to finalize what it was handed.
    abort_source as;
    const auto sub = utils::chain_abort_source(as, caller_as);
    const auto name_str = name.str();
    // No attributes: the entry describes itself.
    auto sink = client.make_upload_sink(std::move(name), sstables::object_storage_attributes{}, &as);
    std::exception_ptr ex;
    try {
        co_await sink.put(temporary_buffer<char>(reinterpret_cast<const char*>(body.data()), body.size()));
        co_await sink.flush();
    } catch (...) {
        ex = std::current_exception();
        as.request_abort();
    }
    try {
        co_await sink.close();
    } catch (...) {
        if (ex) {
            // Closing a sink we have just aborted is expected to report that
            // abort, so this is noise on every failed write; the first error is
            // what the caller retries on.
            writer_log.debug("closing the aborted ledger sink for {} reported: {}", name_str, std::current_exception());
        } else {
            // On GCS the finalizing request, and anything that failed in the
            // background, surface only from close().
            ex = std::current_exception();
        }
    }
    if (ex) {
        // An abort landing inside a request reaches us as whatever the store
        // made of the cancelled request; the retry loop recognises the abort
        // from `as` itself, so this error is reported as it came.
        std::rethrow_exception(std::move(ex));
    }
}

// `bucket`, `key_prefix` and `entry` are taken by value: every one of them is
// read after a suspension, so a caller may hand over a temporary.
future<> write_entry(
        sstables::object_storage_client& client, sstring bucket, sstring key_prefix, segment_entry entry, ledger_retry_policy policy, abort_source& as) {
    as.check();
    // Both throw `ledger_format_error` on an entry or a prefix no writer may
    // produce, which no retry would clear, and both are constant-size work.
    const auto body = encode(entry);
    const chain_key_parts chain{
            std::move(key_prefix),
            entry.host_id,
            entry.shard,
            entry.boot_epoch_us,
            entry.chain_start_us,
            entry.chain,
    };
    const auto key = entry_object_key(chain, entry.sequence);
    // Both ends are raised to a millisecond: a store that fails without a round
    // trip would otherwise be retried in a spin, and the bound has to leave
    // room for the base it bounds.
    const auto max_backoff = std::max(policy.max_backoff, std::chrono::milliseconds(1));
    auto backoff = exponential_backoff_retry(std::clamp(policy.base_backoff, std::chrono::milliseconds(1), max_backoff), max_backoff);
    // One line per failing entry per interval: an outage fails every entry on
    // the shard, over and over.
    logging::logger::rate_limit log_rate_limit(std::chrono::seconds(10));
    for (unsigned attempt = 1;; ++attempt) {
        std::exception_ptr ex;
        try {
            co_await put_whole_object(client, sstables::object_name(bucket, key), body, as);
            // An abort that landed late enough for the put to finish still
            // wins: only a success reported from here may be recorded, and the
            // object it left is the same bytes a retry would write.
            if (!as.abort_requested()) {
                co_return;
            }
        } catch (...) {
            ex = std::current_exception();
        }
        if (as.abort_requested()) {
            std::rethrow_exception(as.abort_requested_exception_ptr());
        }
        const auto wait = backoff.sleep_time();
        writer_log.log(log_level::warn, log_rate_limit, "attempt {} to write ledger entry {} failed, retrying in {}: {}", attempt, key, wait, ex);
        if (policy.on_attempt_failed) {
            policy.on_attempt_failed(ex, wait);
        }
        // sleep_abortable() reports the caller's own exception, or
        // sleep_aborted, which is an abort_requested_exception too.
        co_await backoff.retry(as);
    }
}

} // namespace db::pitr
