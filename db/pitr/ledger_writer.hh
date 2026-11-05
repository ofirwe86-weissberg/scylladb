/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <exception>
#include <functional>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "db/pitr/ledger_entry.hh"
#include "seastarx.hh"

namespace seastar {
class abort_source;
} // namespace seastar

namespace sstables {
class object_storage_client;
} // namespace sstables

namespace db::pitr {

// Waits between attempts: `base_backoff` before the second attempt, twice the
// last wait before each one after it, never more than `max_backoff`. Both are
// raised to a millisecond, so a store that fails instantly is not retried in a
// spin. The same shape as the upload's policy (#8); one type once both are in.
struct ledger_retry_policy {
    std::chrono::milliseconds base_backoff = std::chrono::milliseconds(100);
    std::chrono::milliseconds max_backoff = std::chrono::seconds(10);
    // Reports each failed attempt and the wait before the next one, so a caller
    // can show the error it is retrying (OUT-1). Nothing is reported for the
    // attempt that ends the call, and an exception out of it ends the call.
    std::function<void(std::exception_ptr, std::chrono::milliseconds)> on_attempt_failed;
};

// Writes one ledger entry to `<key_prefix>/<host>/<shard>/<chain>/<seq>.entry`
// in `bucket`, retrying a store failure behind `policy`'s backoff until it
// succeeds or `as` is aborted. Attempts are unbounded; giving up on a down
// store is watched elsewhere. Every argument is copied, so none has to outlive
// the call.
//
// The key comes from the entry's own body, so the identity a chain reader
// checks cannot disagree with it; only the prefix comes from the caller. The
// object is the whole of `encode(entry)`, a pure function of the entry, so
// rewriting an entry is byte-identical and harmless; it fits
// `max_ledger_object_size`, so one put and no large allocation.
//
// A failure aborts the sink before closing it, so a store that finalizes on
// close (GCS) cannot leave a truncated entry. An abort wins even over a put
// that had finished, and can land between this call and the caller's
// continuation: record a success without awaiting in between, or check `as`
// again. Catch an abort as `abort_requested_exception`, covering `sleep_aborted`.
//
// The caller guarantees what nothing here can check: an entry is written only
// after its segment's upload was reported a success, and a chain's entries are
// written in sequence order, one at a time. A write that gives up breaks the
// chain - it ends at the last entry written, and shipping continues under a new
// chain id. Giving up is `ledger_format_error` (a caller bug: an entry or
// prefix no writer may produce, thrown before any store call), the policy
// callback throwing, or an abort for a reason other than shutdown - a shutdown
// abort breaks nothing, as the next boot starts a new chain anyway.
future<> write_entry(sstables::object_storage_client& client, sstring bucket, sstring key_prefix, segment_entry entry, ledger_retry_policy policy,
        seastar::abort_source& as);

} // namespace db::pitr
