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
// store is watched elsewhere. `bucket`, `key_prefix` and `entry` are copied;
// `client` and `as` are this shard's and must outlive the returned future.
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
// again. Which exception an abort ends the call with depends on where it lands
// (an aborted backoff sleep gives `sleep_aborted`), so recognise an abort by
// checking `as`, not by the exception's type. `on_attempt_failed` runs inline
// on this shard and must not block.
//
// The caller guarantees what nothing here can check: an entry is written only
// after its segment's upload was reported a success, and a chain's entries are
// written in sequence order, one at a time. A write that gives up breaks the
// chain - it ends at the last entry written, and the shipper stops shipping
// until the node is restarted, the next boot starting a new chain. Giving up is
// `ledger_format_error` (a caller bug: an entry or prefix no writer may
// produce, thrown before any store call), the policy callback throwing, or an
// abort for a reason other than shutdown. A shutdown abort needs no chain break
// here: the restart is itself the break, recorded by the next boot, which starts
// a new chain.
future<> write_entry(sstables::object_storage_client& client, sstring bucket, sstring key_prefix, segment_entry entry, ledger_retry_policy policy,
        seastar::abort_source& as);

} // namespace db::pitr
