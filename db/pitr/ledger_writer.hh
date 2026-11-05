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
#include <string_view>

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
// last wait before each one after it, and never more than `max_backoff`. Both
// are raised to one millisecond, since a store that fails instantly would
// otherwise be retried in a spin.
//
// The same shape as the segment upload's policy (#8), kept separate while the
// two pieces land on their own branches; they are one type once both are in.
struct ledger_retry_policy {
    std::chrono::milliseconds base_backoff = std::chrono::milliseconds(100);
    std::chrono::milliseconds max_backoff = std::chrono::seconds(10);
    // Reports each failed attempt and the wait before the next one, so a caller
    // can show the error it is retrying (OUT-1). Nothing is reported for the
    // attempt that ends the call, and an exception out of this callback ends it
    // with that exception.
    std::function<void(std::exception_ptr, std::chrono::milliseconds)> on_attempt_failed;
};

// Writes one ledger entry to `<key_prefix>/<host>/<shard>/<chain>/<seq>.entry`
// in `bucket`, retrying a store failure after a backoff from `policy` until it
// succeeds or `as` is aborted.
//
// The key is derived from the entry's own body, so the identity a chain reader
// checks - host id, shard, chain id, boot epoch, chain start and sequence - can
// never disagree between the two. Only the prefix comes from the caller.
//
// An entry is at most `max_ledger_object_size` bytes, so it goes in one put and
// never as a multipart upload. There is no loop over input and no large
// allocation, so the call cannot stall: what it waits for is the store.
//
// The object is the whole of `encode(entry)` and nothing else. `encode()` is a
// pure function of the entry, so writing the same entry again produces byte-
// identical content: a retry after the object landed but before the reply
// arrived rewrites the same bytes at the same key and is harmless, and so is a
// caller that repeats a write it is unsure of.
//
// On failure, including an abort, the sink is aborted before it is closed, so
// no store finalizes a partial object: the key then holds nothing, or the same
// bytes an earlier attempt left. A store whose close finalizes (GCS) therefore
// cannot turn a failed put into a truncated entry.
//
// The abort always wins: once `as` is aborted the call fails with the exception
// it carries, even if the put in flight had already finished, because a caller
// that is being shut down or rolled back must not record a write it did not
// see succeed. The object may then hold a complete entry that nothing records,
// which is harmless: a reader that meets it has the entry it would have had.
// The abort can land once more in the gap between this call returning and the
// caller's continuation, so the wiring must act on the success without awaiting
// anything in between, or check `as` once more itself.
//
// The count of attempts is not bounded here, as in the upload: giving up on a
// store that is down is one condition, watched elsewhere, which stops this loop
// by aborting `as`.
//
// Throws `ledger_format_error` without contacting the store, and without a
// retry, for an entry or a prefix that cannot be represented: that is a bug in
// the caller, not a condition that clears.
//
// What the caller must guarantee, because nothing here can check it:
//   - an entry is written only after its segment's upload was reported a
//     success, since the entry is what claims the segment is in the store;
//   - entries of a chain are written in sequence order, one at a time, so a
//     reader never meets a gap it would have to refuse;
//   - a write that gives up breaks the chain: the chain ends at the last entry
//     that was written, and shipping continues under a new chain id.
// `head` and the covered-through bookkeeping are not written here.
future<> write_entry(sstables::object_storage_client& client, std::string_view bucket, std::string_view key_prefix, const segment_entry& entry,
        ledger_retry_policy policy, seastar::abort_source& as);

} // namespace db::pitr
