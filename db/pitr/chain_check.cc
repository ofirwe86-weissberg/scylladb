/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/chain_check.hh"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <exception>
#include <optional>
#include <utility>

#include <fmt/chrono.h>
#include <seastar/core/abort_source.hh>
#include <seastar/core/format.hh>
#include <seastar/core/iostream.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "bytes.hh"
#include "sstables/object_storage_client.hh"
#include "utils/exceptions.hh"
#include "utils/log.hh"
#include "utils/s3/aws_error.hh"

namespace db::pitr {

static logging::logger check_log("pitr_chain_check");

// Both values are known non-negative here: an entry may not carry a negative
// coverage, and T is only rendered when it is above one.
static sstring iso_time(int64_t us) {
    return seastar::format("{:%Y-%m-%dT%H:%M:%S}Z", std::chrono::sys_time<std::chrono::microseconds>(std::chrono::microseconds(us)));
}

static std::unexpected<refusal> refuse_damaged(std::string_view key, sstring detail) {
    check_log.warn("ledger entry {} is not usable: {}", key, detail);
    return std::unexpected(refusal{
            .reason = refusal_reason::protection_records_damaged,
            .message = "The backup's protection records are damaged, so the interval they cover cannot be proved. Restore from a later backup.",
            .detail = seastar::format("{}: {}", key, detail),
    });
}

// Whether the read failed because the store does not hold the object, as S3
// (NoSuchKey) and the GCS client (ENOENT) report it. The GCS client maps a 3xx
// to ENOENT as well, which its own object_exists() also reads as absent.
static bool is_absent_object_error(std::exception_ptr ex) {
    if (const auto* e = try_catch<aws::aws_exception>(ex)) {
        return e->error().get_error_type() == aws::aws_error_type::NO_SUCH_KEY;
    }
    if (const auto* e = try_catch<storage_io_error>(ex)) {
        return e->code().value() == ENOENT;
    }
    return false;
}

// Reads a whole ledger object, or nothing when the store does not hold it. A
// read that says the object is absent is the end of the chain at that moment:
// asking again would race the shipper appending it (#131). Any other read
// error is told from an absent object by asking the store, so that a failing
// read never reads as the end of a chain.
static future<std::optional<bytes>> read_ledger_object(sstables::object_storage_client& client, const sstring& bucket, const sstring& key, abort_source& as) {
    std::exception_ptr read_error;
    bytes body;
    try {
        auto is = input_stream<char>(client.make_download_source(sstables::object_name(bucket, key), &as));
        std::exception_ptr ex;
        try {
            // One byte over the cap tells an oversized object from a read this
            // code truncated itself; either way the decode refuses it.
            while (body.size() <= max_ledger_object_size) {
                auto buf = co_await is.read();
                if (buf.empty()) {
                    break;
                }
                body.append(reinterpret_cast<const int8_t*>(buf.get()), buf.size());
            }
        } catch (...) {
            ex = std::current_exception();
        }
        co_await is.close();
        if (ex) {
            std::rethrow_exception(std::move(ex));
        }
    } catch (...) {
        read_error = std::current_exception();
    }
    if (read_error) {
        if (is_absent_object_error(read_error)) {
            co_return std::nullopt;
        }
        if (co_await client.object_exists(sstables::object_name(bucket, key), &as)) {
            std::rethrow_exception(std::move(read_error));
        }
        co_return std::nullopt;
    }
    co_return std::move(body);
}

// What in the entry's body does not match the key it was read from, which is
// also the bound file's record: the key is built from the bound, so one compare
// covers both. Nothing checksums a key, so this is what stops a chain moved or
// copied under another identity from being believed, and what keeps one list to
// one boot.
static std::optional<sstring> body_against_key(const segment_entry& entry, const chain_key_parts& chain, uint64_t sequence) {
    if (entry.sequence != sequence) {
        return seastar::format("the body's sequence is {}, and the key's is {}", entry.sequence, sequence);
    }
    if (entry.chain != chain.chain) {
        return seastar::format("the body's chain is {}, and the key's is {}", entry.chain, chain.chain);
    }
    if (entry.host_id != chain.host_id) {
        return seastar::format("the body's host is {}, and the key's is {}", entry.host_id, chain.host_id);
    }
    if (entry.shard != chain.shard) {
        return seastar::format("the body's shard is {}, and the key's is {}", entry.shard, chain.shard);
    }
    if (entry.boot_epoch_us != chain.boot_epoch_us) {
        return seastar::format("the body's boot epoch is {}, and the key's is {}", entry.boot_epoch_us, chain.boot_epoch_us);
    }
    if (entry.chain_start_us != chain.chain_start_us) {
        return seastar::format("the body's chain start is {}, and the key's is {}", entry.chain_start_us, chain.chain_start_us);
    }
    return std::nullopt;
}

// A broken link, which a gap in the middle of a shipped run would show as. The
// reader needs the run unbroken: an acknowledged write's tail may sit in the
// segment after the one it started in, and a missing segment between two listed
// ones would leave that write counted as complete.
static std::optional<sstring> link_against_previous(const segment_entry& entry, uint64_t sequence, uint64_t prev_segment_id) {
    if (sequence == 0) {
        if (entry.prev_segment_id != 0) {
            return seastar::format("the chain's first entry names a previous segment, {}", entry.prev_segment_id);
        }
        return std::nullopt;
    }
    if (entry.prev_segment_id != prev_segment_id) {
        return seastar::format("the entry follows segment {}, and the entry before it holds segment {}", entry.prev_segment_id, prev_segment_id);
    }
    if (entry.segment_id <= prev_segment_id) {
        return seastar::format("the entry's segment {} does not rise above the entry before it, {}", entry.segment_id, prev_segment_id);
    }
    return std::nullopt;
}

future<std::expected<checked_chain, refusal>> check_chain(
        sstables::object_storage_client& client, sstring bucket, sstring key_prefix, chain_bound bound, int64_t target_time_us, abort_source& as) {
    const chain_key_parts chain{
            std::move(key_prefix),
            bound.host_id,
            bound.shard,
            bound.boot_epoch_us,
            bound.chain_start_us,
            bound.chain,
    };
    checked_chain result;
    uint64_t prev_segment_id = 0;
    int64_t covered_through_us = 0;
    // The bound's own segment, and the lowest one the chain holds above it: a
    // chain that reaches past the bound without holding the bound's segment is
    // missing exactly the writes the backup's sstables do not have (#103).
    bool holds_bound_segment = false;
    std::optional<uint64_t> lowest_above_bound;
    for (uint64_t sequence = 0;; ++sequence) {
        as.check();
        const auto key = entry_object_key(chain, sequence);
        const auto body = co_await read_ledger_object(client, bucket, key, as);
        if (!body) {
            break;
        }
        segment_entry entry;
        try {
            entry = decode_segment_entry(bytes_view(body->data(), body->size()));
        } catch (const ledger_format_error& e) {
            co_return refuse_damaged(key, e.what());
        }
        if (const auto bad = body_against_key(entry, chain, sequence)) {
            co_return refuse_damaged(key, *bad);
        }
        if (const auto bad = link_against_previous(entry, sequence, prev_segment_id)) {
            co_return refuse_damaged(key, *bad);
        }
        if (sequence == 0 && bound.bound_segment_id < entry.segment_id) {
            // The chain's floor is the newest segment the shard held when
            // shipping took effect (D27). A bound below it was taken before
            // this shard shipped anything, so the commitlog that would carry
            // the backup forward is not in the store and never will be.
            check_log.warn("the backup's bound segment {} on shard {} is below the chain's first shipped segment {}", bound.bound_segment_id, bound.shard,
                    entry.segment_id);
            co_return std::unexpected(refusal{
                    .reason = refusal_reason::backup_before_shipping,
                    .message = "The backup was taken before this node began shipping, so the commitlog that would carry it forward was never shipped. "
                               "Restore from a backup taken after protection was on.",
                    .detail = seastar::format("shard {}: the backup's bound is segment {}, and the first segment shipped is {}", bound.shard,
                            bound.bound_segment_id, entry.segment_id),
            });
        }
        if (entry.segment_id == bound.bound_segment_id) {
            holds_bound_segment = true;
        } else if (entry.segment_id > bound.bound_segment_id && !lowest_above_bound) {
            // The entries rise in segment id, which link_against_previous()
            // has already proved, so the first one above the bound is the
            // lowest.
            lowest_above_bound = entry.segment_id;
        }
        prev_segment_id = entry.segment_id;
        covered_through_us = entry.covered_through_us;
        ++result.entries_read;
        // Every segment of the chain, T or no T: replay runs the list to its
        // end and the T filter drops the writes above T (P43).
        result.segments.push_back(shipped_segment{
                .path = segment_object_key(chain, entry.file_name),
                .used_size = entry.used_size,
        });
        result.segment_crc32s.push_back(entry.segment_crc32);
        co_await coroutine::maybe_yield();
    }
    if (result.entries_read == 0) {
        // The bound file named this chain, so the shard shipped under it; with
        // no entry at all, nothing says what it shipped (PM-50).
        check_log.warn("the chain at {} named by the backup holds no entries", chain_key_prefix(chain));
        co_return std::unexpected(refusal{
                .reason = refusal_reason::protection_records_missing,
                .message = "The backup names a chain of protection records that is not in the store, so what it covers cannot be proved. "
                           "Restore from a later backup.",
                .detail = seastar::format("shard {}: no entries under {}", bound.shard, chain_key_prefix(chain)),
        });
    }
    if (lowest_above_bound && !holds_bound_segment) {
        // The segment the snapshot was taken in holds the writes the backup's
        // sstables do not, and a chain that goes on above it without it has a
        // hole exactly there. Nothing else shows it: the sequence numbers stay
        // dense, and the ids come from `++_ids`, which skips reserve and
        // recycled segments, so only the bound's exact id proves it is there
        // (REF-4, #103). A bound segment that was empty and never shipped is
        // refused too, which is the accepted trade.
        check_log.warn("the chain at {} reaches segment {} but not the backup's bound segment {} on shard {}", chain_key_prefix(chain), *lowest_above_bound,
                bound.bound_segment_id, bound.shard);
        co_return std::unexpected(refusal{
                .reason = refusal_reason::protection_records_missing,
                .message = "The protection records do not hold the commitlog segment the backup was taken in, so the writes made in it cannot be replayed. "
                           "Restore from a later backup.",
                .detail = seastar::format("shard {}: segment {} is not in the chain under {}, which holds segment {} above it", bound.shard,
                        bound.bound_segment_id, chain_key_prefix(chain), *lowest_above_bound),
        });
    }
    result.covered_through_us = covered_through_us;
    if (target_time_us > covered_through_us) {
        // The interval's lower end is this chain's start; the restore's own
        // range check narrows it to the backup it would restore from.
        co_return std::unexpected(refusal{
                .reason = refusal_reason::t_past_coverage,
                .message = seastar::format("T is {}, and shipping from this node has only reached {}, a lag of {}. Retry with a T at or below that time.",
                        iso_time(target_time_us), iso_time(covered_through_us), std::chrono::microseconds(target_time_us - covered_through_us)),
                .detail = seastar::format("shard {}", bound.shard),
                .interval = restorable_interval{.from_us = bound.chain_start_us, .through_us = covered_through_us},
        });
    }
    check_log.debug("chain {} holds {} entries and {} segments, and covers through {}, at or above T {}", chain_key_prefix(chain), result.entries_read,
            result.segments.size(), covered_through_us, target_time_us);
    co_return std::move(result);
}

future<std::expected<checked_history, refusal>> check_bound_chains(
        sstables::object_storage_client& client, sstring bucket, sstring key_prefix, const bound_file& bound, int64_t target_time_us, abort_source& as) {
    checked_history history;
    // The shard covering least among those that lag, held while the rest are
    // checked: a shard that refuses for good outranks one a lower T would fix.
    std::optional<refusal> lagging;
    for (const auto& shard : bound.shards) {
        as.check();
        if (!shard.chain) {
            check_log.warn("the backup's bound records no chain for shard {}", shard.shard);
            co_return std::unexpected(refusal{
                    .reason = refusal_reason::protection_records_missing,
                    .message = "A shard of the node the backup was taken from shipped no protection records, so what it covers cannot be proved. Restore from "
                               "a later backup.",
                    .detail = seastar::format("shard {}: the backup's bound records no chain", shard.shard),
            });
        }
        auto checked = co_await check_chain(client, bucket, key_prefix,
                chain_bound{
                        .host_id = shard.chain->host_id,
                        .shard = shard.shard,
                        .boot_epoch_us = shard.chain->boot_epoch_us,
                        .chain_start_us = shard.chain->chain_start_us,
                        .chain = shard.chain->chain,
                        .bound_segment_id = shard.bound.id,
                },
                target_time_us, as);
        if (!checked) {
            if (checked.error().reason != refusal_reason::t_past_coverage) {
                co_return std::unexpected(std::move(checked.error()));
            }
            if (!lagging || checked.error().interval->through_us < lagging->interval->through_us) {
                lagging = std::move(checked.error());
            }
            continue;
        }
        history.covered_through_us = history.chains.empty() ? checked->covered_through_us : std::min(history.covered_through_us, checked->covered_through_us);
        history.chains.push_back(std::move(*checked));
    }
    if (lagging) {
        // Below the capture time this backup holds nothing to restore to, so
        // the interval the user is offered starts there and not at the chain.
        lagging->interval->from_us = std::max(lagging->interval->from_us, bound.capture_time_us);
        check_log.warn("T {} is past what the backup's chains cover: {}", target_time_us, *lagging);
        co_return std::unexpected(std::move(*lagging));
    }
    check_log.info(
            "the backup's {} chain(s) hold together and cover through {}, at or above T {}", history.chains.size(), history.covered_through_us, target_time_us);
    co_return std::move(history);
}

} // namespace db::pitr
