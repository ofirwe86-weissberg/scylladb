/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/replay_reader.hh"

#include <filesystem>
#include <stdexcept>
#include <variant>

#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/commitlog/commitlog_extensions.hh"
#include "db/extensions.hh"
#include "mutation/frozen_mutation.hh"
#include "utils/log.hh"

namespace db::pitr {

// Not "pitr": that name belongs to the shipper.
static logging::logger rlogger("pitr_replay");

namespace {

// The first bytes of a segment file: magic, version, id, alignment and a
// checksum, as commitlog.cc writes them. Read here because read_log_file treats
// a zeroed header, and a header naming another segment, as a clean end of log.
constexpr size_t segment_header_size = 6 * sizeof(uint32_t);
constexpr uint32_t segment_magic = ('S' << 24) | ('C' << 16) | ('L' << 8) | 'C';

[[noreturn]] void refuse(const shipped_segment& seg, sstring why) {
    throw invalid_replay_input(fmt::format("pitr: refusing shipped segment {}: {}", seg.path, why));
}

// The entry format is a property of the segment: a tagged file carries the
// variant envelope, an untagged one the bare mutation entry. Any other tag is
// one this build does not know how to read.
::detail::commitlog_entry_serialization_format entry_format(const shipped_segment& seg, const commitlog::descriptor& d) {
    if (d.descriptor_tag.empty()) {
        return ::detail::commitlog_entry_serialization_format::mutation;
    }
    if (d.descriptor_tag != ::detail::variant_format_tag) {
        refuse(seg, fmt::format("carries the unknown descriptor tag '{}'", d.descriptor_tag));
    }
    return ::detail::commitlog_entry_serialization_format::variant;
}

commitlog::descriptor parse_name(const shard_replay_params& params, const shipped_segment& seg) {
    try {
        return commitlog::descriptor(std::string(seg.path), params.filename_prefix);
    } catch (const std::domain_error& e) {
        refuse(seg, fmt::format("is not a segment of '{}': {}", params.filename_prefix, e.what()));
    }
}

// Fails unless the file is exactly the size the caller recorded and starts with
// this segment's own header.
future<> check_file(const shard_replay_params& params, const shipped_segment& seg, const commitlog::descriptor& d) {
    file f;
    try {
        f = co_await open_file_dma(seg.path, open_flags::ro);
    } catch (const std::exception& e) {
        refuse(seg, fmt::format("cannot be opened: {}", e.what()));
    }
    std::exception_ptr ep;
    try {
        const auto size = co_await f.size();
        if (size != seg.used_size) {
            refuse(seg, fmt::format("is {} bytes, not the {} bytes the source used", size, seg.used_size));
        }
        // The header of an encrypted segment is only readable through the same
        // extensions read_log_file will open the file with.
        auto hf = f;
        if (params.extensions) {
            for (auto* ext : params.extensions->commitlog_file_extensions()) {
                if (auto nf = co_await ext->wrap_file(seg.path, hf, open_flags::ro)) {
                    hf = std::move(nf);
                }
            }
        }
        const auto buf = co_await hf.dma_read<char>(0, segment_header_size);
        if (buf.size() < segment_header_size) {
            refuse(seg, fmt::format("is {} bytes, too short for a segment header", buf.size()));
        }
        const auto magic = seastar::read_be<uint32_t>(buf.get());
        const auto id = seastar::read_be<uint64_t>(buf.get() + 2 * sizeof(uint32_t));
        if (magic == 0 && id == 0) {
            refuse(seg, "has a zeroed header: it was preallocated and never written");
        }
        if (magic != segment_magic) {
            refuse(seg, fmt::format("does not start with a commitlog header: magic {:#x}", magic));
        }
        if (id != d.id) {
            refuse(seg, fmt::format("holds segment id {}, not the {} its name claims", id, d.id));
        }
    } catch (...) {
        ep = std::current_exception();
    }
    co_await f.close();
    if (ep) {
        std::rethrow_exception(ep);
    }
}

void check_position(const db::replay_position& rp, std::string_view what, unsigned shard) {
    if (rp != db::replay_position() && rp.shard_id() != shard) {
        throw invalid_replay_input(fmt::format("pitr: the {} {} is not of shard {}", what, rp, shard));
    }
}

future<> consume_entry(const shard_replay_params& params, const replay_consumer& consume, replay_reader_stats& stats,
        ::detail::commitlog_entry_serialization_format format, commitlog::buffer_and_replay_position buf_rp) {
    co_await coroutine::maybe_yield();

    const auto rp = buf_rp.position;
    ++stats.entries_read;

    // Both filters compare positions, and both run before the entry is decoded:
    // decoding is the expensive half, and a start offset handed to the reader
    // would select whole chunks rather than entries.
    if (rp < params.bound) {
        ++stats.dropped_below_bound;
        co_return;
    }
    if (rp <= params.truncate_floor) {
        ++stats.dropped_at_or_below_truncate_floor;
        co_return;
    }

    const commitlog_entry_reader reader(buf_rp.buffer, format);
    const auto& item = reader.entry().item;
    if (std::holds_alternative<raft_commitlog_batch>(item)) {
        // Raft rebuilds its own state from its own log; restore replays the
        // data of one table.
        ++stats.raft_batches_skipped;
        co_return;
    }

    const auto& entry = std::get<mutation_entry>(item);
    if (entry.mutation().column_family_id() != params.source_table) {
        ++stats.dropped_other_table;
        co_return;
    }

    ++stats.entries_replayed;
    // Decoding above does not yield, and this runs while the node serves.
    co_await coroutine::maybe_yield();
    co_await consume(replay_entry{.mutation = entry.mutation(), .mapping = entry.mapping(), .position = rp});
}

} // anonymous namespace

future<replay_reader_stats> replay_shard_segments(const shard_replay_params& params, replay_consumer consume) {
    check_position(params.bound, "bound", params.shard);
    check_position(params.truncate_floor, "truncation floor", params.shard);

    replay_reader_stats stats;
    // One state for the whole shard, so a write split between two of its
    // segments is reassembled and handed over once.
    const commitlog::replay_state state;
    std::optional<segment_id_type> previous_id;

    rlogger.info("strict replay of {} segment(s) for shard {} of table {}, bound {}, truncation floor {}", params.segments.size(), params.shard,
            params.source_table, params.bound, params.truncate_floor);

    for (const auto& seg : params.segments) {
        const auto d = parse_name(params, seg);
        const auto entry_fmt = entry_format(seg, d);
        if (replay_position(d).shard_id() != params.shard) {
            refuse(seg, fmt::format("belongs to shard {}, not {}", replay_position(d).shard_id(), params.shard));
        }
        if (previous_id && d.id <= *previous_id) {
            refuse(seg, fmt::format("has id {}, which does not follow {}: the list must be in ascending id order", d.id, *previous_id));
        }
        previous_id = d.id;
        co_await check_file(params, seg, d);

        try {
            co_await commitlog::read_log_file(
                    state, seg.path, params.filename_prefix,
                    [&](commitlog::buffer_and_replay_position buf_rp) {
                        return consume_entry(params, consume, stats, entry_fmt, std::move(buf_rp));
                    },
                    0, params.extensions);
        } catch (...) {
            // Normal replay counts damage and carries on. Restore cannot: the
            // entries behind it are the ones the user asked for.
            rlogger.error("strict replay of {} failed after {} entries: {:t}", seg.path, stats.entries_read, std::current_exception());
            throw;
        }
        ++stats.segments_read;
        co_await coroutine::maybe_yield();
    }

    co_return stats;
}

} // namespace db::pitr
