/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/replay_reader.hh"

#include <algorithm>
#include <filesystem>
#include <stdexcept>
#include <variant>
#include <vector>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "mutation/frozen_mutation.hh"
#include "utils/lister.hh"

namespace db::pitr {

static logging::logger plogger("pitr");

namespace {

// One segment to read. Kept instead of commitlog::descriptor because that has
// const members and so cannot be sorted in place.
struct segment_file {
    segment_id_type id;
    sstring filename;
    ::detail::commitlog_entry_serialization_format format;
};

} // anonymous namespace

// The entry format is a property of the segment: a tagged file carries the
// variant envelope, an untagged one the bare mutation entry.
static ::detail::commitlog_entry_serialization_format entry_format(const commitlog::descriptor& d) {
    return d.descriptor_tag.empty() ? ::detail::commitlog_entry_serialization_format::mutation : ::detail::commitlog_entry_serialization_format::variant;
}

static future<std::vector<segment_file>> list_shard_segments(const shard_replay_params& params) {
    std::vector<segment_file> segments;
    co_await lister::scan_dir(std::filesystem::path(std::string(params.directory)), lister::dir_entry_types::of<directory_entry_type::regular>(),
            [&](std::filesystem::path parent, directory_entry de) -> future<> {
                if (!std::string_view(de.name).starts_with(std::string_view(params.filename_prefix))) {
                    return make_ready_future<>();
                }
                try {
                    const commitlog::descriptor d(de.name, params.filename_prefix);
                    if (replay_position(d).shard_id() == params.shard) {
                        segments.emplace_back(segment_file{d.id, sstring((parent / de.name).string()), entry_format(d)});
                    }
                } catch (std::domain_error& e) {
                    // The name carries the prefix but is not a segment. Worth a
                    // word, since the restore directory holds only what was
                    // fetched into it, but not a reason to fail.
                    plogger.info("ignoring {} in {}: {}", de.name, params.directory, e.what());
                }
                return make_ready_future<>();
            });
    std::ranges::sort(segments, std::less(), &segment_file::id);
    co_return segments;
}

static future<> consume_entry(const shard_replay_params& params, const replay_consumer& consume, replay_reader_stats& stats,
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
    co_await consume(replay_entry{.mutation = entry.mutation(), .mapping = entry.mapping(), .position = rp});
}

future<replay_reader_stats> replay_shard_segments(const shard_replay_params& params, replay_consumer consume) {
    const auto segments = co_await list_shard_segments(params);
    replay_reader_stats stats;
    // One state for the whole shard, so a write split between two of its
    // segments is reassembled and handed over once.
    const commitlog::replay_state state;

    plogger.info("strict replay of {} segment(s) for shard {} of table {} from {}, bound {}, truncation floor {}", segments.size(), params.shard,
            params.source_table, params.directory, params.bound, params.truncate_floor);

    for (const auto& seg : segments) {
        try {
            co_await commitlog::read_log_file(
                    state, seg.filename, params.filename_prefix,
                    [&](commitlog::buffer_and_replay_position buf_rp) {
                        return consume_entry(params, consume, stats, seg.format, std::move(buf_rp));
                    },
                    0, params.extensions);
        } catch (...) {
            // Normal replay counts damage and carries on. Restore cannot: the
            // entries behind it are the ones the user asked for.
            plogger.error("strict replay of {} failed after {} entries: {:t}", seg.filename, stats.entries_read, std::current_exception());
            throw;
        }
        ++stats.segments_read;
        co_await coroutine::maybe_yield();
    }

    co_return stats;
}

} // namespace db::pitr
