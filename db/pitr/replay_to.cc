/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/pitr/replay_to.hh"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <exception>
#include <utility>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/format.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/file.hh>

#include "utils/log.hh"

namespace db::pitr {

static logging::logger replay_log("pitr_replay_to");

static constexpr std::string_view replay_state_format = "pitr-replay";
static constexpr uint32_t replay_state_version_1 = 1;

static std::string_view phase_name(replay_phase phase) {
    switch (phase) {
    case replay_phase::started:
        return "started";
    case replay_phase::done:
        return "done";
    }
    return "unknown";
}

sstring encode(const replay_state& state) {
    for (const auto& value : {std::string_view(state.snapshot), std::string_view(state.t)}) {
        if (value.find('\n') != std::string_view::npos) {
            throw std::runtime_error("pitr replay mark: a value may not hold a newline");
        }
    }
    if (state.started_at_us < 0) {
        throw std::runtime_error("pitr replay mark: started_at_us may not be negative");
    }
    return seastar::format("format={}\nversion={}\nphase={}\nsnapshot={}\nt={}\nstarted_at_us={}\n", replay_state_format, replay_state_version_1,
            phase_name(state.phase), state.snapshot, state.t, state.started_at_us);
}

// The value of `key`, or an error naming what was there instead: a reader built
// today diagnoses damage the same way whatever wrote the file.
static std::string_view take_line(std::string_view& text, std::string_view key) {
    auto end = text.find('\n');
    if (end == std::string_view::npos) {
        throw std::runtime_error(seastar::format("pitr replay mark: no line for {}", key));
    }
    auto line = text.substr(0, end);
    text.remove_prefix(end + 1);
    if (!line.starts_with(key) || line.size() <= key.size() || line[key.size()] != '=') {
        throw std::runtime_error(seastar::format("pitr replay mark: expected {}=, got '{}'", key, line));
    }
    return line.substr(key.size() + 1);
}

replay_state decode_replay_state(std::string_view text) {
    if (take_line(text, "format") != replay_state_format) {
        throw std::runtime_error("pitr replay mark: not a replay mark");
    }
    auto version = take_line(text, "version");
    if (version != "1") {
        throw std::runtime_error(seastar::format("pitr replay mark: unknown version '{}'", version));
    }
    replay_state state;
    auto phase = take_line(text, "phase");
    if (phase == phase_name(replay_phase::started)) {
        state.phase = replay_phase::started;
    } else if (phase == phase_name(replay_phase::done)) {
        state.phase = replay_phase::done;
    } else {
        throw std::runtime_error(seastar::format("pitr replay mark: unknown phase '{}'", phase));
    }
    state.snapshot = sstring(take_line(text, "snapshot"));
    state.t = sstring(take_line(text, "t"));
    auto started = take_line(text, "started_at_us");
    auto [end, ec] = std::from_chars(started.data(), started.data() + started.size(), state.started_at_us);
    if (ec != std::errc() || end != started.data() + started.size() || state.started_at_us < 0) {
        throw std::runtime_error(seastar::format("pitr replay mark: bad started_at_us '{}'", started));
    }
    if (!text.empty()) {
        throw std::runtime_error("pitr replay mark: trailing bytes");
    }
    return state;
}

std::filesystem::path replay_state_path(const std::filesystem::path& state_dir, ::table_id target) {
    return state_dir / fmt::format("{}.replay", target.uuid());
}

future<> write_replay_state(const std::filesystem::path& state_dir, ::table_id target, const replay_state& state) {
    auto body = encode(state);
    auto path = replay_state_path(state_dir, target);
    auto tmp = path;
    tmp += ".tmp";
    co_await recursive_touch_directory(state_dir.native());
    // Flushed and fsynced under the temporary name, renamed over the final name
    // and the directory fsynced after it, so a crash leaves either no mark or a
    // whole one; a half-written temporary file is overwritten by the next run.
    auto f = co_await open_file_dma(tmp.native(), open_flags::wo | open_flags::create | open_flags::truncate);
    auto out = co_await make_file_output_stream(std::move(f));
    std::exception_ptr ex;
    try {
        co_await out.write(body);
        co_await out.flush();
    } catch (...) {
        ex = std::current_exception();
    }
    co_await out.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
    co_await rename_file(tmp.native(), path.native());
    co_await sync_directory(state_dir.native());
    replay_log.info("wrote the {} mark of {} to {}", phase_name(state.phase), target, path.native());
}

future<std::optional<replay_state>> read_replay_state(const std::filesystem::path& state_dir, ::table_id target) {
    auto path = replay_state_path(state_dir, target);
    if (!co_await file_exists(path.native())) {
        co_return std::nullopt;
    }
    auto body = co_await seastar::util::read_entire_file_contiguous(path);
    co_return decode_replay_state(body);
}

// The identifier of the table a `CREATE TABLE` or `ALTER TABLE` line names,
// which a replay into another table is allowed to differ in.
static constexpr std::string_view table_name_placeholder = "<table>";

static std::string_view trim(std::string_view line) {
    while (!line.empty() && (line.front() == ' ' || line.front() == '\t')) {
        line.remove_prefix(1);
    }
    while (!line.empty() && (line.back() == ' ' || line.back() == '\t' || line.back() == '\r')) {
        line.remove_suffix(1);
    }
    return line;
}

// Replaces the identifier after `keyword`, which runs to the next space, with
// the placeholder. The statement's own words are kept, so a DROP of a column
// still differs from an ADD of it.
static std::optional<sstring> without_table_name(std::string_view line, std::string_view keyword) {
    if (!line.starts_with(keyword)) {
        return std::nullopt;
    }
    auto rest = line.substr(keyword.size());
    auto end = rest.find(' ');
    return seastar::format("{}{}{}", keyword, table_name_placeholder, end == std::string_view::npos ? std::string_view() : rest.substr(end));
}

future<sstring> normalize_schema_cql(std::string_view text) {
    sstring out;
    while (!text.empty()) {
        // The caller caps the text, but a table may carry thousands of columns
        // and the cap is a megabyte: one line's work is bounded, the file's is
        // not.
        co_await coroutine::maybe_yield();
        auto end = text.find('\n');
        auto line = trim(end == std::string_view::npos ? text : text.substr(0, end));
        text.remove_prefix(end == std::string_view::npos ? text.size() : end + 1);
        if (line.empty()) {
            continue;
        }
        // The `ID` option opens the WITH clause, so the next line's `AND` still
        // reads as a continuation of a `WITH` that now carries no option.
        if (auto id = line.find("WITH ID = "); id != std::string_view::npos) {
            line = line.substr(0, id + 4);
        }
        sstring kept;
        if (auto renamed = without_table_name(line, "CREATE TABLE ")) {
            kept = std::move(*renamed);
        } else if (auto renamed = without_table_name(line, "ALTER TABLE ")) {
            kept = std::move(*renamed);
        } else if (line.starts_with("AND compaction = {") || line.starts_with("compaction = {")) {
            continue;
        } else {
            kept = sstring(line);
        }
        out += kept;
        out += "\n";
    }
    co_return out;
}

future<std::optional<refusal>> check_schema_match(std::string_view target_cql, std::string_view backup_cql) {
    auto target = co_await normalize_schema_cql(target_cql);
    auto backup = co_await normalize_schema_cql(backup_cql);
    if (target == backup) {
        co_return std::nullopt;
    }
    replay_log.debug("the target's schema is\n{}\nand the backup's is\n{}", target, backup);
    // The line, so that an operator can find the difference in two statements
    // that are hundreds of lines long, without the message carrying either.
    auto mismatch = std::ranges::mismatch(target, backup);
    auto line = 1 + std::count(target.begin(), mismatch.in1, '\n');
    co_return refusal{
            .reason = refusal_reason::target_schema_differs,
            .message = "The target table's schema differs from the backup's in more than its name, its id and its compaction options. Create the target from "
                       "the backup's schema.cql, with compaction disabled.",
            .detail = seastar::format("the statements differ at line {} of the comparison", line),
    };
}

} // namespace db::pitr
