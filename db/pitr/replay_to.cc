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

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/format.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/file.hh>

#include "cql3/description.hh"
#include "db/config.hh"
#include "marshal_exception.hh"
#include "db/pitr/t_parser.hh"
#include "replica/database.hh"
#include "replica/memtable.hh"
#include "replica/schema_describe_helper.hh"
#include "schema/schema.hh"
#include "sstables/object_storage_client.hh"
#include "sstables/sstables_manager.hh"
#include "utils/crc.hh"
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
    for (const auto& value :
            {std::string_view(state.snapshot), std::string_view(state.t), std::string_view(state.source_keyspace), std::string_view(state.source_table)}) {
        if (value.find('\n') != std::string_view::npos) {
            throw std::runtime_error("pitr replay mark: a value may not hold a newline");
        }
    }
    if (state.started_at_us < 0) {
        throw std::runtime_error("pitr replay mark: started_at_us may not be negative");
    }
    return seastar::format("format={}\nversion={}\nphase={}\nsnapshot={}\nt={}\nsource_keyspace={}\nsource_table={}\nstarted_at_us={}\n", replay_state_format,
            replay_state_version_1, phase_name(state.phase), state.snapshot, state.t, state.source_keyspace, state.source_table, state.started_at_us);
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
    state.source_keyspace = sstring(take_line(text, "source_keyspace"));
    state.source_table = sstring(take_line(text, "source_table"));
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

std::filesystem::path restore_root(const std::filesystem::path& work_dir) {
    return work_dir / "pitr" / "restore";
}

std::filesystem::path restore_dir_path(const std::filesystem::path& work_dir, ::table_id target) {
    return restore_root(work_dir) / fmt::format("{}", target.uuid());
}

future<std::filesystem::path> make_restore_dir(const std::filesystem::path& work_dir, ::table_id target) {
    auto dir = restore_dir_path(work_dir, target);
    if (co_await file_exists(dir.native())) {
        replay_log.info("dropping what an earlier replay of {} left in {}", target, dir.native());
        co_await recursive_remove_directory(dir);
    }
    co_await recursive_touch_directory(dir.native());
    co_return dir;
}

future<> remove_restore_dir(const std::filesystem::path& work_dir, ::table_id target) noexcept {
    auto dir = restore_dir_path(work_dir, target);
    try {
        if (co_await file_exists(dir.native())) {
            co_await recursive_remove_directory(dir);
        }
    } catch (...) {
        replay_log.warn("could not remove the replay's restore directory {}: {}", dir.native(), std::current_exception());
    }
}

future<> sweep_restore_dirs(const std::filesystem::path& work_dir) noexcept {
    auto root = restore_root(work_dir);
    try {
        if (co_await file_exists(root.native())) {
            replay_log.info("dropping the segments an interrupted replay left in {}", root.native());
            co_await recursive_remove_directory(root);
        }
    } catch (...) {
        replay_log.warn("could not sweep {}: {}", root.native(), std::current_exception());
    }
}

static std::unexpected<refusal> refuse(refusal_reason reason, sstring message, sstring detail);

// A file no step may read again. Removing it must not hide the failure that led
// here, nor fail a run that is otherwise fine.
static future<> remove_quietly(const std::filesystem::path& path) noexcept {
    try {
        if (co_await file_exists(path.native())) {
            co_await remove_file(path.native());
        }
    } catch (...) {
        replay_log.warn("could not remove {}: {}", path.native(), std::current_exception());
    }
}

// The bytes the fetch read, so the caller can tell a short object from a long
// one without reading the file again.
struct fetched_bytes {
    uint64_t size = 0;
    uint32_t crc32 = 0;
};

// Streams the object into the open file, stopping one byte past the size the
// ledger recorded: an object that grew is refused by the size check, and never
// written whole.
static future<fetched_bytes> stream_object_to_file(
        sstables::object_storage_client& client, const sstables::object_name& object, output_stream<char>& out, uint64_t used_size, abort_source& as) {
    fetched_bytes fetched;
    utils::crc32 crc;
    auto is = input_stream<char>(client.make_download_source(object, &as));
    std::exception_ptr ex;
    try {
        while (fetched.size <= used_size) {
            as.check();
            auto buf = co_await is.read();
            if (buf.empty()) {
                break;
            }
            fetched.size += buf.size();
            crc.process(reinterpret_cast<const uint8_t*>(buf.get()), buf.size());
            co_await out.write(buf.get(), buf.size());
            co_await coroutine::maybe_yield();
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await is.close();
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
    fetched.crc32 = crc.get();
    co_return fetched;
}

future<std::expected<std::filesystem::path, refusal>> fetch_segment(sstables::object_storage_client& client, sstring bucket, const shipped_segment& segment,
        uint32_t expected_crc32, std::filesystem::path dir, abort_source& as) {
    // The ledger refuses a file name holding a path separator, so this cannot
    // reach out of the directory.
    const auto local = dir / std::filesystem::path(std::string_view(segment.path)).filename();
    const auto object = sstables::object_name(bucket, segment.path);
    std::exception_ptr read_error;
    fetched_bytes fetched;
    try {
        auto f = co_await open_file_dma(local.native(), open_flags::wo | open_flags::create | open_flags::truncate);
        auto out = co_await make_file_output_stream(std::move(f));
        std::exception_ptr ex;
        try {
            fetched = co_await stream_object_to_file(client, object, out, segment.used_size, as);
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        co_await out.close();
        if (ex) {
            std::rethrow_exception(std::move(ex));
        }
    } catch (...) {
        read_error = std::current_exception();
    }
    if (read_error) {
        // Nothing reads a partial file, and the next run would meet it under
        // the same name.
        co_await remove_quietly(local);
        if (co_await client.object_exists(object, &as)) {
            std::rethrow_exception(std::move(read_error));
        }
        co_return refuse(refusal_reason::shipped_object_missing,
                "A commitlog segment the protection records name is not in the store, so the history between the backup and T has a hole. Restore from a "
                "later backup.",
                seastar::format("{}: {}", object, read_error));
    }
    if (fetched.size != segment.used_size || fetched.crc32 != expected_crc32) {
        co_await remove_quietly(local);
        co_return refuse(refusal_reason::shipped_object_damaged,
                "A commitlog segment in the store is not the one its protection record describes, so what it holds cannot be trusted. Restore from a later "
                "backup.",
                seastar::format("{}: {} bytes with checksum {:#x}, where the record says {} bytes with checksum {:#x}", object, fetched.size, fetched.crc32,
                        segment.used_size, expected_crc32));
    }
    replay_log.debug("fetched {} to {}, {} bytes", object, local.native(), fetched.size);
    co_return local;
}

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
        // Only where describe writes it, which is the line that closes the
        // column list: anywhere else those words are a value, not the option.
        // The `ID` option opens the WITH clause, so the next line's `AND` still
        // reads as a continuation of a `WITH` that now carries no option.
        if (line.starts_with(") WITH ID = ")) {
            line = ") WITH";
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

std::optional<::table_id> parse_source_table_id(std::string_view schema_cql) {
    // Describe writes the id as the first WITH option, on the line that closes
    // the column list; anywhere else those words are part of a value.
    static constexpr std::string_view opener = ") WITH ID = ";
    while (!schema_cql.empty()) {
        const auto end = schema_cql.find('\n');
        const auto line = trim(end == std::string_view::npos ? schema_cql : schema_cql.substr(0, end));
        schema_cql.remove_prefix(end == std::string_view::npos ? schema_cql.size() : end + 1);
        if (!line.starts_with(opener)) {
            continue;
        }
        const auto value = trim(line.substr(opener.size()));
        try {
            auto id = ::table_id(utils::UUID(value));
            if (id.uuid()) {
                return id;
            }
        } catch (const marshal_exception&) {
        }
        return std::nullopt;
    }
    return std::nullopt;
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

// Reads one whole file of the backup, or nothing when the store does not hold
// it: a read error is told from an absent object by asking the store. The key
// parts come by value, since this suspends.
static future<std::optional<sstring>> read_backup_file(
        sstables::object_storage_client& client, sstring bucket, sstring prefix, sstring name, size_t cap, abort_source& as) {
    auto object = sstables::object_name(bucket, prefix, name);
    std::exception_ptr read_error;
    sstring body;
    try {
        auto is = input_stream<char>(client.make_download_source(object, &as));
        std::exception_ptr ex;
        try {
            // One byte over the cap tells an oversized object from a read
            // this code truncated; either way the parse refuses it.
            while (body.size() <= cap) {
                auto buf = co_await is.read();
                if (buf.empty()) {
                    break;
                }
                body.append(buf.get(), buf.size());
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
        if (co_await client.object_exists(object, &as)) {
            std::rethrow_exception(std::move(read_error));
        }
        replay_log.info("the backup holds no {}", object);
        co_return std::nullopt;
    }
    if (body.size() > cap) {
        throw std::runtime_error(seastar::format("{} is larger than the {} bytes a replay reads", object, cap));
    }
    co_return std::move(body);
}

// The manifest alone grows with the backup, so it is read in chunks and never
// linearized: `parse_backup_manifest` takes it as it comes off the wire.
static future<std::optional<rjson::chunked_content>> read_backup_manifest_bytes(
        sstables::object_storage_client& client, sstring bucket, sstring prefix, size_t cap, abort_source& as) {
    auto object = sstables::object_name(bucket, prefix, manifest_file_name);
    std::exception_ptr read_error;
    rjson::chunked_content content;
    size_t size = 0;
    try {
        auto is = input_stream<char>(client.make_download_source(object, &as));
        std::exception_ptr ex;
        try {
            while (size <= cap) {
                auto buf = co_await is.read();
                if (buf.empty()) {
                    break;
                }
                size += buf.size();
                content.push_back(std::move(buf));
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
        if (co_await client.object_exists(object, &as)) {
            std::rethrow_exception(std::move(read_error));
        }
        replay_log.info("the backup holds no {}", object);
        co_return std::nullopt;
    }
    if (size > cap) {
        throw std::runtime_error(seastar::format("{} is larger than the {} bytes a replay reads", object, cap));
    }
    co_return std::move(content);
}

static std::unexpected<refusal> refuse(refusal_reason reason, sstring message, sstring detail) {
    replay_log.warn("refusing the replay: {} ({})", message, detail);
    return std::unexpected(refusal{.reason = reason, .message = std::move(message), .detail = std::move(detail)});
}

static std::unexpected<refusal> refuse_damaged_manifest(sstring detail) {
    return refuse(refusal_reason::backup_manifest_damaged,
            "The backup's manifest is not one a replay can read, so its sstables cannot be found. Check the prefix, or restore from another backup.",
            std::move(detail));
}

future<std::expected<backup_manifest, refusal>> parse_backup_manifest(rjson::chunked_content&& content, const sstring& expected_snapshot) {
    backup_manifest manifest;
    rjson::value parsed = rjson::null_value();
    std::optional<sstring> damaged;
    try {
        // Yieldable: the manifest grows with the backup, and this is the only
        // parse on the path that does.
        parsed = rjson::parse_yieldable(std::move(content));
        const auto& snapshot = rjson::get(parsed, "snapshot");
        manifest.snapshot = sstring(rjson::get<std::string>(snapshot, "name"));
        const auto& table = rjson::get(parsed, "table");
        manifest.keyspace = sstring(rjson::get<std::string>(table, "keyspace_name"));
        manifest.table = sstring(rjson::get<std::string>(table, "table_name"));
    } catch (const rjson::error& e) {
        damaged = e.what();
    }
    if (damaged) {
        co_await rjson::destroy_gently(std::move(parsed));
        co_return refuse_damaged_manifest(*damaged);
    }
    if (manifest.snapshot != expected_snapshot) {
        co_await rjson::destroy_gently(std::move(parsed));
        co_return refuse(refusal_reason::backup_names_another_table,
                "The backup under this prefix is not the one the call named, so what would be restored is another table's data. Name the backup the prefix "
                "holds, or the prefix of the backup you named.",
                seastar::format("the manifest records snapshot '{}' of {}.{}, the call named '{}'", manifest.snapshot, manifest.keyspace, manifest.table,
                        expected_snapshot));
    }

    // A backup of a table that held nothing carries no sstables: that is a
    // backup like any other, and the replay goes on to the history.
    if (const auto* sstables = rjson::find(parsed, "sstables")) {
        if (sstables->IsArray()) {
            for (const auto& entry : sstables->GetArray()) {
                co_await coroutine::maybe_yield();
                const auto* toc = entry.IsObject() ? rjson::find(entry, "toc_name") : nullptr;
                if (!toc || !toc->IsString()) {
                    damaged = "an entry of 'sstables' is not an object, or carries no toc_name";
                    break;
                }
                manifest.toc_names.push_back(sstring(rjson::to_string_view(*toc)));
            }
        } else {
            damaged = "'sstables' is not an array";
        }
    }
    // A manifest at the cap builds a DOM of millions of nodes, and freeing it
    // in one go stalls as long as the parse did.
    co_await rjson::destroy_gently(std::move(parsed));
    if (damaged) {
        co_return refuse_damaged_manifest(*damaged);
    }
    replay_log.info(
            "the backup of {}.{} under snapshot '{}' holds {} sstable(s)", manifest.keyspace, manifest.table, manifest.snapshot, manifest.toc_names.size());
    co_return std::move(manifest);
}

replay_in_flight::guard::~guard() {
    if (_owner) {
        _owner->_targets.erase(_target);
    }
}

std::optional<replay_in_flight::guard> replay_in_flight::try_take(::table_id target) {
    if (!_targets.insert(target).second) {
        return std::nullopt;
    }
    return guard(*this, target);
}

future<std::expected<replay_start, refusal>> start_replay_to(sharded<replica::database>& db, sstables::storage_manager& sm, replay_in_flight& in_flight,
        std::filesystem::path state_dir, const replay_to_params& p, abort_source& as) {
    auto t = parse_t(p.t);
    if (!t) {
        co_return std::unexpected(t.error());
    }
    if (p.endpoint.empty() || p.bucket.empty() || !sm.is_known_endpoint(p.endpoint)) {
        co_return refuse(refusal_reason::store_location_missing,
                "The replay was given no object-storage location this node knows. Name a configured endpoint and its bucket.",
                seastar::format("endpoint='{}' bucket='{}'", p.endpoint, p.bucket));
    }
    auto& local = db.local();
    if (!local.has_schema(p.keyspace, p.table)) {
        co_return refuse(refusal_reason::target_missing,
                "The target table does not exist. Create it from the backup's schema.cql with compaction disabled, then call the replay again.",
                seastar::format("{}.{}", p.keyspace, p.table));
    }
    auto target_schema = local.find_schema(p.keyspace, p.table);
    auto target = target_schema->id();

    // Before anything that touches the table: a replay restores one table of
    // one node from its own commitlog, which vnodes give and tablets do not
    // (REF-1), and a tablet split would add a compaction group under the hold.
    if (local.find_column_family(target).uses_tablets()) {
        co_return refuse(refusal_reason::target_uses_tablets,
                "The target's keyspace uses tablets, which point-in-time recovery does not support. Create it with tablets disabled.",
                seastar::format("{}.{}", p.keyspace, p.table));
    }

    // Before the first suspension point: two calls must not both pass it.
    auto held = in_flight.try_take(target);
    if (!held) {
        co_return refuse(refusal_reason::replay_already_running,
                "A replay is already running into the target table. Wait for it to end, or replay into a table created for it.",
                seastar::format("{}.{}", p.keyspace, p.table));
    }

    auto mark_path = replay_state_path(state_dir, target);
    std::optional<replay_state> existing;
    try {
        existing = co_await read_replay_state(state_dir, target);
    } catch (...) {
        // Damage is not a target no replay ever touched.
        co_return refuse(refusal_reason::target_already_replayed,
                "A replay already ran into the target table and its mark cannot be read. Replay into a "
                "table created for it.",
                seastar::format("{}: {}", mark_path.native(), std::current_exception()));
    }
    if (existing) {
        co_return refuse(refusal_reason::target_already_replayed, "A replay already ran into the target table. Replay into a table created for it.",
                seastar::format("{} says {}, from backup '{}' to {}", mark_path.native(), phase_name(existing->phase), existing->snapshot, existing->t));
    }

    // Either way counts: the schema option the refusal asks for, or nodetool
    // having disabled it on every shard - the run's hold is what stops a
    // compaction. Every shard, because one left on purges its own tombstones.
    auto compacting_shards = target_schema->compaction_enabled()
                                     ? co_await db.map_reduce0(
                                               [target](replica::database& db) {
                                                   return size_t(!db.find_column_family(target).is_auto_compaction_disabled_by_user());
                                               },
                                               size_t(0), std::plus<size_t>())
                                     : size_t(0);
    if (compacting_shards) {
        co_return refuse(refusal_reason::target_auto_compaction_enabled,
                "Compaction is enabled on the target, so it could purge a tombstone before the writes it covers are replayed. Create it with compaction = "
                "{'enabled': 'false'}, or disable it, then call the replay again.",
                seastar::format("{}.{}, on {} shard(s)", p.keyspace, p.table, compacting_shards));
    }

    // Sstables and the memtables that hold writes this node took: a replay
    // could not tell its own writes from what was there before.
    auto occupied_shards = co_await db.map_reduce0(
            [target](replica::database& db) {
                auto& t = db.find_column_family(target);
                size_t occupied = t.sstables_count();
                t.for_each_active_memtable([&occupied](replica::memtable& m) {
                    occupied += m.empty() ? 0 : 1;
                });
                return occupied;
            },
            size_t(0), std::plus<size_t>());
    if (occupied_shards) {
        co_return refuse(refusal_reason::target_not_empty,
                "The target table is not empty. A replay writes over it, so it must be a table nothing has been written to.",
                seastar::format("{}.{}", p.keyspace, p.table));
    }

    auto client = sm.get_endpoint_client(p.endpoint);
    auto manifest_bytes = co_await read_backup_manifest_bytes(*client, p.bucket, p.prefix, max_manifest_size, as);
    if (!manifest_bytes) {
        co_return refuse(refusal_reason::backup_file_missing,
                "There is no backup under this prefix: it carries no manifest. Name the prefix the backup was written with.",
                seastar::format("{}/{}", p.prefix, manifest_file_name));
    }
    auto manifest = co_await parse_backup_manifest(std::move(*manifest_bytes), p.snapshot);
    if (!manifest) {
        co_return std::unexpected(std::move(manifest.error()));
    }

    auto bound_body = co_await read_backup_file(*client, p.bucket, p.prefix, bound_file_name, max_bound_file_size, as);
    if (!bound_body) {
        co_return refuse(refusal_reason::backup_file_missing,
                "The backup carries no point-in-time recovery bound, so where its sstables end and the replay starts is unknown. It was taken with shipping "
                "off, or the prefix names another backup.",
                seastar::format("{}/{}", p.prefix, bound_file_name));
    }
    bound_file bound;
    try {
        bound = decode_bound_file(*bound_body);
    } catch (const bound_file_format_error&) {
        co_return refuse(refusal_reason::bound_file_damaged,
                "The backup's point-in-time recovery bound is damaged, so where the replay starts cannot be proved. Restore from another backup.",
                seastar::format("{}/{}: {}", p.prefix, bound_file_name, std::current_exception()));
    }

    auto backup_cql = co_await read_backup_file(*client, p.bucket, p.prefix, schema_cql_file_name, max_schema_cql_size, as);
    if (!backup_cql) {
        co_return refuse(refusal_reason::backup_file_missing,
                "The backup carries no schema, so the target cannot be checked against it. The prefix names another backup.",
                seastar::format("{}/{}", p.prefix, schema_cql_file_name));
    }
    // The replay applies the entries of this one table and no other, so without
    // its id nothing can be replayed: a schema.cql with no `ID` option is not
    // one a snapshot of a supported version wrote.
    const auto source_table_id = parse_source_table_id(*backup_cql);
    if (!source_table_id) {
        co_return refuse(refusal_reason::backup_file_missing,
                "The backup's schema does not record the table's id, so the shipped commitlog cannot be matched to it. The backup was written by an "
                "unsupported version.",
                seastar::format("{}/{} carries no ID option", p.prefix, schema_cql_file_name));
    }

    auto describe_helper = replica::make_schema_describe_helper(target_schema, local.as_data_dictionary());
    auto target_cql = target_schema->describe(describe_helper, cql3::describe_option::STMTS_AND_INTERNALS).create_statement->linearize();
    if (auto mismatch = co_await check_schema_match(target_cql, *backup_cql)) {
        replay_log.warn("refusing the replay: {}", *mismatch);
        co_return std::unexpected(std::move(*mismatch));
    }

    // The history is read from where this node ships, not from the call: the
    // backup and the shipped segments can live in different stores, and the call
    // names the backup's (P51). A node with shipping off has nowhere to read the
    // history from, so the replay is refused with nothing loaded and no mark.
    const auto& cfg = local.get_config();
    const sstring history_endpoint = cfg.pitr_endpoint();
    const sstring history_bucket = cfg.pitr_bucket();
    if (history_endpoint.empty() || history_bucket.empty() || !sm.is_known_endpoint(history_endpoint)) {
        co_return refuse(refusal_reason::store_location_missing,
                "This node has no point-in-time shipping location it knows, so the history between the backup and T cannot be read. Set pitr_endpoint and "
                "pitr_bucket to the store the backup's node shipped to, and restart.",
                seastar::format("pitr_endpoint='{}' pitr_bucket='{}'", history_endpoint, history_bucket));
    }

    // Before the mark, so a T above coverage leaves the target untouched and the
    // user retries on it with a T the refusal names (REF-5).
    auto history_client = sm.get_endpoint_client(history_endpoint);
    auto history = co_await check_bound_chains(*history_client, history_bucket, cfg.pitr_prefix(), bound, *t, as);
    if (!history) {
        replay_log.warn("refusing the replay: {}", history.error());
        co_return std::unexpected(std::move(history.error()));
    }

    // Last, so every refusal above leaves the node as it was.
    co_await write_replay_state(state_dir, target,
            replay_state{
                    .phase = replay_phase::started,
                    .snapshot = p.snapshot,
                    .t = p.t,
                    .source_keyspace = manifest->keyspace,
                    .source_table = manifest->table,
                    .started_at_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count(),
            });
    replay_log.info(
            "replay of backup '{}' of {}.{} into {}.{} to {} started: {} sstable(s), {} shard(s) in the bound, captured at {}us, chains covering through {}us",
            p.snapshot, manifest->keyspace, manifest->table, p.keyspace, p.table, p.t, manifest->toc_names.size(), bound.shards.size(), bound.capture_time_us,
            history->covered_through_us);
    co_return replay_start{
            .target = target,
            .t = *t,
            .bound = std::move(bound),
            .history = std::move(*history),
            .toc_names = std::move(manifest->toc_names),
            .source_keyspace = std::move(manifest->keyspace),
            .source_table = std::move(manifest->table),
            .source_table_id = *source_table_id,
            .in_flight = std::move(*held),
    };
}

} // namespace db::pitr
