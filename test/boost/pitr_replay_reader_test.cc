/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the PITR strict replay reader (issue #17): the segment list it is
// given, the per-entry filters, and everything it refuses.
//
// Every case here writes its segments with a real commitlog and reads them back
// with a fake consumer, so the entries, their positions, the chunking and the
// splitting of an oversized write are the real thing. Nothing below applies a
// mutation anywhere.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <algorithm>
#include <random>
#include <vector>

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>

#include <seastar/core/align.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/layered_file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/commitlog/commitlog_extensions.hh"
#include "db/extensions.hh"
#include "db/pitr/replay_reader.hh"
#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"
#include "keys/keys.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "raft/raft.hh"
#include "schema/schema_builder.hh"
#include "test/lib/log.hh"
#include "test/lib/tmpdir.hh"
#include "utils/UUID_gen.hh"
#include "utils/crc.hh"
#include "utils/lister.hh"

using namespace db;
using namespace db::pitr;

namespace {

// A function, not a variable: FILENAME_PREFIX is defined in another
// translation unit and is still empty when this one's statics are built.
static sstring prefix() {
    return db::commitlog::descriptor::FILENAME_PREFIX;
}

static schema_ptr make_schema(const sstring& cf, std::optional<table_id> id = {}) {
    return schema_builder(this_smp_shard_count(), "ks", cf, id).with_column("pk", utf8_type, column_kind::partition_key).with_column("v", bytes_type).build();
}

// The same table with one more column, so another schema version and another
// column mapping under the same table id.
static schema_ptr make_schema_v2(const sstring& cf, table_id id) {
    return schema_builder(this_smp_shard_count(), "ks", cf, id)
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", bytes_type)
            .with_column("w", bytes_type)
            .build();
}

static mutation make_mutation(schema_ptr s, const sstring& pk, size_t value_size) {
    mutation m(s, partition_key::from_single_value(*s, to_bytes(pk)));
    m.set_clustered_cell(clustering_key::make_empty(), to_bytes("v"), data_value(bytes(value_size, 'x')), api::new_timestamp());
    return m;
}

// A commitlog in `dir`, left on disk when `f` returns: release() keeps the
// segments so that the reader can be pointed at them afterwards.
static future<> with_log(const sstring& dir, commitlog::config cfg, noncopyable_function<future<>(commitlog&)> f) {
    cfg.commit_log_location = dir;
    cfg.metrics_category_name = "commitlog";
    auto log = co_await commitlog::create_commitlog(std::move(cfg));
    std::exception_ptr ep;
    try {
        co_await f(log);
        co_await log.sync_all_segments();
    } catch (...) {
        ep = std::current_exception();
    }
    co_await log.release();
    co_await log.shutdown();
    if (ep) {
        std::rethrow_exception(ep);
    }
}

static commitlog::config small_segments() {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.commitlog_total_space_in_mb = 8 * this_smp_shard_count();
    cfg.allow_going_over_size_limit = true;
    return cfg;
}

static future<replay_position> write_mutation(commitlog& log, schema_ptr s, const mutation& m) {
    auto fm = freeze(m);
    commitlog_mutation_entry_writer w(s, fm, commitlog::force_sync::no);
    auto h = co_await log.add_entry(s->id(), w, db::no_timeout);
    co_return h.release();
}

static future<replay_position> write_raft_batch(commitlog& log, const table_id& tid, raft::group_id gid) {
    const std::vector<raft::log_entry_ptr> entries{
            make_lw_shared<raft::log_entry>(raft::log_entry{.term = raft::term_t(1), .idx = raft::index_t(1), .data = raft::log_entry::dummy{}})};
    commitlog_raft_batch_writer sizer(gid, raft::index_t{0}, entries);
    auto h = co_await log.add(tid, sizer.size(), db::no_timeout, commitlog::force_sync::no, [&](auto& out) {
        commitlog_raft_batch_writer w(gid, raft::index_t{0}, entries);
        w.write(out);
    });
    co_return h.release();
}

// A small entry and an oversized one in one batch of the same schema. The
// commitlog sizes both while the version is still known and may then forget it
// before the chunk is written, so the split write carries no mapping of its own
// and depends on the small entry's.
static future<std::vector<replay_position>> write_small_and_split_batch(commitlog& log, schema_ptr s) {
    auto small = freeze(make_mutation(s, "small", 16));
    auto big = freeze(make_mutation(s, "big", 2 * log.max_record_size()));
    utils::chunked_vector<commitlog_mutation_entry_writer> writers;
    writers.emplace_back(s, small, commitlog::force_sync::no);
    writers.emplace_back(s, big, commitlog::force_sync::no);
    auto handles = co_await log.add_entries(std::move(writers), db::no_timeout);
    std::vector<replay_position> rps;
    for (auto& h : handles) {
        rps.push_back(h.release());
    }
    co_return rps;
}

// An entry the commitlog accepts and checksums, and no format can decode.
static future<replay_position> write_undecodable_entry(commitlog& log, const table_id& tid) {
    constexpr size_t size = 64;
    auto h = co_await log.add(tid, size, db::no_timeout, commitlog::force_sync::no, [](auto& out) {
        const std::vector<char> junk(size, '\xbf');
        out.write(junk.data(), junk.size());
    });
    co_return h.release();
}

// A mutation entry written with its mapping skipped, which the commitlog itself
// does for a version it still knows. Here nothing else carries the mapping for
// this version.
static future<replay_position> write_entry_without_a_mapping(commitlog& log, schema_ptr s, const mutation& m) {
    auto fm = freeze(m);
    bytes_ostream bo;
    ser::writer_of_mutation_entry<bytes_ostream>(bo).skip_mapping().write_mutation(fm).end_mutation_entry();
    auto h = co_await log.add(s->id(), bo.size(), db::no_timeout, commitlog::force_sync::no, [&](auto& out) {
        for (bytes_view f : bo.fragments()) {
            out.write(reinterpret_cast<const char*>(f.data()), f.size());
        }
    });
    co_return h.release();
}

// What the reader handed over, in the order it did.
struct capture {
    std::vector<replay_position> positions;
    std::vector<table_id> tables;
    std::vector<bytes_ostream> blobs;
    std::vector<column_mapping> mappings;
    // Every call, including the one that fails: a reader that carried on past a
    // failure and rethrew at the end would show up here.
    size_t calls = 0;
    size_t fail_at = std::numeric_limits<size_t>::max();

    replay_consumer consumer() {
        return [this](const replay_entry& e) -> future<> {
            if (calls++ == fail_at) {
                throw std::runtime_error("consumer failed on purpose");
            }
            positions.push_back(e.position);
            tables.push_back(e.mutation.column_family_id());
            blobs.push_back(e.mutation.representation());
            mappings.push_back(e.mapping);
            return make_ready_future<>();
        };
    }
};

// Boost cannot print a vector of positions, so compare the rendered lists: a
// failure then shows both of them.
static sstring to_str(const std::vector<replay_position>& v) {
    return fmt::format("[{}]", fmt::join(v, ", "));
}

static future<std::vector<sstring>> segment_names(const sstring& dir) {
    std::vector<sstring> names;
    co_await lister::scan_dir(std::filesystem::path(dir), lister::dir_entry_types::of<directory_entry_type::regular>(),
            [&](std::filesystem::path parent, directory_entry de) -> future<> {
                if (sstring(de.name).find(prefix()) == 0) {
                    names.push_back(sstring((parent / de.name).string()));
                }
                return make_ready_future<>();
            });
    std::ranges::sort(names);
    co_return names;
}

// Stands in for the chain check the reader's caller will run: every one of
// `shard`'s segments in `dir`, by ascending id, each with the size it has on
// disk, which is the size a shipped segment arrives with.
static future<utils::chunked_vector<shipped_segment>> verified_list(const sstring& dir, unsigned shard) {
    std::vector<std::pair<segment_id_type, sstring>> found;
    for (auto& path : co_await segment_names(dir)) {
        const commitlog::descriptor d(std::string(path), prefix());
        if (replay_position(d).shard_id() == shard) {
            found.emplace_back(d.id, path);
        }
    }
    std::ranges::sort(found);
    utils::chunked_vector<shipped_segment> segments;
    for (auto& [id, path] : found) {
        segments.push_back(shipped_segment{.path = path, .used_size = co_await file_size(path)});
    }
    co_return segments;
}

// A bound below every segment a case writes, for the cases that do not care where the replay starts: the reader refuses
// a default bound, and a segment id starts at 1.
static replay_position lowest_bound(unsigned shard = this_shard_id()) {
    return replay_position(shard, segment_id_type(1), 0);
}

static future<shard_replay_params> params_for(const sstring& dir, const table_id& table, std::optional<replay_position> bound = {},
        replay_position floor = {}, unsigned shard = this_shard_id()) {
    co_return shard_replay_params{
            .segments = co_await verified_list(dir, shard),
            .filename_prefix = prefix(),
            .shard = shard,
            .source_table = table,
            .bound = bound.value_or(lowest_bound(shard)),
            .truncate_floor = floor,
    };
}

// The one segment the test wrote, with the size it has now.
static future<shipped_segment> only_segment(const sstring& dir) {
    auto names = co_await segment_names(dir);
    BOOST_REQUIRE_EQUAL(names.size(), 1u);
    co_return shipped_segment{.path = names[0], .used_size = co_await file_size(names[0])};
}

// Overwrites `len` bytes at `off` with `value`, as commitlog_test's own
// corruption cases do.
static future<> patch_segment(const sstring& seg, uint64_t off, size_t len, char value) {
    auto f = co_await open_file_dma(seg, open_flags::rw);
    std::exception_ptr ep;
    try {
        const size_t size = align_up<size_t>(off + len, 4096);
        auto buf = co_await f.dma_read_exactly<char>(0, size);
        std::fill_n(buf.get_write() + off, len, value);
        co_await f.dma_write(0, buf.get(), buf.size());
    } catch (...) {
        ep = std::current_exception();
    }
    co_await f.close();
    if (ep) {
        std::rethrow_exception(ep);
    }
}

static future<> truncate_segment(const sstring& seg, uint64_t size) {
    auto f = co_await open_file_dma(seg, open_flags::rw);
    co_await f.truncate(size);
    co_await f.close();
}

// A segment with a valid header and the zero chunk header the commitlog writes
// when it closes a segment nothing was written to. Built by hand because a live
// commitlog deletes such a segment rather than leaving it to be shipped.
static future<shipped_segment> write_header_only_segment(const sstring& dir, segment_id_type id) {
    constexpr size_t alignment = 4096;
    constexpr size_t sector_overhead = sizeof(uint32_t) + sizeof(segment_id_type);
    const auto ver = commitlog::descriptor::current_version;
    const auto path = sstring(fmt::format("{}/{}{}-{}.log", dir, prefix(), ver, id));

    auto buf = temporary_buffer<char>::aligned(alignment, alignment);
    std::fill_n(buf.get_write(), alignment, 0);

    // The file header: magic, version, id, alignment, checksum.
    char* p = buf.get_write();
    seastar::write_be<uint32_t>(p, ('S' << 24) | ('C' << 16) | ('L' << 8) | 'C');
    seastar::write_be<uint32_t>(p + 4, ver);
    seastar::write_be<uint64_t>(p + 8, id);
    seastar::write_be<uint32_t>(p + 16, alignment);
    utils::crc32 header_crc;
    header_crc.process_be<uint32_t>(ver);
    header_crc.process_be<int32_t>(id & 0xffffffff);
    header_crc.process_be<int32_t>(id >> 32);
    header_crc.process_be<uint32_t>(alignment);
    seastar::write_be<uint32_t>(p + 20, header_crc.get());

    // Bytes 24..32 are the chunk header, and a zero one ends the log. The
    // sector's trailer is the segment id and a checksum over everything before
    // it.
    seastar::write_be<uint64_t>(p + alignment - sector_overhead, id);
    utils::crc32 sector_crc;
    sector_crc.process(reinterpret_cast<const uint8_t*>(buf.get()), alignment - sizeof(uint32_t));
    seastar::write_be<uint32_t>(p + alignment - sizeof(uint32_t), sector_crc.get());

    auto f = co_await open_file_dma(path, open_flags::create | open_flags::wo);
    co_await f.dma_write(0, buf.get(), buf.size());
    co_await f.close();
    co_return shipped_segment{.path = path, .used_size = alignment};
}

// An oversized write, split by the commitlog across more than one segment.
static commitlog::config fragmenting_segments() {
    auto cfg = small_segments();
    cfg.allow_fragmented_entries = true;
    return cfg;
}

// Trims every segment in `dir` to the bytes it actually uses, as the shipper
// does, and returns the list for them. A segment on disk is preallocated and
// padded with zeroes, so the used length is the last non-zero byte rounded up to
// the sector the commitlog writes in.
static future<utils::chunked_vector<shipped_segment>> trim_to_used_bytes(const sstring& dir, unsigned shard) {
    constexpr size_t alignment = 4096;
    size_t before = 0;
    for (auto& [path, size] : co_await verified_list(dir, shard)) {
        auto f = co_await open_file_dma(path, open_flags::rw);
        auto buf = co_await f.dma_read_exactly<char>(0, align_up<size_t>(size, alignment));
        size_t used = buf.size();
        while (used > 0 && buf[used - 1] == 0) {
            --used;
        }
        const auto trimmed = align_up<size_t>(used, alignment);
        BOOST_REQUIRE_LE(trimmed, size);
        before += size;
        co_await f.truncate(trimmed);
        co_await f.close();
    }
    auto segments = co_await verified_list(dir, shard);
    size_t after = 0;
    for (auto& seg : segments) {
        after += seg.used_size;
    }
    // The first segment of a split write is full and has no tail to lose, but the
    // last one always has one, so the total must shrink: the point of this helper
    // is that the reader is handed files with no zero tail.
    BOOST_REQUIRE_LT(after, before);
    co_return segments;
}

// A fake commitlog file extension that obfuscates the bytes on disk, the way
// encryption at rest does: the segment is unreadable except through the
// extension, so a read that skips the wrapper fails rather than miscounts.
constexpr char obfuscation_key = '\x5a';

// Closes of the wrapper the fake extension returns. A reader that closed the
// file under it, or closed nothing, leaves this at zero.
static size_t obfuscated_closes = 0;

class obfuscated_file_impl : public layered_file_impl {
    static void flip(char* p, size_t n) {
        for (size_t i = 0; i < n; ++i) {
            p[i] ^= obfuscation_key;
        }
    }

public:
    using layered_file_impl::layered_file_impl;

    future<size_t> write_dma(uint64_t, const void*, size_t, io_intent*) override {
        throw std::logic_error("obfuscated_file_impl is read-only");
    }
    future<size_t> write_dma(uint64_t, std::vector<iovec>, io_intent*) override {
        throw std::logic_error("obfuscated_file_impl is read-only");
    }
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent* intent) override {
        const auto n = co_await _underlying_file.dma_read(pos, buffer, len, intent);
        flip(reinterpret_cast<char*>(buffer), n);
        co_return n;
    }
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        const auto n = co_await _underlying_file.dma_read(pos, iov, intent);
        for (auto& v : iov) {
            flip(reinterpret_cast<char*>(v.iov_base), v.iov_len);
        }
        co_return n;
    }
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) override {
        auto buf = co_await _underlying_file.dma_read_bulk<uint8_t>(offset, range_size, intent);
        flip(reinterpret_cast<char*>(buf.get_write()), buf.size());
        co_return buf;
    }
    future<> flush() override {
        return _underlying_file.flush();
    }
    future<struct stat> stat() override {
        return _underlying_file.stat();
    }
    future<> truncate(uint64_t length) override {
        return _underlying_file.truncate(length);
    }
    future<> discard(uint64_t offset, uint64_t length) override {
        return _underlying_file.discard(offset, length);
    }
    future<> allocate(uint64_t position, uint64_t length) override {
        return _underlying_file.allocate(position, length);
    }
    future<uint64_t> size() override {
        return _underlying_file.size();
    }
    future<> close() override {
        ++obfuscated_closes;
        return _underlying_file.close();
    }
    std::unique_ptr<seastar::file_handle_impl> dup() override {
        throw std::logic_error("obfuscated_file_impl cannot be duplicated");
    }
    subscription<directory_entry> list_directory(std::function<future<>(directory_entry)>) override {
        throw std::logic_error("obfuscated_file_impl is not a directory");
    }
};

class obfuscating_extension : public db::commitlog_file_extension {
public:
    future<file> wrap_file(const sstring&, file f, open_flags) override {
        co_return file(seastar::make_shared<obfuscated_file_impl>(std::move(f)));
    }
    future<> before_delete(const sstring&) override {
        co_return;
    }
    future<> before_rename(const sstring&, const sstring&) override {
        co_return;
    }
    future<> after_rename(const sstring&, const sstring&) override {
        co_return;
    }
};

// Turns the bytes on disk into what the extension above expects to find.
static future<> obfuscate_segment(const sstring& path) {
    auto f = co_await open_file_dma(path, open_flags::rw);
    auto buf = co_await f.dma_read_exactly<char>(0, co_await f.size());
    for (size_t i = 0; i < buf.size(); ++i) {
        buf.get_write()[i] ^= obfuscation_key;
    }
    co_await f.dma_write(0, buf.get(), buf.size());
    co_await f.close();
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE(pitr_replay_reader_test)

// The bound is inclusive: the entry sitting exactly on it replays, the one
// below it does not, and the one above it does.
SEASTAR_TEST_CASE(test_entry_exactly_at_the_bound_is_replayed) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });
    BOOST_REQUIRE_EQUAL(rps.size(), 3u);

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), rps[1]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[1], rps[2]}));
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, 2u);
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 1u);
    BOOST_REQUIRE_EQUAL(stats.entries_read, 3u);
}

// A bound that falls between two entries of the same chunk must drop only the
// entries below it. Passing it to the reader as a start offset instead would
// select whole chunks, and all twenty entries here share one chunk.
SEASTAR_TEST_CASE(test_bound_inside_a_chunk_drops_only_the_entries_below_it) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 20; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });
    // One chunk: nothing synced in between, and the entries are tiny.
    BOOST_REQUIRE_EQUAL(rps.front().id, rps.back().id);

    // Between entry 9 and entry 10, and above the chunk's own start.
    auto bound = replay_position(rps[10].id, rps[10].pos - 1);
    BOOST_REQUIRE_GT(bound.pos, rps[0].pos);

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), bound), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>(rps.begin() + 10, rps.end())));
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 10u);
}

// System tables share the commitlog, so everything but the source table is
// dropped on the table id alone.
SEASTAR_TEST_CASE(test_only_the_source_table_is_replayed) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto src = make_schema("src");
    auto other = make_schema("other");
    std::vector<replay_position> src_rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 4; ++i) {
            src_rps.push_back(co_await write_mutation(log, src, make_mutation(src, format("pk{}", i), 16)));
            co_await write_mutation(log, other, make_mutation(other, format("pk{}", i), 16));
        }
    });

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, src->id()), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(src_rps));
    BOOST_REQUIRE(std::ranges::all_of(c.tables, [&](const table_id& t) {
        return t == src->id();
    }));
    BOOST_REQUIRE_EQUAL(stats.dropped_other_table, 4u);
}

// A raft batch carries no table data to restore, and is counted and skipped
// rather than mistaken for a mutation.
SEASTAR_TEST_CASE(test_raft_batches_are_skipped) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    auto gid = raft::group_id{utils::UUID_gen::get_time_UUID()};
    std::vector<replay_position> rps;

    auto cfg = small_segments();
    cfg.descriptor_tag = "variant";

    co_await with_log(dir, cfg, [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            co_await write_raft_batch(log, s->id(), gid);
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id()), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(rps));
    BOOST_REQUIRE_EQUAL(stats.raft_batches_skipped, 3u);
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, 3u);
}

// A TRUNCATE above the bound raises the floor: what the source truncated away
// must not come back. The floor is exclusive, so the entry at it is dropped
// where the entry at the bound is kept.
SEASTAR_TEST_CASE(test_truncate_floor_above_the_bound_drops_up_to_the_floor) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 5; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), rps[1], rps[3]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[4]}));
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 1u);
    BOOST_REQUIRE_EQUAL(stats.dropped_at_or_below_truncate_floor, 3u);
}

// A TRUNCATE below the bound is already covered by the snapshot and changes
// nothing.
SEASTAR_TEST_CASE(test_truncate_floor_below_the_bound_drops_nothing_extra) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 5; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), rps[3], rps[1]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[3], rps[4]}));
    BOOST_REQUIRE_EQUAL(stats.dropped_at_or_below_truncate_floor, 0u);
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 3u);
}

// Each shard has its own truncation floor, and one shard's floor says nothing
// about another's. The higher shard is read first, so a floor carried over from
// one read to the next shows up as the wrong entries dropped.
SEASTAR_TEST_CASE(test_truncate_floor_is_applied_per_shard) {
    if (this_smp_shard_count() < 2) {
        testlog.warn("test_truncate_floor_is_applied_per_shard needs at least two shards; skipped");
        co_return;
    }
    tmpdir tmp;
    tmpdir tmp_other;
    auto dir = sstring(tmp.path().string());
    auto other_dir = sstring(tmp_other.path().string());
    const auto tid = table_id(utils::UUID_gen::get_time_UUID());
    auto s = make_schema("cf", tid);
    std::vector<replay_position> local_rps;
    std::vector<replay_position> remote_rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 4; ++i) {
            local_rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });
    // The other shard's segments, written by its own commitlog so that they
    // carry its shard id, then moved in beside this shard's.
    const auto other_shard = this_shard_id() + 1;
    co_await smp::submit_to(other_shard, [&]() -> future<> {
        auto remote_s = make_schema("cf", tid);
        co_await with_log(other_dir, small_segments(), [&](commitlog& log) -> future<> {
            for (int i = 0; i < 4; ++i) {
                remote_rps.push_back(co_await write_mutation(log, remote_s, make_mutation(remote_s, format("pk{}", i), 16)));
            }
        });
    });
    for (auto& seg : co_await segment_names(other_dir)) {
        co_await commitlog::rename_segment(seg, dir + "/" + sstring(std::filesystem::path(seg).filename().string()));
    }
    BOOST_REQUIRE_EQUAL(remote_rps.front().shard_id(), other_shard);
    BOOST_REQUIRE_NE(local_rps.front().shard_id(), remote_rps.front().shard_id());

    // The higher shard truncated at its first entry, this one at its third. The remote bound is its own shard's first
    // position, since a default bound is refused.
    capture remote;
    auto remote_stats =
            co_await replay_shard_segments(co_await params_for(dir, tid, remote_rps[0], remote_rps[0], other_shard), remote.consumer());
    BOOST_REQUIRE_EQUAL(to_str(remote.positions), to_str(std::vector<replay_position>(remote_rps.begin() + 1, remote_rps.end())));
    BOOST_REQUIRE_EQUAL(remote_stats.dropped_at_or_below_truncate_floor, 1u);

    capture local;
    auto local_stats = co_await replay_shard_segments(co_await params_for(dir, tid, {}, local_rps[2]), local.consumer());
    BOOST_REQUIRE_EQUAL(to_str(local.positions), to_str(std::vector<replay_position>{local_rps[3]}));
    BOOST_REQUIRE_EQUAL(local_stats.dropped_at_or_below_truncate_floor, 3u);
}

// A bound nobody set would replay everything. No shard can mean the default position, shard 0 included, since a segment
// id starts at 1.
SEASTAR_TEST_CASE(test_a_default_bound_is_refused) {
    capture c;
    shard_replay_params params{.filename_prefix = prefix(), .shard = this_shard_id(), .source_table = make_schema("cf")->id()};

    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);

    if (this_smp_shard_count() >= 2) {
        params.shard = this_shard_id() + 1;
        BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
    }
}

// Nothing to read is not an error: a shard that took no writes in the window
// ships no segments.
SEASTAR_TEST_CASE(test_empty_list_yields_no_entries) {
    capture c;
    shard_replay_params params{.filename_prefix = prefix(), .shard = this_shard_id(), .source_table = make_schema("cf")->id(),
            .bound = lowest_bound()};

    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.segments_read, 0u);
    BOOST_REQUIRE_EQUAL(stats.entries_read, 0u);
}

// A segment that was closed before anything was written to it holds a valid
// header and the zero chunk header that ends a log. It is read, and contributes
// nothing.
SEASTAR_TEST_CASE(test_header_only_segment_is_read_and_delivers_nothing) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    shard_replay_params params{.filename_prefix = prefix(), .shard = this_shard_id(), .source_table = s->id(), .bound = lowest_bound()};
    params.segments.push_back(co_await write_header_only_segment(dir, replay_position(this_shard_id(), segment_id_type(17), 0).id));

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.segments_read, 1u);
    BOOST_REQUIRE_EQUAL(stats.entries_read, 0u);
}

// The entries arrive in the order of the list, which is the order the source
// wrote them in.
SEASTAR_TEST_CASE(test_entries_arrive_in_the_list_order) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
            co_await log.force_new_active_segment();
        }
    });
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_GE(params.segments.size(), 3u);

    capture c;
    co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(rps));
}

// The commitlog writes a schema version's mapping on the first entry for it
// after the version was last forgotten, so a bound that drops that entry must
// not take the mapping with it: the entries above the bound still need it.
SEASTAR_TEST_CASE(test_the_mapping_survives_a_bound_inside_a_segment) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });
    // One chunk, so only the first of these entries carries the mapping.
    BOOST_REQUIRE_EQUAL(rps.front().id, rps.back().id);

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), rps[1]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[1], rps[2]}));
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 1u);
    BOOST_REQUIRE_EQUAL(c.mappings.size(), 2u);
    for (auto& m : c.mappings) {
        BOOST_REQUIRE(m == s->get_column_mapping());
    }
}

// The same for the truncation floor, which drops entries the same way.
SEASTAR_TEST_CASE(test_the_mapping_survives_a_floor_inside_a_segment) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });
    BOOST_REQUIRE_EQUAL(rps.front().id, rps.back().id);

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), {}, rps[1]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[2]}));
    BOOST_REQUIRE_EQUAL(stats.dropped_at_or_below_truncate_floor, 2u);
    BOOST_REQUIRE_EQUAL(c.mappings.size(), 1u);
    BOOST_REQUIRE(c.mappings[0] == s->get_column_mapping());
}

// A split write whose own entry carries no mapping, reassembled across segments
// and delivered above a bound that dropped the entry its mapping came from.
SEASTAR_TEST_CASE(test_the_mapping_survives_a_bound_across_a_split_write) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        rps = co_await write_small_and_split_batch(log, s);
    });
    BOOST_REQUIRE_EQUAL(rps.size(), 2u);

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), rps[1]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[1]}));
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 1u);
    BOOST_REQUIRE_EQUAL(c.mappings.size(), 1u);
    BOOST_REQUIRE(c.mappings[0] == s->get_column_mapping());
}

// And the same under a floor.
SEASTAR_TEST_CASE(test_the_mapping_survives_a_floor_across_a_split_write) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        rps = co_await write_small_and_split_batch(log, s);
    });
    BOOST_REQUIRE_EQUAL(rps.size(), 2u);

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), {}, rps[0]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[1]}));
    BOOST_REQUIRE_EQUAL(stats.dropped_at_or_below_truncate_floor, 1u);
    BOOST_REQUIRE_EQUAL(c.mappings.size(), 1u);
    BOOST_REQUIRE(c.mappings[0] == s->get_column_mapping());
}

// The mappings are kept across the shard's segments: an entry in a later segment
// resolves against a mapping an earlier segment carried. Clearing the table per
// segment refuses this one.
SEASTAR_TEST_CASE(test_a_mapping_from_an_earlier_segment_resolves_a_later_one) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        // The batch's split write starts in the first segment and ends in the
        // next, and carries no mapping of its own.
        rps = co_await write_small_and_split_batch(log, s);
    });
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_GT(params.segments.size(), 1u);

    capture c;
    co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(rps));
    BOOST_REQUIRE_EQUAL(c.mappings.size(), 2u);
    for (auto& m : c.mappings) {
        BOOST_REQUIRE(m == s->get_column_mapping());
    }
}

// An entry below the bound that carries no mapping is simply dropped: unlike
// normal replay, which fails on any entry of an unknown version, only an entry
// that survives the filters needs its mapping.
SEASTAR_TEST_CASE(test_a_dropped_entry_without_a_mapping_is_not_an_error) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position above;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_entry_without_a_mapping(log, s, make_mutation(s, "no_mapping", 16));
        above = co_await write_mutation(log, s, make_mutation(s, "with_mapping", 16));
    });

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), above), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{above}));
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 1u);
    BOOST_REQUIRE_EQUAL(c.mappings.size(), 1u);
}

// Two versions of the table in one segment: each surviving entry must get its
// own version's mapping, not the other's.
SEASTAR_TEST_CASE(test_two_schema_versions_in_one_segment_keep_their_own_mappings) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    const auto tid = table_id(utils::UUID_gen::get_time_UUID());
    auto v1 = make_schema("cf", tid);
    auto v2 = make_schema_v2("cf", tid);
    BOOST_REQUIRE_NE(v1->version(), v2->version());
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        // The first entry of each version carries its mapping, and the bound
        // below drops both of them.
        rps.push_back(co_await write_mutation(log, v1, make_mutation(v1, "a", 16)));
        rps.push_back(co_await write_mutation(log, v2, make_mutation(v2, "b", 16)));
        rps.push_back(co_await write_mutation(log, v1, make_mutation(v1, "c", 16)));
        rps.push_back(co_await write_mutation(log, v2, make_mutation(v2, "d", 16)));
    });
    BOOST_REQUIRE_EQUAL(rps.front().id, rps.back().id);

    capture c;
    co_await replay_shard_segments(co_await params_for(dir, tid, rps[2]), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rps[2], rps[3]}));
    BOOST_REQUIRE_EQUAL(c.mappings.size(), 2u);
    BOOST_REQUIRE(c.mappings[0] == v1->get_column_mapping());
    BOOST_REQUIRE(c.mappings[1] == v2->get_column_mapping());
    BOOST_REQUIRE(!(c.mappings[0] == c.mappings[1]));
}

// Without a mapping for an entry's version the caller cannot read the mutation,
// and guessing one would corrupt the restore, so the segment is refused.
SEASTAR_TEST_CASE(test_an_entry_whose_mapping_was_never_written_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_entry_without_a_mapping(log, s, make_mutation(s, "pk", 16));
    });

    capture c;
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
    BOOST_REQUIRE(c.positions.empty());
}

// An oversized write is split across segments. One reassembly state shared by
// the shard's segments must put it back together and deliver it once, whole, at
// the position its first fragment took.
SEASTAR_TEST_CASE(test_write_split_across_segments_arrives_whole) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position rp;
    bytes_ostream frozen;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        // Twice the largest entry a segment can hold, so the write cannot land
        // in one segment however the commitlog packs it.
        auto m = make_mutation(s, "big", 2 * log.max_record_size());
        frozen = freeze(m).representation();
        BOOST_REQUIRE_GT(frozen.size(), log.max_record_size());
        rp = co_await write_mutation(log, s, m);
    });

    auto params = co_await params_for(dir, s->id(), rp);
    BOOST_REQUIRE_GT(params.segments.size(), 1u);

    capture c;
    // The bound sits exactly on the split write, which must still survive.
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rp}));
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, 1u);
    // The bytes, not just their count: a wrongly ordered reassembly would have
    // the same size.
    BOOST_REQUIRE(c.blobs.at(0) == frozen);
}

// A split write is filtered at the position of its first fragment, so a bound
// one position below it keeps the write.
SEASTAR_TEST_CASE(test_split_write_survives_a_bound_just_below_its_first_fragment) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position rp;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        rp = co_await write_mutation(log, s, make_mutation(s, "big", 2 * log.max_record_size()));
    });
    BOOST_REQUIRE_GT(rp.pos, 0u);

    capture c;
    auto bound = replay_position(rp.id, rp.pos - 1);
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), bound), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rp}));
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 0u);
}

// The same write, filtered the same way: a bound at the start of the segment
// its tail landed in is above the write's own position, so the write is below
// the bound and the snapshot already holds it.
SEASTAR_TEST_CASE(test_split_write_is_dropped_by_a_bound_in_its_next_segment) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "big", 2 * log.max_record_size()));
    });
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_GT(params.segments.size(), 1u);
    const commitlog::descriptor second(std::string(params.segments[1].path), prefix());
    params.bound = replay_position(second.id, 0);

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 1u);
}

// The floor is exclusive, so a floor exactly on the split write's first
// fragment drops it.
SEASTAR_TEST_CASE(test_split_write_is_dropped_by_a_floor_at_its_first_fragment) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position rp;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        rp = co_await write_mutation(log, s, make_mutation(s, "big", 2 * log.max_record_size()));
    });

    capture c;
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), {}, rp), c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.dropped_at_or_below_truncate_floor, 1u);
}

// One position lower, and the floor keeps it.
SEASTAR_TEST_CASE(test_split_write_survives_a_floor_just_below_its_first_fragment) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position rp;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        rp = co_await write_mutation(log, s, make_mutation(s, "big", 2 * log.max_record_size()));
    });
    BOOST_REQUIRE_GT(rp.pos, 0u);

    capture c;
    auto floor = replay_position(rp.id, rp.pos - 1);
    auto stats = co_await replay_shard_segments(co_await params_for(dir, s->id(), {}, floor), c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rp}));
    BOOST_REQUIRE_EQUAL(stats.dropped_at_or_below_truncate_floor, 0u);
}

// A list that ends inside a split write delivers nothing of it and still
// succeeds: the write is counted as not verified, with the position to pick an
// earlier T from (#17, decision P22).
SEASTAR_TEST_CASE(test_a_split_write_missing_its_tail_is_counted_not_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position rp;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        rp = co_await write_mutation(log, s, make_mutation(s, "big", 2 * log.max_record_size()));
    });
    auto params = co_await params_for(dir, s->id(), rp);
    BOOST_REQUIRE_GT(params.segments.size(), 1u);
    // The history ends at a break: every segment but the one the tail landed in.
    params.segments.pop_back();

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, 0u);
    BOOST_REQUIRE_EQUAL(stats.incomplete_split_writes, 1u);
    BOOST_REQUIRE_EQUAL(to_str({stats.earliest_incomplete_split_write}), to_str({rp}));
}

// The same write with its last segment in the list is restored, and nothing is
// left pending: the count must not fire on a write the history holds whole.
SEASTAR_TEST_CASE(test_a_complete_split_write_counts_nothing_incomplete) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position rp;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        rp = co_await write_mutation(log, s, make_mutation(s, "big", 2 * log.max_record_size()));
    });
    auto params = co_await params_for(dir, s->id(), rp);
    BOOST_REQUIRE_GT(params.segments.size(), 1u);

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{rp}));
    BOOST_REQUIRE_EQUAL(stats.incomplete_split_writes, 0u);
    BOOST_REQUIRE_EQUAL(to_str({stats.earliest_incomplete_split_write}), to_str({replay_position()}));
}

// A bound inside a split write changes nothing about reassembly: the write is
// put back together from the whole list and only then dropped by the bound, as
// the reader's bound rule has it, so it is not an incomplete write.
SEASTAR_TEST_CASE(test_a_bound_inside_a_split_write_counts_nothing_incomplete) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "big", 2 * log.max_record_size()));
    });
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_GT(params.segments.size(), 1u);
    const commitlog::descriptor second(std::string(params.segments[1].path), prefix());
    params.bound = replay_position(second.id, 0);

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.dropped_below_bound, 1u);
    BOOST_REQUIRE_EQUAL(stats.incomplete_split_writes, 0u);
}

// Two writes left incomplete, the first missing its head and the second its
// tail. Only the second is counted: a write whose head the list does not hold
// started before the list, so the backup holds it and the source cannot have
// been waiting on it.
SEASTAR_TEST_CASE(test_only_the_incomplete_split_write_whose_head_was_read_is_counted) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position second_rp;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "first", 4 * log.max_record_size()));
        second_rp = co_await write_mutation(log, s, make_mutation(s, "second", 4 * log.max_record_size()));
    });
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_GE(params.segments.size(), 4u);
    // Both ends cut off: the first write loses its head, the second its tail.
    auto all = params.segments;
    params.segments.clear();
    for (size_t i = 1; i + 1 < all.size(); ++i) {
        params.segments.push_back(all[i]);
    }

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.incomplete_split_writes, 1u);
    BOOST_REQUIRE_EQUAL(to_str({stats.earliest_incomplete_split_write}), to_str({second_rp}));
}

// A list that starts inside a split write holds no head of it, so the write
// began before the list and the backup already holds it: nothing is counted.
SEASTAR_TEST_CASE(test_a_list_starting_with_a_split_writes_tail_counts_nothing) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "big", 2 * log.max_record_size()));
    });
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_GT(params.segments.size(), 1u);
    // Everything but the segment the write's head landed in.
    auto all = params.segments;
    params.segments.clear();
    for (size_t i = 1; i < all.size(); ++i) {
        params.segments.push_back(all[i]);
    }

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, 0u);
    BOOST_REQUIRE_EQUAL(stats.incomplete_split_writes, 0u);
}

// A pending split write that a later entry follows was rolled back and never
// acknowledged: an oversized write holds every request-controller permit, so its
// fragments are contiguous and exclusive, and anything written above it means it
// was abandoned. Nothing is counted.
SEASTAR_TEST_CASE(test_a_pending_split_write_a_later_entry_follows_counts_nothing) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position small_rp;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "big", 2 * log.max_record_size()));
        // The small entry in a segment of its own, above every fragment.
        co_await log.force_new_active_segment();
        small_rp = co_await write_mutation(log, s, make_mutation(s, "small", 16));
    });
    auto params = co_await params_for(dir, s->id());
    // Drop the segment the tail landed in, which is the last one below the
    // segment the small entry opened, and keep the small entry above the break.
    std::optional<size_t> tail;
    for (size_t i = 0; i < params.segments.size(); ++i) {
        const commitlog::descriptor d(std::string(params.segments[i].path), prefix());
        if (d.id < small_rp.id) {
            tail = i;
        }
    }
    BOOST_REQUIRE(tail);
    BOOST_REQUIRE_GT(*tail, 0u);
    auto all = params.segments;
    params.segments.clear();
    for (size_t i = 0; i < all.size(); ++i) {
        if (i != *tail) {
            params.segments.push_back(all[i]);
        }
    }

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(std::vector<replay_position>{small_rp}));
    BOOST_REQUIRE_EQUAL(stats.incomplete_split_writes, 0u);
}

// The count is of writes, not of fragments: a write with two fragments held
// apart is one incomplete write.
SEASTAR_TEST_CASE(test_an_incomplete_split_write_of_many_fragments_counts_one) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    replay_position rp;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        rp = co_await write_mutation(log, s, make_mutation(s, "big", 8 * log.max_record_size()));
    });
    auto params = co_await params_for(dir, s->id());
    // Fragments that touch are joined as they arrive, so the second segment is
    // left out: the first and third fragments stay two.
    BOOST_REQUIRE_GE(params.segments.size(), 4u);
    auto first = params.segments[0];
    auto third = params.segments[2];
    params.segments.clear();
    params.segments.push_back(std::move(first));
    params.segments.push_back(std::move(third));

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.incomplete_split_writes, 1u);
    BOOST_REQUIRE_EQUAL(to_str({stats.earliest_incomplete_split_write}), to_str({rp}));
}

// A segment holding only the middle of a split write has no entry to complete:
// the fragments are read and contribute nothing, and that is not a failure. The
// write is not counted either, since its head is not in the list (#17, P22).
SEASTAR_TEST_CASE(test_middle_segment_of_a_split_write_delivers_nothing) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "big", 4 * log.max_record_size()));
    });
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_GE(params.segments.size(), 3u);
    auto middle = params.segments[1];
    params.segments.clear();
    params.segments.push_back(middle);

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE(c.positions.empty());
    BOOST_REQUIRE_EQUAL(stats.segments_read, 1u);
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, 0u);
    BOOST_REQUIRE_EQUAL(stats.incomplete_split_writes, 0u);
}

// Normal replay counts a corrupt stretch and carries on. Restore cannot: the
// entries behind the damage are the ones the user asked for.
SEASTAR_TEST_CASE(test_corrupted_segment_fails_the_operation) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });

    auto seg = co_await only_segment(dir);
    // The checksum half of the second entry's header. The file keeps its size,
    // so this is damage only the commitlog can report.
    co_await patch_segment(seg.path, rps[1].pos + 4, 4, '\x5a');

    capture c;
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), commitlog::segment_data_corruption_error);
}

// A consumer failure is the operation's failure, at the entry that failed:
// nothing past it is read, and the reader does not go on to the end.
SEASTAR_TEST_CASE(test_consumer_failure_stops_the_read) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 5; ++i) {
            co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16));
        }
    });

    capture c;
    c.fail_at = 2;
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), std::runtime_error);

    BOOST_REQUIRE_EQUAL(c.positions.size(), 2u);
    BOOST_REQUIRE_EQUAL(c.calls, 3u);
}

// Random entry sizes, so the reader meets chunk and segment boundaries it was
// not written against, checked against the positions the commitlog handed back.
SEASTAR_TEST_CASE(test_random_sized_entries_match_the_recorded_positions) {
    constexpr uint32_t seed = 0x17ead;
    testlog.info("test_random_sized_entries_match_the_recorded_positions seed: {}", seed);
    std::mt19937 rnd(seed);
    std::uniform_int_distribution<size_t> size_dist(16, 200 * 1024);

    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto src = make_schema("src");
    auto other = make_schema("other");
    std::vector<replay_position> src_rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 40; ++i) {
            auto& s = (rnd() % 4 == 0) ? other : src;
            auto rp = co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), size_dist(rnd)));
            if (s == src) {
                src_rps.push_back(rp);
            }
        }
    });
    auto params = co_await params_for(dir, src->id());
    BOOST_REQUIRE_GT(params.segments.size(), 1u);
    BOOST_REQUIRE_GT(src_rps.size(), 2u);

    auto bound = src_rps[src_rps.size() / 2];
    params.bound = bound;
    std::vector<replay_position> expected;
    std::ranges::copy_if(src_rps, std::back_inserter(expected), [&](const replay_position& rp) {
        return rp >= bound;
    });

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(expected));
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, expected.size());
}

// A shipped segment holds exactly the bytes the source wrote, so a file cut
// short is refused rather than replayed up to the cut.
SEASTAR_TEST_CASE(test_segment_cut_short_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 200; ++i) {
            co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 1024));
        }
    });

    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_EQUAL(params.segments.size(), 1u);
    co_await truncate_segment(params.segments[0].path, align_down<uint64_t>(params.segments[0].used_size / 2, 4096) + 7);

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
    BOOST_REQUIRE(c.positions.empty());
}

// Cut on a chunk boundary instead, where the commitlog sees a complete log that
// simply ends: only the recorded size catches it.
SEASTAR_TEST_CASE(test_segment_cut_at_a_chunk_boundary_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 10; ++i) {
            co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 1024));
            // One chunk per entry.
            co_await log.sync_all_segments();
        }
    });

    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_EQUAL(params.segments.size(), 1u);
    const auto cut = align_down<uint64_t>(params.segments[0].used_size / 2, 4096);
    BOOST_REQUIRE_GT(cut, 0u);
    co_await truncate_segment(params.segments[0].path, cut);

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
    BOOST_REQUIRE(c.positions.empty());
}

// Larger than recorded is as wrong as smaller: the file is not the one the
// caller verified.
SEASTAR_TEST_CASE(test_segment_larger_than_recorded_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });

    auto params = co_await params_for(dir, s->id());
    params.segments[0].used_size -= 4096;

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// The extreme of the same thing: the shipped file never arrived.
SEASTAR_TEST_CASE(test_zero_byte_segment_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });

    auto params = co_await params_for(dir, s->id());
    co_await truncate_segment(params.segments[0].path, 0);

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// A segment the caller listed and the fetch did not deliver.
SEASTAR_TEST_CASE(test_missing_segment_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    shard_replay_params params{.filename_prefix = prefix(), .shard = this_shard_id(), .source_table = s->id(), .bound = lowest_bound()};
    params.segments.push_back(shipped_segment{.path = sstring(fmt::format("{}/{}{}-{}.log", dir, prefix(), commitlog::descriptor::current_version,
                                                      replay_position(this_shard_id(), segment_id_type(3), 0).id)),
            .used_size = 4096});

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// A header of nothing but zeroes is a segment that was never written. The
// commitlog reader takes it for a clean end of log, so the reader checks it
// before handing the file over.
SEASTAR_TEST_CASE(test_zeroed_header_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });

    auto params = co_await params_for(dir, s->id());
    co_await patch_segment(params.segments[0].path, 0, 24, '\0');

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// Not a commitlog segment at all, whatever its name says.
SEASTAR_TEST_CASE(test_wrong_header_magic_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });

    auto params = co_await params_for(dir, s->id());
    co_await patch_segment(params.segments[0].path, 0, 4, '\x7f');

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// A segment whose header names another segment is a recycled file, and the
// commitlog reader again takes it for a clean end of log.
SEASTAR_TEST_CASE(test_header_id_from_another_segment_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });

    auto params = co_await params_for(dir, s->id());
    // The low half of the header's segment id.
    co_await patch_segment(params.segments[0].path, 12, 4, '\x3c');

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// The same segment twice would replay its entries twice.
SEASTAR_TEST_CASE(test_duplicate_segment_id_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });

    auto params = co_await params_for(dir, s->id());
    params.segments.push_back(params.segments[0]);

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// Out of order, a split write between two segments would be reassembled from
// the wrong halves.
SEASTAR_TEST_CASE(test_out_of_order_segment_ids_are_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk0", 16));
        co_await log.force_new_active_segment();
        co_await write_mutation(log, s, make_mutation(s, "pk1", 16));
    });

    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_GE(params.segments.size(), 2u);
    std::ranges::reverse(params.segments);

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// A name the reader cannot parse is a name it cannot take the segment id or the
// entry format from, so it is refused rather than skipped.
SEASTAR_TEST_CASE(test_unparsable_segment_name_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });

    auto seg = co_await only_segment(dir);
    const auto renamed = dir + "/" + prefix() + "not-a-descriptor.log";
    co_await commitlog::rename_segment(seg.path, renamed);

    shard_replay_params params{.filename_prefix = prefix(), .shard = this_shard_id(), .source_table = s->id(), .bound = lowest_bound()};
    params.segments.push_back(shipped_segment{.path = renamed, .used_size = seg.used_size});

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// The tag decides the entry format, and the only one this build can read is the
// variant tag. Normal replay asserts on any other; the reader refuses.
SEASTAR_TEST_CASE(test_unknown_descriptor_tag_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    auto cfg = small_segments();
    cfg.descriptor_tag = "mystery";
    co_await with_log(dir, cfg, [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });

    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_GE(params.segments.size(), 1u);

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// Another shard's segment has its own bound, floor and reassembly state, and
// belongs to another reader. Reading it here would apply the wrong filters.
SEASTAR_TEST_CASE(test_segment_of_another_shard_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });

    auto params = co_await params_for(dir, s->id());
    params.shard = this_shard_id() + 1;

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// Positions are only comparable within a shard, so a bound from another shard
// would silently keep or drop the wrong entries.
SEASTAR_TEST_CASE(test_bound_of_another_shard_is_refused) {
    capture c;
    shard_replay_params params{.filename_prefix = prefix(), .shard = 1, .source_table = make_schema("cf")->id(), .bound = lowest_bound(1)};

    params.bound = replay_position(0u, segment_id_type(5), 10);
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);

    params.bound = replay_position(2u, segment_id_type(5), 10);
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

SEASTAR_TEST_CASE(test_truncate_floor_of_another_shard_is_refused) {
    capture c;
    shard_replay_params params{.filename_prefix = prefix(), .shard = 1, .source_table = make_schema("cf")->id(), .bound = lowest_bound(1)};

    params.truncate_floor = replay_position(0u, segment_id_type(5), 10);
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);

    params.truncate_floor = replay_position(2u, segment_id_type(5), 10);
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// An entry the commitlog checksummed and no reader can decode. Normal replay
// counts one bad mutation and carries on; restore fails.
SEASTAR_TEST_CASE(test_undecodable_mutation_entry_fails_the_operation) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_undecodable_entry(log, s->id());
    });

    capture c;
    auto params = co_await params_for(dir, s->id());
    // Not invalid_replay_input: the entry is undecodable, not a segment that is
    // not the one the list named.
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), std::exception);
    try {
        co_await replay_shard_segments(params, c.consumer());
        BOOST_FAIL("the undecodable entry did not fail the call");
    } catch (const invalid_replay_input& e) {
        BOOST_FAIL(format("failed as invalid input, not as a decode error: {}", e.what()));
    } catch (const std::exception&) {
    }
    BOOST_REQUIRE(c.positions.empty());
}

// The same, in the variant format, where the envelope is read before the
// payload.
SEASTAR_TEST_CASE(test_undecodable_variant_entry_fails_the_operation) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    auto cfg = small_segments();
    cfg.descriptor_tag = "variant";
    co_await with_log(dir, cfg, [&](commitlog& log) -> future<> {
        co_await write_undecodable_entry(log, s->id());
    });

    capture c;
    auto params = co_await params_for(dir, s->id());
    // Not invalid_replay_input: the entry is undecodable, not a segment that is
    // not the one the list named.
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), std::exception);
    try {
        co_await replay_shard_segments(params, c.consumer());
        BOOST_FAIL("the undecodable entry did not fail the call");
    } catch (const invalid_replay_input& e) {
        BOOST_FAIL(format("failed as invalid input, not as a decode error: {}", e.what()));
    } catch (const std::exception&) {
    }
    BOOST_REQUIRE(c.positions.empty());
}

// A recycled segment keeps its id and its size, and read_log_file declines to
// read a file whose name does not start with the prefix, without saying so.
SEASTAR_TEST_CASE(test_recycled_segment_name_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });

    auto seg = co_await only_segment(dir);
    const auto renamed = dir + "/Recycled-" + sstring(std::filesystem::path(std::string(seg.path)).filename().string());
    co_await commitlog::rename_segment(seg.path, renamed);

    shard_replay_params params{.filename_prefix = prefix(), .shard = this_shard_id(), .source_table = s->id(), .bound = lowest_bound()};
    params.segments.push_back(shipped_segment{.path = renamed, .used_size = seg.used_size});

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// An id too large for the descriptor to parse is a name the reader cannot take
// an id from, not an error to let out raw.
SEASTAR_TEST_CASE(test_segment_name_with_an_oversized_id_is_refused) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());

    shard_replay_params params{.filename_prefix = prefix(), .shard = this_shard_id(), .source_table = make_schema("cf")->id(),
            .bound = lowest_bound()};
    params.segments.push_back(shipped_segment{.path = dir + "/" + prefix() + "4-99999999999999999999999999.log", .used_size = 4096});

    capture c;
    BOOST_REQUIRE_THROW(co_await replay_shard_segments(params, c.consumer()), invalid_replay_input);
}

// Both reads of a segment, the header and the entries, must go through the
// commitlog file extensions it was written with. The segment here is unreadable
// without them, so a read that skips the wrapper fails.
SEASTAR_TEST_CASE(test_both_reads_go_through_the_file_extensions) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        for (int i = 0; i < 3; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 16)));
        }
    });
    auto params = co_await params_for(dir, s->id());
    for (auto& seg : params.segments) {
        co_await obfuscate_segment(seg.path);
    }

    db::extensions exts;
    exts.add_commitlog_file_extension("pitr_test_obfuscation", std::make_unique<obfuscating_extension>());
    params.extensions = &exts;

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(rps));
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, 3u);
}

// The shape a segment really arrives in: trimmed to the bytes the source wrote,
// with no zero tail to stop the reader. The split write's bytes must come back
// byte for byte.
SEASTAR_TEST_CASE(test_trimmed_segments_replay_in_the_mutation_format) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;
    bytes_ostream split;

    co_await with_log(dir, fragmenting_segments(), [&](commitlog& log) -> future<> {
        auto big = make_mutation(s, "big", 2 * log.max_record_size());
        split = freeze(big).representation();
        rps.push_back(co_await write_mutation(log, s, big));
        for (int i = 0; i < 3; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 1024)));
        }
    });

    auto params = co_await params_for(dir, s->id());
    params.segments = co_await trim_to_used_bytes(dir, this_shard_id());
    BOOST_REQUIRE_GT(params.segments.size(), 1u);

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(rps));
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, rps.size());
    BOOST_REQUIRE(c.blobs.at(0) == split);
}

// The same, in the variant format, where every entry carries the envelope.
SEASTAR_TEST_CASE(test_trimmed_segments_replay_in_the_variant_format) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");
    std::vector<replay_position> rps;
    bytes_ostream split;

    auto cfg = fragmenting_segments();
    cfg.descriptor_tag = "variant";
    co_await with_log(dir, cfg, [&](commitlog& log) -> future<> {
        auto big = make_mutation(s, "big", 2 * log.max_record_size());
        split = freeze(big).representation();
        rps.push_back(co_await write_mutation(log, s, big));
        for (int i = 0; i < 3; ++i) {
            rps.push_back(co_await write_mutation(log, s, make_mutation(s, format("pk{}", i), 1024)));
        }
    });

    auto params = co_await params_for(dir, s->id());
    params.segments = co_await trim_to_used_bytes(dir, this_shard_id());
    BOOST_REQUIRE_GT(params.segments.size(), 1u);

    capture c;
    auto stats = co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE_EQUAL(to_str(c.positions), to_str(rps));
    BOOST_REQUIRE_EQUAL(stats.entries_replayed, rps.size());
    BOOST_REQUIRE(c.blobs.at(0) == split);
}

// The header pre-read must close the wrapper it read through, not the file under
// it: closing the raw file leaves the wrapper to be destroyed unclosed, and
// closing nothing leaks the descriptor. Either way the wrapper's own close is
// never called, which is what this counts.
SEASTAR_TEST_CASE(test_the_header_read_closes_the_outermost_wrapper) {
    tmpdir tmp;
    auto dir = sstring(tmp.path().string());
    auto s = make_schema("cf");

    co_await with_log(dir, small_segments(), [&](commitlog& log) -> future<> {
        co_await write_mutation(log, s, make_mutation(s, "pk", 16));
    });
    auto params = co_await params_for(dir, s->id());
    BOOST_REQUIRE_EQUAL(params.segments.size(), 1u);
    co_await obfuscate_segment(params.segments[0].path);

    db::extensions exts;
    exts.add_commitlog_file_extension("pitr_test_obfuscation", std::make_unique<obfuscating_extension>());
    params.extensions = &exts;

    capture c;
    obfuscated_closes = 0;
    co_await replay_shard_segments(params, c.consumer());

    BOOST_REQUIRE_EQUAL(c.positions.size(), 1u);
    BOOST_REQUIRE_EQUAL(obfuscated_closes, 1u);
}

BOOST_AUTO_TEST_SUITE_END()
