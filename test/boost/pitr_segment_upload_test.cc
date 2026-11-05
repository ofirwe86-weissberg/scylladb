/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the PITR segment upload (issue #8): streaming exactly the used
// bytes of a local segment file to an object store, with a checksum.
//
// The store is a fake object_storage_client that behaves like a store: the
// bytes a put hands it are invisible until the upload is finalized, and it can
// finalize the way either of the two real sinks does. It can fail a chosen put,
// the flush or the close. The segment file is a real on-disk file, wrapped in a
// file_impl that counts closes and can fail, shorten or empty a read. The
// wrapper delegates dma_read_bulk() rather than re-implementing it, so the
// over-read up to the DMA alignment boundary that a real file does is what the
// code under test sees.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/test/unit_test.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>

#include "db/pitr/segment_upload.hh"
#include "sstables/object_storage_client.hh"
#include "test/lib/log.hh"
#include "test/lib/tmpdir.hh"
#include "utils/exceptions.hh"
#include "utils/memory_data_sink.hh"
#include "utils/upload_progress.hh"

using namespace db::pitr;

namespace {

// Fixed so that a failure can be reproduced; logged wherever content is made.
constexpr uint64_t content_seed = 20261105;

// The byte every recycled tail is filled with. Generated content never contains
// it, so a single one of them in the store proves the tail was uploaded.
constexpr char tail_byte = '\xd7';

constexpr size_t chunk = upload_chunk_size;
constexpr size_t alignment = 4096;

class store_error : public std::runtime_error {
public:
    store_error()
        : std::runtime_error("injected store failure") {
    }
};

class read_error : public std::runtime_error {
public:
    read_error()
        : std::runtime_error("injected read failure") {
    }
};

class file_close_error : public std::runtime_error {
public:
    file_close_error()
        : std::runtime_error("injected file close failure") {
    }
};

// The exception a caller puts on its own abort source, to see it come back.
class caller_cancelled : public std::runtime_error {
public:
    caller_cancelled()
        : std::runtime_error("the caller cancelled the upload") {
    }
};

// Which real sink's finalization the fake imitates.
enum class store_kind {
    // S3: flush() finalizes, and close() aborts an upload that was not
    // finalized, so a failed upload leaves nothing behind.
    abort_on_close,
    // GCS: close() finalizes whatever was put, unless the sink's abort source
    // was aborted, in which case it abandons the upload and reports the abort.
    finalize_on_close,
};

constexpr store_kind both_kinds[] = {store_kind::abort_on_close, store_kind::finalize_on_close};

// What the fake store recorded, and what it was told to fail.
struct fake_store {
    store_kind kind = store_kind::abort_on_close;
    std::optional<unsigned> fail_put_at;
    // Fail every put in the first this-many sinks, which is one failed attempt
    // each for a retry loop.
    unsigned fail_first_sinks = 0;
    bool fail_flush = false;
    bool fail_close = false;
    // Fail the close of this sink only, counting from 0.
    std::optional<unsigned> fail_close_on_sink;
    // Called with the index of each put, before it is applied.
    std::function<void(unsigned)> on_put;
    // Called when the sink is flushed, before the flush is applied.
    std::function<void()> on_flush;
    // Called when the sink is closed, before the close is applied. On GCS the
    // request that finalizes the object is sent from close().
    std::function<void()> on_close;

    // Only finalized objects, as a store's listing holds only those.
    std::unordered_map<std::string, std::string> objects;
    unsigned sinks_created = 0;
    unsigned sinks_closed = 0;
    unsigned puts = 0;
    unsigned flushes = 0;
    // The abort source the last sink was made with. A store needs one to cancel
    // an upload on, so the upload must not pass nullptr.
    abort_source* sink_abort_source = nullptr;
    // The exception that source carried when the sink found it aborted, which
    // is what a real client reports and cancels with.
    std::exception_ptr sink_abort_exception;

    bool has_object(const std::string& name) const {
        return objects.contains(name);
    }
    const std::string& stored(const std::string& name) const {
        auto it = objects.find(name);
        BOOST_REQUIRE_MESSAGE(it != objects.end(), fmt::format("no object named {} in the store", name));
        return it->second;
    }
    uint64_t total_bytes() const {
        uint64_t n = 0;
        for (const auto& [_, bytes] : objects) {
            n += bytes.size();
        }
        return n;
    }
};

class fake_sink_impl final : public data_sink_impl {
    fake_store& _store;
    std::string _name;
    abort_source* _as;
    // Which upload this is, counting from 0, so that a test can fail the first
    // few attempts and let a later one through.
    unsigned _index;
    // Bytes this upload was handed. They reach the store only when the upload
    // is finalized, and they replace the object rather than extending it.
    std::string _pending;

    void finalize() {
        _store.objects[_name] = _pending;
    }
    bool aborted() {
        if (!_as || !_as->abort_requested()) {
            return false;
        }
        _store.sink_abort_exception = _as->abort_requested_exception_ptr();
        return true;
    }
    // A cancelled request reaches the real clients' callers as a store error,
    // not as the abort exception, because the HTTP layer wraps it.
    static storage_io_error cancelled_request() {
        return storage_io_error(ECANCELED, "fake store: request cancelled");
    }

public:
    fake_sink_impl(fake_store& store, std::string name, abort_source* as)
        : _store(store)
        , _name(std::move(name))
        , _as(as)
        , _index(_store.sinks_created++) {
        _store.sink_abort_source = as;
    }
    future<> put(std::span<temporary_buffer<char>> bufs) override {
        for (auto& buf : bufs) {
            const auto index = _store.puts++;
            if (_store.on_put) {
                _store.on_put(index);
            }
            if (_index < _store.fail_first_sinks) {
                return make_exception_future<>(store_error());
            }
            if (_store.fail_put_at == index) {
                return make_exception_future<>(store_error());
            }
            _pending.append(buf.get(), buf.size());
        }
        return make_ready_future<>();
    }
    future<> flush() override {
        ++_store.flushes;
        if (_store.on_flush) {
            _store.on_flush();
        }
        if (_store.fail_flush) {
            return make_exception_future<>(store_error());
        }
        if (_store.kind == store_kind::abort_on_close) {
            // The S3-style flush is what finalizes, so an abort stops it here.
            // The GCS-style one only buffers, and reports nothing.
            if (aborted()) {
                return make_exception_future<>(cancelled_request());
            }
            finalize();
        }
        return make_ready_future<>();
    }
    future<> close() override {
        ++_store.sinks_closed;
        if (_store.on_close) {
            _store.on_close();
        }
        if (_store.fail_close || _store.fail_close_on_sink == _index) {
            return make_exception_future<>(store_error());
        }
        // Asked before the kinds part ways: either real sink cancels its upload
        // on close when the source is aborted.
        const bool was_aborted = aborted();
        if (_store.kind == store_kind::finalize_on_close) {
            if (was_aborted) {
                return make_exception_future<>(cancelled_request());
            }
            finalize();
        }
        return make_ready_future<>();
    }
    size_t buffer_size() const noexcept override {
        return upload_chunk_size;
    }
};

// Only make_upload_sink() is implemented: every other member throws, so a test
// fails loudly if the code under test reaches for a different route.
class fake_client final : public sstables::object_storage_client {
    fake_store& _store;

    static std::logic_error unused(const char* what) {
        return std::logic_error(fmt::format("fake_client: {} is not part of the segment upload", what));
    }

public:
    explicit fake_client(fake_store& store)
        : _store(store) {
    }
    data_sink make_upload_sink(sstables::object_name name, sstables::object_storage_attributes attributes, abort_source* as) override {
        BOOST_REQUIRE_MESSAGE(attributes.empty(), "a shipped segment carries no attributes");
        return data_sink(std::make_unique<fake_sink_impl>(_store, name.str(), as));
    }
    future<> put_object(sstables::object_name, ::memory_data_sink_buffers, sstables::object_storage_attributes, abort_source*) override {
        throw unused("put_object");
    }
    future<> copy_object(sstables::object_name, sstables::object_name, sstables::object_storage_attributes, abort_source*) override {
        throw unused("copy_object");
    }
    future<> delete_object(sstables::object_name, abort_source*) override {
        throw unused("delete_object");
    }
    file make_readable_file(sstables::object_name, abort_source*) override {
        throw unused("make_readable_file");
    }
    data_sink make_data_upload_sink(sstables::object_name, sstables::object_storage_attributes, std::optional<unsigned>, abort_source*) override {
        throw unused("make_data_upload_sink");
    }
    data_source make_download_source(sstables::object_name, abort_source*) override {
        throw unused("make_download_source");
    }
    future<bool> object_exists(sstables::object_name, abort_source*) override {
        throw unused("object_exists");
    }
    future<sstables::object_storage_attributes> get_object_metadata(sstables::object_name, abort_source*) override {
        throw unused("get_object_metadata");
    }
    abstract_lister make_object_lister(std::string, std::string, lister::filter_type) override {
        throw unused("make_object_lister");
    }
    future<> upload_file(std::filesystem::path, sstables::object_name, utils::upload_progress&, abort_source*) override {
        throw unused("upload_file");
    }
    void update_config_sync(const db::object_storage_endpoint_param&) override {
        throw unused("update_config_sync");
    }
    void update_connections_per_shard(unsigned) override {
        throw unused("update_connections_per_shard");
    }
    future<> close() override {
        return make_ready_future<>();
    }
};

struct file_tracker {
    unsigned closes = 0;
    // Fail any read at or past this offset.
    std::optional<uint64_t> fail_read_from;
    // Return at most this many bytes per read, which a real file may also do.
    std::optional<size_t> short_read_size;
    // Return no bytes at or past this offset, as a file that shrank does.
    std::optional<uint64_t> empty_read_from;
    // Fail the handle's close, after the underlying file is really closed.
    bool fail_close = false;
    unsigned reads = 0;
    // A read past this many is a loop the code under test should not be in, so
    // the double fails it: a missing guard then shows as a failure, not a hang.
    unsigned max_reads = 10000;
};

class read_loop_error : public std::runtime_error {
public:
    read_loop_error()
        : std::runtime_error("the file double was read more times than the test allows") {
    }
};

class tracked_file_impl final : public file_impl {
    file _f;
    file_tracker& _tracker;

public:
    tracked_file_impl(file f, file_tracker& tracker)
        : _f(std::move(f))
        , _tracker(tracker) {
        _memory_dma_alignment = _f.memory_dma_alignment();
        _disk_read_dma_alignment = _f.disk_read_dma_alignment();
        _disk_write_dma_alignment = _f.disk_write_dma_alignment();
    }
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t pos, size_t range_size, io_intent* intent) override {
        if (++_tracker.reads > _tracker.max_reads) {
            testlog.error("read {} at {} is past the {} this test allows", _tracker.reads, pos, _tracker.max_reads);
            return make_exception_future<temporary_buffer<uint8_t>>(read_loop_error());
        }
        if (_tracker.fail_read_from && pos >= *_tracker.fail_read_from) {
            testlog.info("failing read at {}", pos);
            return make_exception_future<temporary_buffer<uint8_t>>(read_error());
        }
        if (_tracker.empty_read_from && pos >= *_tracker.empty_read_from) {
            testlog.info("returning an empty read at {}", pos);
            return make_ready_future<temporary_buffer<uint8_t>>(temporary_buffer<uint8_t>());
        }
        if (_tracker.short_read_size) {
            const auto limit = *_tracker.short_read_size;
            return _f.dma_read_bulk<uint8_t>(pos, range_size, intent).then([limit](temporary_buffer<uint8_t> buf) {
                if (buf.size() > limit) {
                    buf.trim(limit);
                }
                return buf;
            });
        }
        return _f.dma_read_bulk<uint8_t>(pos, range_size, intent);
    }
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent* intent) override {
        return _f.dma_read(pos, reinterpret_cast<uint8_t*>(buffer), len, intent);
    }
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        return _f.dma_read(pos, std::move(iov), intent);
    }
    future<uint64_t> size() override {
        return _f.size();
    }
    future<struct stat> stat() override {
        return _f.stat();
    }
    future<> close() override {
        ++_tracker.closes;
        return _f.close().then([this] {
            return _tracker.fail_close ? make_exception_future<>(file_close_error()) : make_ready_future<>();
        });
    }
    future<> flush() override {
        return make_ready_future<>();
    }
    future<> truncate(uint64_t) override {
        throw std::logic_error("read-only");
    }
    future<> allocate(uint64_t, uint64_t) override {
        return make_ready_future<>();
    }
    future<> discard(uint64_t, uint64_t) override {
        return make_ready_future<>();
    }
    future<size_t> write_dma(uint64_t, const void*, size_t, io_intent*) override {
        throw std::logic_error("read-only");
    }
    future<size_t> write_dma(uint64_t, std::vector<iovec>, io_intent*) override {
        throw std::logic_error("read-only");
    }
    subscription<directory_entry> list_directory(std::function<future<>(directory_entry)>) override {
        throw std::logic_error("not a directory");
    }
};

// A pseudo-random prefix of `size` bytes, with `tail_byte` left out of it.
std::string make_content(size_t size) {
    testlog.info("generating {} bytes of content with seed {}", size, content_seed);
    std::mt19937_64 rnd(content_seed);
    std::uniform_int_distribution<int> dist(0, 254);
    std::string content;
    content.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        const int v = dist(rnd);
        content.push_back(static_cast<char>(v >= static_cast<unsigned char>(tail_byte) ? v + 1 : v));
    }
    return content;
}

// Writes `used` followed by tail bytes up to `total_size`, in bounded pieces so
// that a 32 MiB file does not need a 32 MiB buffer.
future<> write_segment_file(const fs::path& path, std::string_view used, uint64_t total_size) {
    BOOST_REQUIRE_GE(total_size, used.size());
    auto f = co_await open_file_dma(path.string(), open_flags::wo | open_flags::create | open_flags::truncate);
    auto os = co_await make_file_output_stream(std::move(f));
    co_await os.write(used.data(), used.size());
    const std::string tail(std::min<uint64_t>(total_size - used.size(), 128 * 1024), tail_byte);
    for (uint64_t written = used.size(); written < total_size;) {
        const auto n = std::min<uint64_t>(total_size - written, tail.size());
        co_await os.write(tail.data(), n);
        written += n;
    }
    co_await os.flush();
    co_await os.close();
}

const std::string object = sstables::object_name("bucket", "segment-1").str();

struct upload_fixture {
    tmpdir dir;
    fake_store store;
    file_tracker tracker;
    fake_client client{store};

    // Writes a segment file holding `used_content` plus a recycled tail up to
    // `total_size`, and uploads `used_size` bytes of it.
    future<upload_result> upload(std::string_view used_content, uint64_t total_size, uint64_t used_size, abort_source* as = nullptr) {
        const auto path = dir.path() / "segment";
        co_await write_segment_file(path, used_content, total_size);
        auto raw = co_await open_file_dma(path.string(), open_flags::ro);
        file f(seastar::make_shared<tracked_file_impl>(std::move(raw), tracker));
        co_return co_await upload_used_bytes(client, sstables::object_name("bucket", "segment-1"), std::move(f), used_size, as);
    }
    future<upload_result> upload(std::string_view used_content, uint64_t total_size) {
        return upload(used_content, total_size, used_content.size());
    }

    // The file and the sink are closed exactly once per attempt, on every path.
    void require_closed(unsigned expected_sinks = 1, unsigned expected_attempts = 1) {
        BOOST_REQUIRE_EQUAL(tracker.closes, expected_attempts);
        BOOST_REQUIRE_EQUAL(store.sinks_created, expected_sinks);
        BOOST_REQUIRE_EQUAL(store.sinks_closed, expected_sinks);
    }
};

// The Castagnoli CRC utils::crc32 computes - initial value 0, no final xor -
// bit by bit, so that it shares nothing with its hardware instructions and
// chunking.
uint32_t crc_reference(std::string_view data) {
    uint32_t crc = 0;
    for (const char c : data) {
        crc ^= static_cast<unsigned char>(c);
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & 1) ? (crc >> 1) ^ 0x82f63b78u : (crc >> 1);
        }
    }
    return crc;
}

// Drives upload_with_retry() over one segment file, opening a tracked handle
// per attempt so that each one can be failed and accounted for on its own.
struct retry_fixture {
    tmpdir dir;
    fake_store store;
    fake_client client{store};
    // One per attempt, in order. Held by pointer so that a reference handed to
    // a handle survives the next attempt's push_back.
    std::vector<std::unique_ptr<file_tracker>> trackers;
    // The waits the unit announced, in order, instead of waiting out long ones.
    std::vector<std::chrono::milliseconds> backoffs;
    // Called after each wait is announced, to abort or otherwise interfere.
    std::function<void()> on_backoff;
    fs::path path;
    abort_source as;

    void write(std::string_view used_content, uint64_t total_size) {
        path = dir.path() / "segment";
        write_segment_file(path, used_content, total_size).get();
    }

    segment_file_opener opener() {
        return [this]() -> future<file> {
            auto& tracker = *trackers.emplace_back(std::make_unique<file_tracker>());
            auto raw = co_await open_file_dma(path.string(), open_flags::ro);
            co_return file(seastar::make_shared<tracked_file_impl>(std::move(raw), tracker));
        };
    }

    // Short enough that even an unbounded sequence of waits stays in the
    // milliseconds, since what is checked is the sequence, not its duration.
    retry_policy policy() {
        return retry_policy{std::chrono::milliseconds(1), std::chrono::milliseconds(4), [this](std::chrono::milliseconds wait) {
                                backoffs.push_back(wait);
                                if (on_backoff) {
                                    on_backoff();
                                }
                            }};
    }

    future<upload_result> upload(uint64_t used_size) {
        return upload_with_retry(client, sstables::object_name("bucket", "segment-1"), used_size, opener(), policy(), as);
    }

    // Every handle that was opened was closed exactly once.
    void require_all_handles_closed(unsigned expected_attempts) {
        BOOST_REQUIRE_EQUAL(trackers.size(), expected_attempts);
        for (const auto& tracker : trackers) {
            BOOST_REQUIRE_EQUAL(tracker->closes, 1u);
        }
    }
};

} // namespace

SEASTAR_THREAD_TEST_CASE(test_stored_bytes_are_the_files_first_used_bytes) {
    upload_fixture fx;
    const auto content = make_content(5 * chunk + 1234);
    const auto res = fx.upload(content, 8 * chunk).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, content.size());
    const auto& stored = fx.store.stored(object);
    BOOST_REQUIRE_EQUAL(stored.size(), content.size());
    BOOST_REQUIRE(stored == content);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_recycled_tail_never_reaches_the_store) {
    // 1000 is neither chunk- nor alignment-aligned, so the single read reaches
    // past the used size and has to be trimmed before it is put.
    upload_fixture fx;
    const auto content = make_content(1000);
    const auto res = fx.upload(content, 8 * chunk).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, content.size());
    const auto& stored = fx.store.stored(object);
    BOOST_REQUIRE_EQUAL(std::ranges::count(stored, tail_byte), 0);
    BOOST_REQUIRE(stored == content);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_one_small_write_in_a_large_segment_stores_little) {
    // A 32 MiB segment holding one 1 KB write ships 1 KB.
    upload_fixture fx;
    const auto content = make_content(1024);
    const auto res = fx.upload(content, 32 * 1024 * 1024).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, 1024u);
    BOOST_REQUIRE_LT(fx.store.total_bytes(), 1024u * 1024u);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_used_size_zero_stores_an_empty_object) {
    upload_fixture fx;
    const auto res = fx.upload(std::string_view(), 8 * chunk, 0).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, 0u);
    BOOST_REQUIRE_EQUAL(res.crc32, 0u);
    BOOST_REQUIRE(fx.store.has_object(object));
    BOOST_REQUIRE_EQUAL(fx.store.stored(object).size(), 0u);
    BOOST_REQUIRE_EQUAL(fx.store.puts, 0u);
    BOOST_REQUIRE_EQUAL(fx.store.flushes, 1u);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_used_sizes_around_chunk_and_alignment_boundaries_store_the_exact_prefix) {
    const uint64_t sizes[] = {
            alignment - 1,
            alignment,
            alignment + 1,
            chunk - 1,
            chunk,
            chunk + 1,
            2 * chunk,
            3 * chunk - 1,
            3 * chunk + 1,
    };
    for (const auto used_size : sizes) {
        testlog.info("used size {}", used_size);
        upload_fixture fx;
        const auto content = make_content(used_size);
        const auto res = fx.upload(content, 4 * chunk).get();
        BOOST_REQUIRE_EQUAL(res.bytes_uploaded, used_size);
        BOOST_REQUIRE(fx.store.stored(object) == content);
        BOOST_REQUIRE_EQUAL(std::ranges::count(fx.store.stored(object), tail_byte), 0);
        fx.require_closed();
    }
}

SEASTAR_THREAD_TEST_CASE(test_used_size_equal_to_the_whole_file) {
    upload_fixture fx;
    const auto content = make_content(2 * chunk + 7);
    const auto res = fx.upload(content, content.size()).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, content.size());
    BOOST_REQUIRE(fx.store.stored(object) == content);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_file_shorter_than_the_used_size_fails) {
    upload_fixture fx;
    const auto content = make_content(alignment);
    // Nothing is uploaded, so no sink is ever made.
    BOOST_REQUIRE_THROW(fx.upload(content, content.size(), content.size() + 1).get(), segment_too_short);
    BOOST_REQUIRE_EQUAL(fx.store.total_bytes(), 0u);
    fx.require_closed(0);
}

SEASTAR_THREAD_TEST_CASE(test_the_checksum_matches_an_independent_computation) {
    upload_fixture fx;
    const auto content = make_content(chunk + 5678);
    const auto res = fx.upload(content, 4 * chunk).get();
    BOOST_REQUIRE_EQUAL(res.crc32, crc_reference(content));
    // And it covers the used bytes only, not the tail.
    BOOST_REQUIRE_NE(res.crc32, crc_reference(std::string(content).append(chunk, tail_byte)));
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_the_checksum_of_the_check_string_is_the_known_answer) {
    // Castagnoli with an initial value of 0 and no final xor over "123456789".
    // Standard CRC32C, which inverts both, gives 0xe3069283 for the same input.
    upload_fixture fx;
    const std::string content = "123456789";
    const auto res = fx.upload(content, alignment).get();
    BOOST_REQUIRE_EQUAL(res.crc32, 0x58e3fa20u);
    BOOST_REQUIRE_EQUAL(crc_reference(content), 0x58e3fa20u);
}

SEASTAR_THREAD_TEST_CASE(test_a_failing_first_put_propagates) {
    upload_fixture fx;
    fx.store.fail_put_at = 0;
    const auto content = make_content(3 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 4 * chunk).get(), store_error);
    BOOST_REQUIRE_EQUAL(fx.store.total_bytes(), 0u);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_failing_later_put_propagates) {
    upload_fixture fx;
    fx.store.fail_put_at = 2;
    const auto content = make_content(5 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 8 * chunk).get(), store_error);
    // Two puts went through before it, and the upload stopped there.
    BOOST_REQUIRE_EQUAL(fx.store.puts, 3u);
    // The bytes those puts carried never become an object.
    BOOST_REQUIRE(!fx.store.has_object(object));
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_failing_flush_propagates) {
    upload_fixture fx;
    fx.store.fail_flush = true;
    const auto content = make_content(2 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 4 * chunk).get(), store_error);
    BOOST_REQUIRE_EQUAL(fx.store.flushes, 1u);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_failing_close_propagates) {
    // The S3-style upload was finalized by the flush, so the object is there
    // and complete; a retry only replaces it with the same bytes.
    upload_fixture fx;
    fx.store.fail_close = true;
    const auto content = make_content(2 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 4 * chunk).get(), store_error);
    BOOST_REQUIRE(fx.store.stored(object) == content);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_failing_close_leaves_no_object_where_the_close_finalizes) {
    // On the GCS path the close is the request that finalizes, so a close that
    // fails as the upload's only error must leave nothing behind.
    upload_fixture fx;
    fx.store.kind = store_kind::finalize_on_close;
    fx.store.fail_close = true;
    const auto content = make_content(2 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 4 * chunk).get(), store_error);
    BOOST_REQUIRE(!fx.store.has_object(object));
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_read_failure_keeps_its_own_error_when_the_file_close_fails_too) {
    // Both the upload and the file's close fail. A retry loop reads the error's
    // type, so the first error has to survive, not be nested in the second.
    upload_fixture fx;
    fx.tracker.fail_close = true;
    fx.tracker.fail_read_from = chunk;
    const auto content = make_content(4 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 8 * chunk).get(), read_error);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_keeps_its_own_error_when_the_file_close_fails_too) {
    upload_fixture fx;
    abort_source as;
    as.request_abort_ex(caller_cancelled());
    fx.tracker.fail_close = true;
    const auto content = make_content(2 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 4 * chunk, content.size(), &as).get(), caller_cancelled);
}

SEASTAR_THREAD_TEST_CASE(test_a_file_close_failure_alone_is_swallowed_by_seastar) {
    // seastar::file::close() reports a failing close and swallows it, so a
    // failing close of the segment file can neither reach the caller nor mask
    // the upload's own error: the two tests above hold for that reason too.
    upload_fixture fx;
    fx.tracker.fail_close = true;
    const auto content = make_content(2 * chunk);
    const auto res = fx.upload(content, 4 * chunk).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, content.size());
    BOOST_REQUIRE(fx.store.stored(object) == content);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_failing_read_propagates) {
    upload_fixture fx;
    fx.tracker.fail_read_from = chunk;
    const auto content = make_content(4 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 8 * chunk).get(), read_error);
    BOOST_REQUIRE_EQUAL(fx.store.puts, 1u);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_read_failure_leaves_no_object_in_either_store) {
    for (const auto kind : both_kinds) {
        testlog.info("store kind {}", static_cast<int>(kind));
        upload_fixture fx;
        fx.store.kind = kind;
        fx.tracker.fail_read_from = chunk;
        const auto content = make_content(4 * chunk);
        BOOST_REQUIRE_THROW(fx.upload(content, 8 * chunk).get(), read_error);
        BOOST_REQUIRE(!fx.store.has_object(object));
        fx.require_closed();
    }
}

SEASTAR_THREAD_TEST_CASE(test_a_read_failure_keeps_its_own_error_when_the_close_fails_too) {
    // A sink that finalizes on close reports the abort the failure path asked
    // for; the read error is still what the caller sees.
    upload_fixture fx;
    fx.store.kind = store_kind::finalize_on_close;
    fx.tracker.fail_read_from = chunk;
    const auto content = make_content(4 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 8 * chunk).get(), read_error);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_retry_to_the_same_name_replaces_the_object) {
    upload_fixture fx;
    const auto first = make_content(3 * chunk);
    fx.upload(first, 4 * chunk).get();
    const std::string second(1000, 'z');
    const auto res = fx.upload(second, 4 * chunk).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, second.size());
    BOOST_REQUIRE(fx.store.stored(object) == second);
    fx.require_closed(2, 2);
}

SEASTAR_THREAD_TEST_CASE(test_the_sink_is_made_with_an_abort_source) {
    upload_fixture fx;
    const auto content = make_content(1000);
    fx.upload(content, 4 * chunk).get();
    BOOST_REQUIRE(fx.store.sink_abort_source != nullptr);
}

SEASTAR_THREAD_TEST_CASE(test_short_reads_still_store_the_exact_prefix) {
    upload_fixture fx;
    fx.tracker.short_read_size = 512;
    const auto content = make_content(2 * chunk + 77);
    const auto res = fx.upload(content, 4 * chunk).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, content.size());
    BOOST_REQUIRE(fx.store.stored(object) == content);
    BOOST_REQUIRE_EQUAL(res.crc32, crc_reference(content));
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_file_that_shrinks_mid_upload_fails) {
    upload_fixture fx;
    fx.tracker.empty_read_from = 2 * chunk;
    // Two chunks and the empty read, with room to spare; a loop on the empty
    // read then fails the read instead of spinning.
    fx.tracker.max_reads = 8;
    const auto content = make_content(4 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 8 * chunk).get(), segment_too_short);
    BOOST_REQUIRE(!fx.store.has_object(object));
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_pre_aborted_abort_source_uploads_nothing) {
    upload_fixture fx;
    abort_source as;
    as.request_abort();
    const auto content = make_content(2 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 4 * chunk, content.size(), &as).get(), abort_requested_exception);
    // Nothing is handed to the store at all, not even a sink to abort.
    fx.require_closed(0);
    BOOST_REQUIRE(!fx.store.has_object(object));
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_reaches_the_caller_with_its_own_exception) {
    // The store reports a cancelled request as a store error, as the real
    // clients do, so the caller's own exception has to be put back.
    upload_fixture fx;
    abort_source as;
    fx.store.on_put = [&as](unsigned index) {
        if (index == 1) {
            as.request_abort_ex(caller_cancelled());
        }
    };
    const auto content = make_content(6 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 8 * chunk, content.size(), &as).get(), caller_cancelled);
    BOOST_REQUIRE(!fx.store.has_object(object));
    // The sink's own source carries it too, which is what the store cancels with.
    BOOST_REQUIRE(fx.store.sink_abort_exception);
    BOOST_REQUIRE_THROW(std::rethrow_exception(fx.store.sink_abort_exception), caller_cancelled);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_during_the_flush_reaches_the_caller_as_an_abort) {
    for (const auto kind : both_kinds) {
        testlog.info("store kind {}", static_cast<int>(kind));
        upload_fixture fx;
        fx.store.kind = kind;
        abort_source as;
        fx.store.on_flush = [&as] {
            as.request_abort_ex(caller_cancelled());
        };
        const auto content = make_content(2 * chunk);
        BOOST_REQUIRE_THROW(fx.upload(content, 4 * chunk, content.size(), &as).get(), caller_cancelled);
        BOOST_REQUIRE(!fx.store.has_object(object));
        fx.require_closed();
    }
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_during_the_close_reaches_the_caller_as_an_abort) {
    // A GCS-style sink sends the request that finalizes the object from
    // close(), and reports an abort landing there as a store error.
    upload_fixture fx;
    fx.store.kind = store_kind::finalize_on_close;
    abort_source as;
    fx.store.on_close = [&as] {
        as.request_abort_ex(caller_cancelled());
    };
    const auto content = make_content(2 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 4 * chunk, content.size(), &as).get(), caller_cancelled);
    BOOST_REQUIRE(!fx.store.has_object(object));
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_during_the_upload_propagates) {
    upload_fixture fx;
    abort_source as;
    fx.store.on_put = [&as](unsigned index) {
        if (index == 1) {
            as.request_abort();
        }
    };
    const auto content = make_content(6 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 8 * chunk, content.size(), &as).get(), abort_requested_exception);
    // The abort is seen before the next chunk is read, not after the whole file.
    BOOST_REQUIRE_EQUAL(fx.store.puts, 2u);
    BOOST_REQUIRE(!fx.store.has_object(object));
    fx.require_closed();
}

// --- Retrying a failed upload (issue #8) ---

SEASTAR_THREAD_TEST_CASE(test_a_working_store_is_uploaded_to_once) {
    retry_fixture fx;
    const auto content = make_content(3 * chunk);
    fx.write(content, 4 * chunk);
    const auto res = fx.upload(content.size()).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, content.size());
    BOOST_REQUIRE(fx.store.stored(object) == content);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 1u);
    BOOST_REQUIRE(fx.backoffs.empty());
    fx.require_all_handles_closed(1);
}

SEASTAR_THREAD_TEST_CASE(test_failed_attempts_are_retried_until_one_succeeds) {
    retry_fixture fx;
    fx.store.fail_first_sinks = 3;
    const auto content = make_content(2 * chunk + 99);
    fx.write(content, 4 * chunk);
    const auto res = fx.upload(content.size()).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, content.size());
    BOOST_REQUIRE_EQUAL(res.crc32, crc_reference(content));
    // The failed attempts left nothing of their own: one object, whole.
    BOOST_REQUIRE_EQUAL(fx.store.objects.size(), 1u);
    BOOST_REQUIRE(fx.store.stored(object) == content);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 4u);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_closed, 4u);
    fx.require_all_handles_closed(4);
}

SEASTAR_THREAD_TEST_CASE(test_every_attempt_opens_its_own_handle) {
    retry_fixture fx;
    fx.store.fail_first_sinks = 2;
    const auto content = make_content(chunk);
    fx.write(content, 2 * chunk);
    fx.upload(content.size()).get();
    // Three handles, each read from and closed on its own: a reused one would
    // show as fewer handles, or as reads on a handle that was already closed.
    BOOST_REQUIRE_EQUAL(fx.trackers.size(), 3u);
    for (const auto& tracker : fx.trackers) {
        BOOST_REQUIRE_EQUAL(tracker->closes, 1u);
        BOOST_REQUIRE_GT(tracker->reads, 0u);
    }
}

SEASTAR_THREAD_TEST_CASE(test_the_backoff_doubles_up_to_its_bound) {
    retry_fixture fx;
    fx.store.fail_first_sinks = 5;
    const auto content = make_content(chunk);
    fx.write(content, 2 * chunk);
    fx.upload(content.size()).get();
    using ms = std::chrono::milliseconds;
    const std::vector<ms> expected{ms(1), ms(2), ms(4), ms(4), ms(4)};
    BOOST_REQUIRE(fx.backoffs == expected);
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_during_an_attempt_is_not_retried) {
    retry_fixture fx;
    const auto content = make_content(6 * chunk);
    fx.write(content, 8 * chunk);
    fx.store.on_put = [&fx](unsigned index) {
        if (index == 1) {
            fx.as.request_abort_ex(caller_cancelled());
        }
    };
    BOOST_REQUIRE_THROW(fx.upload(content.size()).get(), caller_cancelled);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 1u);
    BOOST_REQUIRE(fx.backoffs.empty());
    BOOST_REQUIRE(!fx.store.has_object(object));
    fx.require_all_handles_closed(1);
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_during_a_backoff_is_not_retried) {
    retry_fixture fx;
    // Every attempt fails, so only the abort can end the loop.
    fx.store.fail_first_sinks = 1000;
    fx.on_backoff = [&fx] {
        fx.as.request_abort_ex(caller_cancelled());
    };
    const auto content = make_content(2 * chunk);
    fx.write(content, 4 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content.size()).get(), caller_cancelled);
    BOOST_REQUIRE_EQUAL(fx.backoffs.size(), 1u);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 1u);
    fx.require_all_handles_closed(1);
}

SEASTAR_THREAD_TEST_CASE(test_a_pre_aborted_source_opens_no_file_at_all) {
    retry_fixture fx;
    fx.as.request_abort_ex(caller_cancelled());
    const auto content = make_content(chunk);
    fx.write(content, 2 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content.size()).get(), caller_cancelled);
    BOOST_REQUIRE(fx.trackers.empty());
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 0u);
    BOOST_REQUIRE(!fx.store.has_object(object));
}

SEASTAR_THREAD_TEST_CASE(test_a_segment_too_short_is_not_retried) {
    retry_fixture fx;
    const auto content = make_content(alignment);
    fx.write(content, content.size());
    BOOST_REQUIRE_THROW(fx.upload(content.size() + 1).get(), segment_too_short);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 0u);
    BOOST_REQUIRE(fx.backoffs.empty());
    fx.require_all_handles_closed(1);
}

SEASTAR_THREAD_TEST_CASE(test_a_failed_close_report_and_an_empty_segment_are_not_shippable) {
    // The entry contract: the caller drops these without uploading anything,
    // because the retry loop does not protect either case.
    BOOST_REQUIRE(!segment_is_shippable(7, 4096, true));
    BOOST_REQUIRE(!segment_is_shippable(7, 0, false));
    BOOST_REQUIRE(!segment_is_shippable(7, 0, true));
    BOOST_REQUIRE(segment_is_shippable(7, 1, false));

    // A caller honouring it creates no object for either case.
    retry_fixture fx;
    const auto content = make_content(chunk);
    fx.write(content, 2 * chunk);
    for (const auto [used_size, failed] : {std::pair<uint64_t, bool>(content.size(), true), std::pair<uint64_t, bool>(0, false)}) {
        if (segment_is_shippable(7, used_size, failed)) {
            fx.upload(used_size).get();
        }
    }
    BOOST_REQUIRE(fx.store.objects.empty());
    BOOST_REQUIRE(fx.trackers.empty());
}

SEASTAR_THREAD_TEST_CASE(test_a_failed_attempt_leaves_no_object_in_either_store) {
    for (const auto kind : both_kinds) {
        testlog.info("store kind {}", static_cast<int>(kind));
        retry_fixture fx;
        fx.store.kind = kind;
        fx.store.fail_first_sinks = 1000;
        // Stop after the first failed attempt, and look at what it left.
        fx.on_backoff = [&fx] {
            fx.as.request_abort_ex(caller_cancelled());
        };
        const auto content = make_content(3 * chunk);
        fx.write(content, 4 * chunk);
        BOOST_REQUIRE_THROW(fx.upload(content.size()).get(), caller_cancelled);
        BOOST_REQUIRE(fx.store.objects.empty());
        BOOST_REQUIRE_EQUAL(fx.store.sinks_closed, 1u);
    }
}

SEASTAR_THREAD_TEST_CASE(test_a_failure_after_the_object_landed_leaves_one_object_after_the_retry) {
    // The S3-style flush finalized the object and then the close failed, so the
    // attempt reports a failure over an object that is already complete. The
    // retry writes the same name, so the store keeps exactly one object and the
    // bytes are counted once.
    retry_fixture fx;
    fx.store.fail_close_on_sink = 0;
    const auto content = make_content(2 * chunk + 5);
    fx.write(content, 4 * chunk);
    const auto res = fx.upload(content.size()).get();
    BOOST_REQUIRE_EQUAL(res.bytes_uploaded, content.size());
    BOOST_REQUIRE_EQUAL(fx.store.objects.size(), 1u);
    BOOST_REQUIRE_EQUAL(fx.store.total_bytes(), content.size());
    BOOST_REQUIRE(fx.store.stored(object) == content);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 2u);
    fx.require_all_handles_closed(2);
}
