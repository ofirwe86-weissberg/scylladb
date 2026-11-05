/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the PITR segment upload (issue #8): streaming exactly the used
// bytes of a local segment file to an object store, with a checksum.
//
// The store is a fake object_storage_client that keeps every uploaded byte in
// memory and can fail a chosen put, the flush or the close. The segment file is
// a real on-disk file, wrapped in a file_impl that counts closes and can fail a
// read at a chosen offset. The wrapper delegates dma_read_bulk() rather than
// re-implementing it, so the over-read up to the DMA alignment boundary that a
// real file does is what the code under test sees.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <algorithm>
#include <functional>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <unordered_map>

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

// What the fake store recorded, and what it was told to fail.
struct fake_store {
    std::optional<unsigned> fail_put_at;
    bool fail_flush = false;
    bool fail_close = false;
    // Called with the index of each put, before it is applied.
    std::function<void(unsigned)> on_put;

    std::unordered_map<std::string, std::string> objects;
    unsigned sinks_created = 0;
    unsigned sinks_closed = 0;
    unsigned puts = 0;
    unsigned flushes = 0;

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

public:
    fake_sink_impl(fake_store& store, std::string name)
        : _store(store)
        , _name(std::move(name)) {
        ++_store.sinks_created;
        // An upload that puts nothing still names an object, as a store does.
        _store.objects.try_emplace(_name);
    }
    future<> put(std::span<temporary_buffer<char>> bufs) override {
        for (auto& buf : bufs) {
            const auto index = _store.puts++;
            if (_store.on_put) {
                _store.on_put(index);
            }
            if (_store.fail_put_at == index) {
                return make_exception_future<>(store_error());
            }
            _store.objects[_name].append(buf.get(), buf.size());
        }
        return make_ready_future<>();
    }
    future<> flush() override {
        ++_store.flushes;
        if (_store.fail_flush) {
            return make_exception_future<>(store_error());
        }
        return make_ready_future<>();
    }
    future<> close() override {
        ++_store.sinks_closed;
        if (_store.fail_close) {
            return make_exception_future<>(store_error());
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
    data_sink make_upload_sink(sstables::object_name name, sstables::object_storage_attributes, abort_source*) override {
        return data_sink(std::make_unique<fake_sink_impl>(_store, name.str()));
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
        if (_tracker.fail_read_from && pos >= *_tracker.fail_read_from) {
            testlog.info("failing read at {}", pos);
            return make_exception_future<temporary_buffer<uint8_t>>(read_error());
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
        return _f.close();
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

    // The file and the sink are closed exactly once, on every path.
    void require_closed(unsigned expected_sinks = 1) {
        BOOST_REQUIRE_EQUAL(tracker.closes, 1u);
        BOOST_REQUIRE_EQUAL(store.sinks_created, expected_sinks);
        BOOST_REQUIRE_EQUAL(store.sinks_closed, expected_sinks);
    }
};

// CRC32C, bit by bit, written out here so that it shares nothing with
// utils::crc32's hardware instructions and chunking.
uint32_t crc32c_reference(std::string_view data) {
    uint32_t crc = 0;
    for (const char c : data) {
        crc ^= static_cast<unsigned char>(c);
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & 1) ? (crc >> 1) ^ 0x82f63b78u : (crc >> 1);
        }
    }
    return crc;
}

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
    // SHP-6 at this level: a 32 MiB segment holding one 1 KB write ships 1 KB.
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
    BOOST_REQUIRE_EQUAL(res.crc32c, 0u);
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
    BOOST_REQUIRE_EQUAL(res.crc32c, crc32c_reference(content));
    // And it covers the used bytes only, not the tail.
    BOOST_REQUIRE_NE(res.crc32c, crc32c_reference(std::string(content).append(chunk, tail_byte)));
    fx.require_closed();
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
    // The two puts before it went through, and nothing after.
    BOOST_REQUIRE_EQUAL(fx.store.stored(object).size(), 2 * chunk);
    BOOST_REQUIRE_EQUAL(fx.store.puts, 3u);
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
    upload_fixture fx;
    fx.store.fail_close = true;
    const auto content = make_content(2 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 4 * chunk).get(), store_error);
    fx.require_closed();
}

SEASTAR_THREAD_TEST_CASE(test_a_failing_read_propagates) {
    upload_fixture fx;
    fx.tracker.fail_read_from = chunk;
    const auto content = make_content(4 * chunk);
    BOOST_REQUIRE_THROW(fx.upload(content, 8 * chunk).get(), read_error);
    BOOST_REQUIRE_EQUAL(fx.store.stored(object).size(), chunk);
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
    BOOST_REQUIRE_EQUAL(fx.store.stored(object).size(), 2 * chunk);
    fx.require_closed();
}
