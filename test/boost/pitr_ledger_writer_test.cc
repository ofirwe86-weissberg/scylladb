/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the PITR ledger writer (issue #9): putting one encoded ledger entry
// whole at its chain-scoped key, with a retry behind it.
//
// The store is a fake object_storage_client that behaves like a store: the
// bytes a put hands it are invisible until the upload is finalized, and it can
// finalize the way either of the two real sinks does - S3's flush() or GCS's
// close(). It can fail a chosen put, the flush or the close, and the first few
// sinks whole, which is one failed attempt each for a retry loop.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/test/unit_test.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/later.hh>

#include "db/pitr/ledger_entry.hh"
#include "db/pitr/ledger_writer.hh"
#include "sstables/object_storage_client.hh"
#include "utils/exceptions.hh"
#include "utils/memory_data_sink.hh"
#include "utils/upload_progress.hh"

using namespace db::pitr;

namespace {

constexpr const char* bucket = "test-bucket";
constexpr const char* key_prefix = "pitr/site-a";

const utils::UUID test_host_id{"a1b2c3d4-0000-4000-8000-00000000000f"};
const chain_id test_chain{utils::UUID("11112222-0000-4000-8000-00000000000a")};

class store_error : public std::runtime_error {
public:
    store_error()
        : std::runtime_error("injected store failure") {
    }
};

// The exception a caller puts on its own abort source, to see it come back.
class caller_cancelled : public std::runtime_error {
public:
    caller_cancelled()
        : std::runtime_error("the caller cancelled the ledger write") {
    }
};

// The guard that ends a run the retry loop is plainly not going to end itself.
class too_many_attempts : public std::runtime_error {
public:
    too_many_attempts()
        : std::runtime_error("the writer made more attempts than the test allows") {
    }
};

// Which real sink's finalization the fake imitates.
enum class store_kind {
    // S3: flush() finalizes, and close() aborts an upload that was not
    // finalized, so a failed write leaves nothing behind.
    abort_on_close,
    // GCS: close() finalizes whatever was put, unless the sink's abort source
    // was aborted, in which case it abandons the upload and reports the abort.
    finalize_on_close,
};

constexpr store_kind both_kinds[] = {store_kind::abort_on_close, store_kind::finalize_on_close};

// What the fake store recorded, and what it was told to fail.
struct fake_store {
    store_kind kind = store_kind::abort_on_close;
    // Fail every put in the first this-many sinks, which is one failed attempt
    // each for a retry loop.
    unsigned fail_first_sinks = 0;
    bool fail_flush = false;
    bool fail_close = false;
    // Called with the index of each put, before it is applied.
    std::function<void(unsigned)> on_put;
    // Called when the sink is closed, before the close is applied. On GCS the
    // request that finalizes the object is sent from close().
    std::function<void()> on_close;
    // Called once the object is in the store, from whichever call finalized it:
    // flush() on the S3-style sink, close() on the GCS-style one.
    std::function<void()> on_finalized;

    // Only finalized objects, as a store's listing holds only those.
    std::unordered_map<std::string, std::string> objects;
    unsigned sinks_created = 0;
    unsigned sinks_closed = 0;
    unsigned puts = 0;
    // The abort source the last sink was made with. A store needs one to cancel
    // an upload on, so the writer must not pass nullptr.
    abort_source* sink_abort_source = nullptr;

    bool has_object(const std::string& name) const {
        return objects.contains(name);
    }
    const std::string& stored(const std::string& name) const {
        auto it = objects.find(name);
        BOOST_REQUIRE_MESSAGE(it != objects.end(), fmt::format("no object named {} in the store", name));
        return it->second;
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
        if (_store.on_finalized) {
            _store.on_finalized();
        }
    }
    bool aborted() const {
        return _as && _as->abort_requested();
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
            _pending.append(buf.get(), buf.size());
        }
        return make_ready_future<>();
    }
    future<> flush() override {
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
        if (_store.fail_close) {
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
        return max_ledger_object_size;
    }
};

// Only make_upload_sink() is implemented: every other member throws, so a test
// fails loudly if the code under test reaches for a different route - a
// multipart upload among them.
class fake_client final : public sstables::object_storage_client {
    fake_store& _store;

    static std::logic_error unused(const char* what) {
        return std::logic_error(fmt::format("fake_client: {} is not part of the ledger write", what));
    }

public:
    explicit fake_client(fake_store& store)
        : _store(store) {
    }
    data_sink make_upload_sink(sstables::object_name name, sstables::object_storage_attributes attributes, abort_source* as) override {
        BOOST_REQUIRE_MESSAGE(attributes.empty(), "a ledger entry carries no attributes");
        return data_sink(std::make_unique<fake_sink_impl>(_store, std::string(name.object()), as));
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

segment_entry make_entry(uint64_t sequence = 42) {
    segment_entry e;
    e.chain = test_chain;
    e.sequence = sequence;
    e.boot_epoch_us = 1762300000000000LL;
    e.segment_id = 1762300123456789ULL;
    e.prev_segment_id = sequence == 0 ? 0 : 1762300123456788ULL;
    e.used_size = 33554432;
    e.segment_crc32 = 0xdeadbeef;
    e.upload_time_us = 1762300200000001LL;
    e.covered_through_us = 1762300199000000LL;
    e.chain_start_us = 1762300001000000LL;
    e.host_id = test_host_id;
    e.shard = 5;
    e.file_name = "CommitLog-2-1762300123456789.log";
    return e;
}

// Drives write_entry() over one entry and accounts for what the store saw.
struct writer_fixture {
    fake_store store;
    fake_client client{store};
    // What the unit reported about each failed attempt, in order.
    std::vector<std::exception_ptr> failures;
    std::vector<std::chrono::milliseconds> backoffs;
    // Called after each failure is reported, to abort or otherwise interfere.
    std::function<void()> on_failure;
    // Short enough that even an unbounded sequence of waits stays in the
    // milliseconds, since what is checked is the sequence, not its duration.
    std::chrono::milliseconds base_backoff{1};
    std::chrono::milliseconds max_backoff{4};
    // The loop is unbounded by design, so every test bounds it: the guard
    // aborts once a run is plainly not going to stop.
    unsigned max_attempts = 50;
    abort_source as;

    ledger_retry_policy policy() {
        return ledger_retry_policy{base_backoff, max_backoff, [this](std::exception_ptr ex, std::chrono::milliseconds wait) {
                                       failures.push_back(std::move(ex));
                                       backoffs.push_back(wait);
                                       if (failures.size() > max_attempts) {
                                           as.request_abort_ex(too_many_attempts());
                                       } else if (on_failure) {
                                           on_failure();
                                       }
                                   }};
    }

    future<> write(const segment_entry& e) {
        return write_entry(client, bucket, key_prefix, e, policy(), as);
    }
};

std::string key_of(const segment_entry& e) {
    const chain_key_parts chain{key_prefix, e.host_id, e.shard, e.boot_epoch_us, e.chain_start_us, e.chain};
    return std::string(entry_object_key(chain, e.sequence));
}

std::string encoded(const segment_entry& e) {
    const auto b = encode(e);
    return std::string(reinterpret_cast<const char*>(b.data()), b.size());
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_the_entry_lands_at_its_key_as_exactly_its_encoding) {
    for (const auto kind : both_kinds) {
        writer_fixture fx;
        fx.store.kind = kind;
        const auto e = make_entry();
        fx.write(e).get();
        BOOST_REQUIRE_EQUAL(fx.store.objects.size(), 1u);
        const auto& stored = fx.store.stored(key_of(e));
        BOOST_REQUIRE_EQUAL(stored.size(), encoded(e).size());
        BOOST_REQUIRE(stored == encoded(e));
        // The reader's side of the same contract: the object decodes back.
        const auto round_tripped = decode_segment_entry(bytes_view(reinterpret_cast<const int8_t*>(stored.data()), stored.size()));
        BOOST_REQUIRE(round_tripped == e);
        BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 1u);
        BOOST_REQUIRE(fx.backoffs.empty());
    }
}

SEASTAR_THREAD_TEST_CASE(test_an_entry_goes_in_a_single_put) {
    // A 4 KiB cap is one request: a second put would mean the writer chunked
    // what the format promises never needs chunking.
    writer_fixture fx;
    fx.write(make_entry()).get();
    BOOST_REQUIRE_EQUAL(fx.store.puts, 1u);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 1u);
}

SEASTAR_THREAD_TEST_CASE(test_each_chain_and_sequence_gets_its_own_key) {
    writer_fixture fx;
    auto first = make_entry(0);
    auto second = make_entry(1);
    auto other_chain = make_entry(0);
    other_chain.chain = chain_id(utils::UUID("33334444-0000-4000-8000-00000000000b"));
    fx.write(first).get();
    fx.write(second).get();
    fx.write(other_chain).get();
    BOOST_REQUIRE_EQUAL(fx.store.objects.size(), 3u);
    BOOST_REQUIRE(fx.store.has_object(key_of(first)));
    BOOST_REQUIRE(fx.store.has_object(key_of(second)));
    BOOST_REQUIRE(fx.store.has_object(key_of(other_chain)));
}

SEASTAR_THREAD_TEST_CASE(test_a_failed_attempt_is_retried_and_reported) {
    for (const auto kind : both_kinds) {
        writer_fixture fx;
        fx.store.kind = kind;
        fx.store.fail_first_sinks = 3;
        const auto e = make_entry();
        fx.write(e).get();
        BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 4u);
        BOOST_REQUIRE_EQUAL(fx.store.sinks_closed, 4u);
        // The failed attempts left nothing of their own: one object, whole.
        BOOST_REQUIRE_EQUAL(fx.store.objects.size(), 1u);
        BOOST_REQUIRE(fx.store.stored(key_of(e)) == encoded(e));
        BOOST_REQUIRE_EQUAL(fx.failures.size(), 3u);
        for (const auto& ex : fx.failures) {
            BOOST_REQUIRE_THROW(std::rethrow_exception(ex), store_error);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_a_failed_write_leaves_no_partial_object) {
    // The GCS-style sink finalizes from close(), so only an abort before the
    // close keeps a failed write from becoming an entry that is not an entry.
    for (const auto kind : both_kinds) {
        writer_fixture fx;
        fx.store.kind = kind;
        fx.store.fail_flush = true;
        fx.max_attempts = 3;
        const auto e = make_entry();
        BOOST_REQUIRE_THROW(fx.write(e).get(), too_many_attempts);
        BOOST_REQUIRE(fx.store.objects.empty());
        BOOST_REQUIRE_EQUAL(fx.store.sinks_closed, fx.store.sinks_created);
        BOOST_REQUIRE(fx.store.sink_abort_source != nullptr);
    }
}

SEASTAR_THREAD_TEST_CASE(test_a_close_that_fails_is_the_writes_failure) {
    // On GCS the finalizing request surfaces only from close(), so a close
    // error is the only error a write may get - and it must be retried.
    writer_fixture fx;
    fx.store.kind = store_kind::finalize_on_close;
    fx.store.fail_close = true;
    fx.max_attempts = 2;
    BOOST_REQUIRE_THROW(fx.write(make_entry()).get(), too_many_attempts);
    BOOST_REQUIRE(fx.store.objects.empty());
    BOOST_REQUIRE_EQUAL(fx.failures.size(), 3u);
    BOOST_REQUIRE_THROW(std::rethrow_exception(fx.failures.front()), store_error);
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_during_a_retry_ends_the_write_as_the_abort) {
    writer_fixture fx;
    // Long enough that only an abortable wait can end this test: a backoff that
    // ignores the abort would leave it waiting out the hour.
    fx.base_backoff = std::chrono::hours(1);
    fx.max_backoff = std::chrono::hours(1);
    // Every attempt fails, so only the abort can end the loop.
    fx.store.fail_first_sinks = 1000;
    // The abort is scheduled, not raised here: this callback runs before the
    // backoff's sleep starts, and a yield lands the abort while the sleep is
    // pending, which is where a write waiting out an outage is aborted.
    future<> aborter = make_ready_future<>();
    fx.on_failure = [&fx, &aborter] {
        aborter = seastar::yield().then([&fx] {
            fx.as.request_abort_ex(caller_cancelled());
        });
    };
    // The aborter is resolved before any check can fail and leave it running.
    auto written = fx.write(make_entry());
    written.wait();
    aborter.get();
    BOOST_REQUIRE_THROW(written.get(), caller_cancelled);
    BOOST_REQUIRE_EQUAL(fx.backoffs.size(), 1u);
    BOOST_REQUIRE(fx.backoffs.front() == std::chrono::hours(1));
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 1u);
    BOOST_REQUIRE(fx.store.objects.empty());
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_after_a_successful_put_still_ends_as_the_abort) {
    // The object is in the store and the put reported success, but a caller
    // being shut down or rolled back must not be told it succeeded. Aborting
    // from the call that finalizes puts both kinds on this path: on either one
    // the write then ends without an error of its own.
    for (const auto kind : both_kinds) {
        writer_fixture fx;
        fx.store.kind = kind;
        const auto e = make_entry();
        fx.store.on_finalized = [&fx] {
            fx.as.request_abort_ex(caller_cancelled());
        };
        BOOST_REQUIRE_THROW(fx.write(e).get(), caller_cancelled);
        BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 1u);
        BOOST_REQUIRE(fx.backoffs.empty());
        // The entry the caller will never record is there, whole: a reader that
        // meets it has the entry it would have had.
        BOOST_REQUIRE(fx.store.stored(key_of(e)) == encoded(e));
    }
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_while_the_sink_is_closing_ends_the_write_as_the_abort) {
    // The GCS-style sink finalizes from close(), so an abort that lands there
    // fails the close instead: a different path to the same answer.
    writer_fixture fx;
    fx.store.kind = store_kind::finalize_on_close;
    fx.store.on_close = [&fx] {
        fx.as.request_abort_ex(caller_cancelled());
    };
    BOOST_REQUIRE_THROW(fx.write(make_entry()).get(), caller_cancelled);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 1u);
    BOOST_REQUIRE(fx.store.objects.empty());
    BOOST_REQUIRE(fx.backoffs.empty());
}

SEASTAR_THREAD_TEST_CASE(test_the_callers_abort_reaches_the_sinks_abort_source) {
    // Without that, a shutdown waits for the store request in flight, including
    // the real client's own retries inside it.
    for (const auto kind : both_kinds) {
        writer_fixture fx;
        fx.store.kind = kind;
        bool sink_saw_the_abort = false;
        fx.store.on_put = [&](unsigned) {
            fx.as.request_abort_ex(caller_cancelled());
            sink_saw_the_abort = fx.store.sink_abort_source->abort_requested();
        };
        BOOST_REQUIRE_THROW(fx.write(make_entry()).get(), caller_cancelled);
        BOOST_REQUIRE(sink_saw_the_abort);
    }
}

SEASTAR_THREAD_TEST_CASE(test_an_abort_during_the_put_ends_the_write_as_the_abort) {
    writer_fixture fx;
    fx.store.on_put = [&fx](unsigned) {
        fx.as.request_abort_ex(caller_cancelled());
    };
    BOOST_REQUIRE_THROW(fx.write(make_entry()).get(), caller_cancelled);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 1u);
    BOOST_REQUIRE(fx.backoffs.empty());
}

SEASTAR_THREAD_TEST_CASE(test_a_pre_aborted_source_makes_no_sink_at_all) {
    writer_fixture fx;
    fx.as.request_abort_ex(caller_cancelled());
    BOOST_REQUIRE_THROW(fx.write(make_entry()).get(), caller_cancelled);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 0u);
    BOOST_REQUIRE(fx.store.objects.empty());
}

SEASTAR_THREAD_TEST_CASE(test_a_rewrite_after_a_lost_reply_leaves_one_identical_object) {
    // The store kept the object and the reply never arrived, so the caller
    // writes the same entry again. The bytes are a pure function of the entry,
    // so the second write is the first one's bytes at the first one's key.
    writer_fixture fx;
    const auto e = make_entry();
    fx.write(e).get();
    const auto after_first = fx.store.stored(key_of(e));
    fx.write(e).get();
    BOOST_REQUIRE_EQUAL(fx.store.objects.size(), 1u);
    BOOST_REQUIRE(fx.store.stored(key_of(e)) == after_first);
    BOOST_REQUIRE(fx.store.stored(key_of(e)) == encoded(e));
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 2u);
}

SEASTAR_THREAD_TEST_CASE(test_the_backoff_doubles_up_to_its_bound) {
    writer_fixture fx;
    fx.store.fail_first_sinks = 5;
    fx.write(make_entry()).get();
    using ms = std::chrono::milliseconds;
    const std::vector<ms> expected{ms(1), ms(2), ms(4), ms(4), ms(4)};
    BOOST_REQUIRE(fx.backoffs == expected);
}

SEASTAR_THREAD_TEST_CASE(test_a_base_backoff_below_a_millisecond_is_raised) {
    // A store that fails without a round trip would otherwise be retried in a
    // spin, which on one shard is an outage of its own.
    writer_fixture fx;
    fx.base_backoff = std::chrono::milliseconds(0);
    fx.max_backoff = std::chrono::milliseconds(0);
    fx.store.fail_first_sinks = 3;
    fx.write(make_entry()).get();
    using ms = std::chrono::milliseconds;
    const std::vector<ms> expected{ms(1), ms(1), ms(1)};
    BOOST_REQUIRE(fx.backoffs == expected);
}

SEASTAR_THREAD_TEST_CASE(test_an_entry_no_writer_may_emit_is_refused_without_a_store_call) {
    // encode() refuses it, and no retry would ever clear it, so nothing is sent.
    writer_fixture fx;
    auto e = make_entry();
    e.file_name = "../other-chain/CommitLog-2-1.log";
    BOOST_REQUIRE_THROW(fx.write(e).get(), ledger_format_error);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 0u);
    BOOST_REQUIRE(fx.backoffs.empty());
}

SEASTAR_THREAD_TEST_CASE(test_a_prefix_no_key_builder_accepts_is_refused_without_a_store_call) {
    writer_fixture fx;
    const auto e = make_entry();
    BOOST_REQUIRE_THROW(write_entry(fx.client, bucket, "/pitr/site-a", e, fx.policy(), fx.as).get(), ledger_format_error);
    BOOST_REQUIRE_EQUAL(fx.store.sinks_created, 0u);
}
