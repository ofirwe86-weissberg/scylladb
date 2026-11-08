/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the PITR chain check (issue #15): walking one chain's ledger
// entries in the store, proving the chain holds together, and deciding what a
// restore to T may replay from it.
//
// The store is a fake object_storage_client holding the entry objects a
// shipper would have written. It serves a download of an object it holds, says
// an object it does not hold is absent, and can fail the read of an object it
// does hold, which is how a store error is told from the end of a chain. A
// miss is reported as S3 or GCS report it, or as an error that does not say.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <algorithm>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <boost/test/unit_test.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/later.hh>

#include <seastar/util/file.hh>

#include "db/config.hh"
#include "db/pitr/bound_file.hh"
#include "db/pitr/chain_check.hh"
#include "db/pitr/ledger_entry.hh"
#include "db/pitr/replay_to.hh"
#include "sstables/object_storage_client.hh"
#include "test/lib/tmpdir.hh"
#include "utils/crc.hh"
#include "utils/exceptions.hh"
#include "utils/memory_data_sink.hh"
#include "utils/s3/aws_error.hh"
#include "utils/upload_progress.hh"

using namespace db::pitr;

namespace {

constexpr const char* bucket = "test-bucket";
constexpr const char* key_prefix = "pitr/site-a";

const utils::UUID test_host_id{"a1b2c3d4-0000-4000-8000-00000000000f"};
const chain_id test_chain{utils::UUID("11112222-0000-4000-8000-00000000000a")};
const chain_id other_chain{utils::UUID("11112222-0000-4000-8000-00000000000b")};

constexpr uint32_t test_shard = 3;
constexpr int64_t test_boot_epoch_us = 1762300000000000LL;
constexpr int64_t test_chain_start_us = 1762300001000000LL;
// The first segment the chain shipped, and the step between the segments after
// it: segment ids have gaps of their own, so the test leaves some.
constexpr uint64_t first_segment_id = 1000;
constexpr uint64_t segment_id_step = 7;
// Entry 0's coverage, and the step each later entry adds.
constexpr int64_t first_covered_us = 1762300100000000LL;
constexpr int64_t covered_step_us = 1000000;

// A read of an object the store does not hold reaches a caller as a store
// error, the same way the real clients report a 404 on a download.
class store_error : public std::runtime_error {
public:
    explicit store_error(const std::string& what)
        : std::runtime_error(what) {
    }
};

// Serves one object's bytes, then end of stream.
class string_source_impl final : public data_source_impl {
    std::string _body;
    bool _done = false;

public:
    explicit string_source_impl(std::string body)
        : _body(std::move(body)) {
    }
    future<temporary_buffer<char>> get() override {
        if (_done) {
            return make_ready_future<temporary_buffer<char>>();
        }
        _done = true;
        return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>(_body.data(), _body.size()));
    }
};

// Fails the read instead of serving it, which is what a store under an outage
// does to an object it holds.
class failing_source_impl final : public data_source_impl {
    std::string _name;

public:
    explicit failing_source_impl(std::string name)
        : _name(std::move(name)) {
    }
    future<temporary_buffer<char>> get() override {
        return make_exception_future<temporary_buffer<char>>(store_error(fmt::format("fake store: reading {} failed", _name)));
    }
};

// How the store reports the download of an object it does not hold.
enum class miss_report {
    // A store error that does not say why, which only the store's answer to
    // object_exists() can tell from an outage.
    unclassified,
    // What S3 answers a GET of an absent key.
    s3_no_such_key,
    // What the GCS client turns a 404 into.
    gcs_not_found,
};

// Fails the read the way the store reports an object it does not hold.
class missing_source_impl final : public data_source_impl {
    std::string _name;
    miss_report _report;

public:
    missing_source_impl(std::string name, miss_report report)
        : _name(std::move(name))
        , _report(report) {
    }
    future<temporary_buffer<char>> get() override {
        switch (_report) {
        case miss_report::unclassified:
            break;
        case miss_report::s3_no_such_key:
            return make_exception_future<temporary_buffer<char>>(aws::aws_exception(
                    aws::aws_error(aws::aws_error_type::NO_SUCH_KEY, fmt::format("fake store: no key {}", _name), utils::http::retryable::no)));
        case miss_report::gcs_not_found:
            return make_exception_future<temporary_buffer<char>>(storage_io_error(ENOENT, fmt::format("fake store: no object {}", _name)));
        }
        return make_exception_future<temporary_buffer<char>>(store_error(fmt::format("fake store: reading {} failed", _name)));
    }
};

struct fake_store {
    std::map<std::string, std::string> objects;
    miss_report misses = miss_report::unclassified;
    // Objects a shipper appends right after a read found them absent, before
    // the reader can ask the store about them: the race of #131.
    std::map<std::string, std::string> appended_after_miss;
    // Objects the store holds but will not serve.
    std::set<std::string> fail_reads;
    // Every key a download was made for, in order.
    std::vector<std::string> reads;
    // Every key the store was asked to confirm, which is how the check tells a
    // read error from an absent object.
    std::vector<std::string> exists_calls;
    // Called with each key a download is made for, before it is served.
    std::function<void(const std::string&)> on_read;

    void put(const std::string& key, std::string body) {
        objects[key] = std::move(body);
    }
};

// Only the two calls the chain check may make are implemented; every other
// member throws, so a test fails loudly if the check reaches for another route,
// a bucket listing among them.
class fake_client final : public sstables::object_storage_client {
    fake_store& _store;

    static std::logic_error unused(const char* what) {
        return std::logic_error(fmt::format("fake_client: {} is not part of the chain check", what));
    }

public:
    explicit fake_client(fake_store& store)
        : _store(store) {
    }
    data_source make_download_source(sstables::object_name name, abort_source*) override {
        auto key = std::string(name.object());
        _store.reads.push_back(key);
        if (_store.on_read) {
            _store.on_read(key);
        }
        if (_store.fail_reads.contains(key)) {
            return data_source(std::make_unique<failing_source_impl>(std::move(key)));
        }
        auto it = _store.objects.find(key);
        if (it == _store.objects.end()) {
            if (auto appended = _store.appended_after_miss.extract(key)) {
                _store.objects.insert(std::move(appended));
            }
            return data_source(std::make_unique<missing_source_impl>(std::move(key), _store.misses));
        }
        return data_source(std::make_unique<string_source_impl>(it->second));
    }
    future<bool> object_exists(sstables::object_name name, abort_source*) override {
        auto key = std::string(name.object());
        _store.exists_calls.push_back(key);
        return make_ready_future<bool>(_store.objects.contains(key));
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
    data_sink make_upload_sink(sstables::object_name, sstables::object_storage_attributes, abort_source*) override {
        throw unused("make_upload_sink");
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

chain_key_parts test_chain_key(chain_id chain = test_chain) {
    return chain_key_parts{
            .prefix = key_prefix,
            .host_id = test_host_id,
            .shard = test_shard,
            .boot_epoch_us = test_boot_epoch_us,
            .chain_start_us = test_chain_start_us,
            .chain = chain,
    };
}

uint64_t segment_id_of(uint64_t sequence) {
    return first_segment_id + sequence * segment_id_step;
}

int64_t covered_us_of(uint64_t sequence) {
    return first_covered_us + int64_t(sequence) * covered_step_us;
}

// The entry a healthy shipper would have written at this sequence.
segment_entry make_entry(uint64_t sequence) {
    segment_entry e;
    e.chain = test_chain;
    e.sequence = sequence;
    e.boot_epoch_us = test_boot_epoch_us;
    e.segment_id = segment_id_of(sequence);
    e.prev_segment_id = sequence == 0 ? 0 : segment_id_of(sequence - 1);
    e.used_size = 4096 + sequence;
    // Per entry, so that a checksum carried out of order, or a constant one,
    // does not pass.
    e.segment_crc32 = 0xdeadbeef ^ uint32_t(sequence);
    e.upload_time_us = covered_us_of(sequence) + 1000;
    e.covered_through_us = covered_us_of(sequence);
    e.chain_start_us = test_chain_start_us;
    e.host_id = test_host_id;
    e.shard = test_shard;
    e.file_name = fmt::format("CommitLog-2-{}.log", segment_id_of(sequence));
    return e;
}

// The bound file's record for the shard, with the backup's bound inside the
// chain's first segment.
chain_bound make_bound(uint64_t bound_segment_id = first_segment_id) {
    return chain_bound{
            .host_id = test_host_id,
            .shard = test_shard,
            .boot_epoch_us = test_boot_epoch_us,
            .chain_start_us = test_chain_start_us,
            .chain = test_chain,
            .bound_segment_id = bound_segment_id,
    };
}

// Puts one entry in the store under a key of the test's choosing, so that a
// test can write a body that does not match its key.
void store_entry(fake_store& store, const segment_entry& entry, uint64_t key_sequence, chain_id key_chain = test_chain) {
    const auto body = encode(entry);
    store.put(entry_object_key(test_chain_key(key_chain), key_sequence), std::string(reinterpret_cast<const char*>(body.data()), body.size()));
}

// A chain of `count` healthy entries, sequences 0 to count-1.
void store_chain(fake_store& store, uint64_t count) {
    for (uint64_t sequence = 0; sequence < count; ++sequence) {
        store_entry(store, make_entry(sequence), sequence);
    }
}

// A chain whose sequences are dense and whose segment ids are the ones given,
// which is what a shipper that skipped a segment leaves behind: the ids come
// from `++_ids` and skip reserve and recycled segments, so a hole in them is
// invisible to the sequence numbers.
void store_chain_of_ids(fake_store& store, const std::vector<uint64_t>& segment_ids) {
    for (uint64_t sequence = 0; sequence < segment_ids.size(); ++sequence) {
        auto entry = make_entry(sequence);
        entry.segment_id = segment_ids[sequence];
        entry.prev_segment_id = sequence == 0 ? 0 : segment_ids[sequence - 1];
        entry.file_name = fmt::format("CommitLog-2-{}.log", segment_ids[sequence]);
        store_entry(store, entry, sequence);
    }
}

future<std::expected<checked_chain, refusal>> check(fake_store& store, int64_t target_time_us, chain_bound bound = make_bound()) {
    fake_client client(store);
    abort_source as;
    co_return co_await check_chain(client, bucket, key_prefix, bound, target_time_us, as);
}

std::vector<sstring> segment_paths(const checked_chain& chain) {
    std::vector<sstring> paths;
    for (const auto& s : chain.segments) {
        paths.push_back(s.path);
    }
    return paths;
}

// The refusal a check is expected to have ended in, with its code.
const refusal& refused(const std::expected<checked_chain, refusal>& result, refusal_reason reason) {
    BOOST_REQUIRE_MESSAGE(!result.has_value(), "the check accepted the chain where a refusal was expected");
    BOOST_REQUIRE_EQUAL(result.error().code(), reason_code(reason));
    return result.error();
}

const checked_chain& accepted(const std::expected<checked_chain, refusal>& result) {
    if (!result.has_value()) {
        BOOST_FAIL(fmt::format("the check refused the chain: {}", result.error()));
    }
    return *result;
}

// The whole bound file, one shard at a time. Each shard ships a chain of its
// own, as a real node does: the shard is part of every key, so a check that
// read shard 0's chain twice would show up as the wrong keys.

// The target a replay fetches for; the restore directory is named after it.
const ::table_id test_target{utils::UUID("55556666-0000-4000-8000-00000000000c")};

constexpr int64_t test_capture_time_us = test_chain_start_us + 10000;

chain_id chain_of_shard(uint32_t shard) {
    return chain_id{utils::UUID(std::string_view(fmt::format("11112222-0000-4000-8000-00000000{:04x}", 0x100 + shard)))};
}

chain_key_parts shard_chain_key(uint32_t shard) {
    auto key = test_chain_key(chain_of_shard(shard));
    key.shard = shard;
    return key;
}

// `coverage` is what its last entry carries, and the entries below it step down
// from it, so one shard's coverage is set by the test alone.
void store_shard_chain(fake_store& store, uint32_t shard, uint64_t count, int64_t coverage) {
    for (uint64_t sequence = 0; sequence < count; ++sequence) {
        auto entry = make_entry(sequence);
        entry.shard = shard;
        entry.chain = chain_of_shard(shard);
        entry.covered_through_us = coverage - int64_t(count - 1 - sequence) * covered_step_us;
        entry.upload_time_us = entry.covered_through_us + 1000;
        const auto body = encode(entry);
        store.put(entry_object_key(shard_chain_key(shard), sequence), std::string(reinterpret_cast<const char*>(body.data()), body.size()));
    }
}

shard_bound bound_row(uint32_t shard, bool with_chain = true) {
    shard_bound row;
    row.shard = shard;
    row.bound = db::replay_position(first_segment_id, 1);
    if (with_chain) {
        row.chain = db::pitr::shard_chain{
                .host_id = test_host_id,
                .boot_epoch_us = test_boot_epoch_us,
                .chain_start_us = test_chain_start_us,
                .chain = chain_of_shard(shard),
                .floor_segment_id = first_segment_id,
        };
    }
    return row;
}

bound_file bound_of(std::vector<shard_bound> shards) {
    bound_file bound;
    bound.capture_time_us = test_capture_time_us;
    bound.shards = std::move(shards);
    return bound;
}

future<std::expected<checked_history, refusal>> check_all(fake_store& store, const bound_file& bound, int64_t target_time_us) {
    fake_client client(store);
    abort_source as;
    co_return co_await check_bound_chains(client, bucket, key_prefix, bound, target_time_us, as);
}

const refusal& refused_history(const std::expected<checked_history, refusal>& result, refusal_reason reason) {
    BOOST_REQUIRE_MESSAGE(!result.has_value(), "the check accepted the bound where a refusal was expected");
    BOOST_REQUIRE_EQUAL(result.error().code(), reason_code(reason));
    return result.error();
}

const checked_history& accepted_history(const std::expected<checked_history, refusal>& result) {
    if (!result.has_value()) {
        BOOST_FAIL(fmt::format("the check refused the bound: {}", result.error()));
    }
    return *result;
}

} // namespace

SEASTAR_TEST_CASE(test_one_entry_chain_is_accepted) {
    fake_store store;
    store_chain(store, 1);
    const auto result = co_await check(store, covered_us_of(0) - 1);
    const auto& chain = accepted(result);
    BOOST_REQUIRE_EQUAL(chain.entries_read, 1);
    BOOST_REQUIRE_EQUAL(chain.covered_through_us, covered_us_of(0));
    BOOST_REQUIRE_EQUAL(chain.segments.size(), 1);
    BOOST_REQUIRE_EQUAL(chain.segments[0].path, segment_object_key(test_chain_key(), make_entry(0).file_name));
    BOOST_REQUIRE_EQUAL(chain.segments[0].used_size, make_entry(0).used_size);
    // The checksum the fetch verifies the downloaded segment against.
    BOOST_REQUIRE_EQUAL(chain.segment_crc32s.size(), 1);
    BOOST_REQUIRE_EQUAL(chain.segment_crc32s[0], make_entry(0).segment_crc32);
}

SEASTAR_TEST_CASE(test_segments_are_listed_in_chain_order) {
    fake_store store;
    store_chain(store, 3);
    const auto result = co_await check(store, covered_us_of(2));
    const auto& chain = accepted(result);
    BOOST_REQUIRE_EQUAL(chain.entries_read, 3);
    BOOST_REQUIRE_EQUAL(chain.covered_through_us, covered_us_of(2));
    const std::vector<sstring> expected{
            segment_object_key(test_chain_key(), make_entry(0).file_name),
            segment_object_key(test_chain_key(), make_entry(1).file_name),
            segment_object_key(test_chain_key(), make_entry(2).file_name),
    };
    const auto paths = segment_paths(chain);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(paths.begin(), paths.end(), expected.begin(), expected.end());
    // One checksum per segment, each the one its own entry recorded.
    BOOST_REQUIRE_EQUAL(chain.segment_crc32s.size(), chain.segments.size());
    for (uint64_t sequence = 0; sequence < chain.segments.size(); ++sequence) {
        BOOST_REQUIRE_EQUAL(chain.segment_crc32s[sequence], make_entry(sequence).segment_crc32);
    }
}

// P43: replay runs the chain to its end and the T filter drops the writes above
// T, so a T inside the chain takes nothing off the list.
SEASTAR_TEST_CASE(test_a_t_below_the_chain_end_still_returns_every_segment) {
    fake_store store;
    store_chain(store, 5);
    // T sits inside entry 2's interval, with two entries above it.
    const auto result = co_await check(store, covered_us_of(2) - 1);
    const auto& chain = accepted(result);
    BOOST_REQUIRE_EQUAL(chain.entries_read, 5);
    BOOST_REQUIRE_EQUAL(chain.segments.size(), 5);
    BOOST_REQUIRE_EQUAL(chain.covered_through_us, covered_us_of(4));
    BOOST_REQUIRE_EQUAL(chain.segments[4].path, segment_object_key(test_chain_key(), make_entry(4).file_name));
}

SEASTAR_TEST_CASE(test_t_exactly_at_the_chain_coverage_is_accepted) {
    fake_store store;
    store_chain(store, 3);
    const auto result = co_await check(store, covered_us_of(2));
    const auto& chain = accepted(result);
    BOOST_REQUIRE_EQUAL(chain.segments.size(), 3);
    BOOST_REQUIRE_EQUAL(chain.covered_through_us, covered_us_of(2));
}

SEASTAR_TEST_CASE(test_a_chain_with_no_entries_is_refused) {
    fake_store store;
    const auto result = co_await check(store, covered_us_of(0));
    const auto& r = refused(result, refusal_reason::protection_records_missing);
    BOOST_REQUIRE(r.detail.find(chain_key_prefix(test_chain_key())) != sstring::npos);
}

SEASTAR_TEST_CASE(test_a_missing_entry_ends_the_chain_before_it) {
    fake_store store;
    store_entry(store, make_entry(0), 0);
    store_entry(store, make_entry(1), 1);
    // Sequence 2 is absent, so the entries above it are out of reach and the
    // coverage the chain can prove is entry 1's.
    store_entry(store, make_entry(3), 3);
    const auto result = co_await check(store, covered_us_of(1));
    const auto& chain = accepted(result);
    BOOST_REQUIRE_EQUAL(chain.entries_read, 2);
    BOOST_REQUIRE_EQUAL(chain.covered_through_us, covered_us_of(1));
    BOOST_REQUIRE_EQUAL(chain.segments.size(), 2);
}

// REF-4, #103: the ledger can miss the segment that was open when the snapshot
// was taken, which is the one holding the writes the backup's sstables do not.
// The sequence numbers stay dense over the hole, so only the bound's own id
// shows it.
SEASTAR_TEST_CASE(test_a_chain_without_the_bounds_own_segment_is_refused) {
    fake_store store;
    store_chain_of_ids(store, {segment_id_of(0), segment_id_of(1), segment_id_of(3), segment_id_of(4)});
    const auto result = co_await check(store, covered_us_of(3), make_bound(segment_id_of(2)));
    const auto& r = refused(result, refusal_reason::protection_records_missing);
    // The shard and the id, because the user's next step is to look for that
    // segment of that shard, or to restore from a later backup.
    BOOST_REQUIRE(r.detail.find(fmt::format("shard {}", test_shard)) != sstring::npos);
    BOOST_REQUIRE(r.detail.find(fmt::format("{}", segment_id_of(2))) != sstring::npos);
}

SEASTAR_TEST_CASE(test_a_chain_holding_the_bounds_own_segment_is_accepted) {
    fake_store store;
    store_chain_of_ids(store, {segment_id_of(0), segment_id_of(1), segment_id_of(2), segment_id_of(3)});
    const auto result = co_await check(store, covered_us_of(3), make_bound(segment_id_of(2)));
    BOOST_REQUIRE_EQUAL(accepted(result).segments.size(), 4);
}

SEASTAR_TEST_CASE(test_a_chain_that_skips_an_id_above_the_bound_is_accepted) {
    fake_store store;
    // A reserve or recycled segment takes an id and is never written to, so a
    // hole anywhere but at the bound is what a healthy chain looks like.
    store_chain_of_ids(store, {segment_id_of(0), segment_id_of(1), segment_id_of(3)});
    const auto result = co_await check(store, covered_us_of(2), make_bound(segment_id_of(1)));
    BOOST_REQUIRE_EQUAL(accepted(result).segments.size(), 3);
}

SEASTAR_TEST_CASE(test_a_chain_wholly_below_the_bound_is_left_to_the_coverage_rule) {
    fake_store store;
    store_chain(store, 3);
    // Nothing after the snapshot was shipped, so there is no segment to name:
    // what the chain cannot cover is the coverage rule's answer, and its
    // interval is what tells the user where to retry.
    const auto result = co_await check(store, covered_us_of(2) + 1, make_bound(segment_id_of(5)));
    const auto& r = refused(result, refusal_reason::t_past_coverage);
    BOOST_REQUIRE(r.interval.has_value());
    BOOST_REQUIRE_EQUAL(r.interval->through_us, covered_us_of(2));
}

SEASTAR_TEST_CASE(test_a_t_past_a_shortened_chain_is_refused) {
    fake_store store;
    store_entry(store, make_entry(0), 0);
    store_entry(store, make_entry(3), 3);
    const auto result = co_await check(store, covered_us_of(1));
    const auto& r = refused(result, refusal_reason::t_past_coverage);
    BOOST_REQUIRE(r.interval.has_value());
    BOOST_REQUIRE_EQUAL(r.interval->through_us, covered_us_of(0));
}

SEASTAR_TEST_CASE(test_a_t_past_the_chain_coverage_is_refused_with_the_interval) {
    fake_store store;
    store_chain(store, 3);
    const auto result = co_await check(store, covered_us_of(2) + 1);
    const auto& r = refused(result, refusal_reason::t_past_coverage);
    BOOST_REQUIRE(r.interval.has_value());
    BOOST_REQUIRE_EQUAL(r.interval->from_us, test_chain_start_us);
    BOOST_REQUIRE_EQUAL(r.interval->through_us, covered_us_of(2));
    // REF-5: the latest restorable time and the lag are both in the message.
    BOOST_REQUIRE(r.message.find("lag") != sstring::npos);
    BOOST_REQUIRE(r.message.find("2025-11-04T23:48:22.000000Z") != sstring::npos);
}

SEASTAR_TEST_CASE(test_the_first_entry_may_not_name_a_previous_segment) {
    fake_store store;
    auto first = make_entry(0);
    first.prev_segment_id = first_segment_id - 1;
    store_entry(store, first, 0);
    const auto result = co_await check(store, covered_us_of(0));
    refused(result, refusal_reason::protection_records_damaged);
}

SEASTAR_TEST_CASE(test_a_broken_link_to_the_entry_before_is_refused) {
    fake_store store;
    store_entry(store, make_entry(0), 0);
    auto second = make_entry(1);
    second.prev_segment_id = first_segment_id + 1;
    store_entry(store, second, 1);
    const auto result = co_await check(store, covered_us_of(0));
    refused(result, refusal_reason::protection_records_damaged);
}

SEASTAR_TEST_CASE(test_a_segment_id_that_does_not_rise_is_refused) {
    fake_store store;
    store_entry(store, make_entry(0), 0);
    auto second = make_entry(1);
    // The link holds, and the id repeats, which no shipped run can do.
    second.segment_id = first_segment_id;
    store_entry(store, second, 1);
    const auto result = co_await check(store, covered_us_of(0));
    refused(result, refusal_reason::protection_records_damaged);
}

SEASTAR_TEST_CASE(test_an_entry_body_must_match_its_key) {
    fake_store store;
    store_entry(store, make_entry(0), 0);
    // The body of sequence 2 under the key of sequence 1: a reader that trusted
    // the key alone would read the chain as dense and miss the entry between.
    store_entry(store, make_entry(2), 1);
    const auto result = co_await check(store, covered_us_of(0));
    const auto& r = refused(result, refusal_reason::protection_records_damaged);
    BOOST_REQUIRE(r.detail.find("sequence") != sstring::npos);
}

SEASTAR_TEST_CASE(test_an_entry_of_another_chain_is_refused) {
    fake_store store;
    store_entry(store, make_entry(0), 0);
    auto second = make_entry(1);
    second.chain = other_chain;
    // Under this chain's key, as a copy between prefixes would leave it.
    store_entry(store, second, 1);
    const auto result = co_await check(store, covered_us_of(0));
    const auto& r = refused(result, refusal_reason::protection_records_damaged);
    BOOST_REQUIRE(r.detail.find("chain") != sstring::npos);
}

SEASTAR_TEST_CASE(test_an_entry_of_another_boot_is_refused) {
    fake_store store;
    store_entry(store, make_entry(0), 0);
    auto second = make_entry(1);
    // A fragment id of a split write restarts at zero with each commitlog, so
    // two boots in one list can reassemble into nonsense.
    second.boot_epoch_us = test_boot_epoch_us + 1;
    store_entry(store, second, 1);
    const auto result = co_await check(store, covered_us_of(0));
    const auto& r = refused(result, refusal_reason::protection_records_damaged);
    BOOST_REQUIRE(r.detail.find("boot epoch") != sstring::npos);
}

SEASTAR_TEST_CASE(test_an_entry_of_another_host_is_refused) {
    fake_store store;
    store_entry(store, make_entry(0), 0);
    auto second = make_entry(1);
    // A chain copied whole under this host's prefix would otherwise be read as
    // coverage this host never had.
    second.host_id = utils::UUID("a1b2c3d4-0000-4000-8000-000000000010");
    store_entry(store, second, 1);
    const auto result = co_await check(store, covered_us_of(0));
    const auto& r = refused(result, refusal_reason::protection_records_damaged);
    BOOST_REQUIRE(r.detail.find("host") != sstring::npos);
}

SEASTAR_TEST_CASE(test_an_entry_of_another_shard_is_refused) {
    fake_store store;
    store_entry(store, make_entry(0), 0);
    auto second = make_entry(1);
    // Each shard reassembles its own split writes, so another shard's segment
    // in this list would decode against the wrong state.
    second.shard = test_shard + 1;
    store_entry(store, second, 1);
    const auto result = co_await check(store, covered_us_of(0));
    const auto& r = refused(result, refusal_reason::protection_records_damaged);
    BOOST_REQUIRE(r.detail.find("shard") != sstring::npos);
}

SEASTAR_TEST_CASE(test_an_entry_of_another_chain_start_is_refused) {
    fake_store store;
    store_entry(store, make_entry(0), 0);
    auto second = make_entry(1);
    // Two chains of one boot differ only in their start, so this is the whole
    // of what separates them.
    second.chain_start_us = test_chain_start_us + 1;
    store_entry(store, second, 1);
    const auto result = co_await check(store, covered_us_of(0));
    const auto& r = refused(result, refusal_reason::protection_records_damaged);
    BOOST_REQUIRE(r.detail.find("chain start") != sstring::npos);
}

SEASTAR_TEST_CASE(test_an_abort_ends_the_walk) {
    fake_store store;
    store_chain(store, 5);
    fake_client client(store);
    abort_source as;
    store.on_read = [&as, &store](const std::string&) {
        if (store.reads.size() == 2) {
            as.request_abort();
        }
    };
    bool aborted = false;
    try {
        [[maybe_unused]] const auto result = co_await check_chain(client, bucket, key_prefix, make_bound(), covered_us_of(4), as);
    } catch (const abort_requested_exception&) {
        aborted = true;
    }
    BOOST_REQUIRE_MESSAGE(aborted, "an abort during the walk did not end it");
    // The entry being read when the abort landed is finished, and the walk
    // stops before reading the next one.
    BOOST_REQUIRE_EQUAL(store.reads.size(), 2);
}

SEASTAR_TEST_CASE(test_a_corrupt_entry_is_refused) {
    fake_store store;
    store_chain(store, 2);
    const auto key = entry_object_key(test_chain_key(), 1);
    auto& body = store.objects[key];
    // One bit in the segment id, which the trailing checksum covers.
    body[40] = char(body[40] ^ 0x01);
    const auto result = co_await check(store, covered_us_of(0));
    const auto& r = refused(result, refusal_reason::protection_records_damaged);
    BOOST_REQUIRE(r.detail.find(key) != sstring::npos);
}

SEASTAR_TEST_CASE(test_a_bound_below_the_chain_floor_is_refused) {
    fake_store store;
    store_chain(store, 2);
    const auto result = co_await check(store, covered_us_of(0), make_bound(first_segment_id - 1));
    const auto& r = refused(result, refusal_reason::backup_before_shipping);
    BOOST_REQUIRE(r.detail.find("bound") != sstring::npos);
}

SEASTAR_TEST_CASE(test_a_bound_at_the_chain_floor_is_accepted) {
    fake_store store;
    store_chain(store, 2);
    const auto result = co_await check(store, covered_us_of(0), make_bound(first_segment_id));
    accepted(result);
}

SEASTAR_TEST_CASE(test_a_failing_read_is_not_the_end_of_the_chain) {
    fake_store store;
    store_chain(store, 3);
    const auto key = entry_object_key(test_chain_key(), 1);
    store.fail_reads.insert(std::string(key));
    // Reading the chain short here would report a coverage the store can
    // better, so the error reaches the caller instead.
    bool threw = false;
    try {
        [[maybe_unused]] const auto result = co_await check(store, covered_us_of(0));
    } catch (const store_error&) {
        threw = true;
    }
    BOOST_REQUIRE_MESSAGE(threw, "a failing read was not reported to the caller");
    // The store was asked, which is what told the error from an absent object.
    BOOST_REQUIRE(std::ranges::find(store.exists_calls, std::string(key)) != store.exists_calls.end());
}

// #131: the shipper appends the entry one past the chain's end between the
// read that finds it absent and any question about it. A store that says the
// key is absent has answered, so the chain ends there and is not an error.
static future<> check_entry_appended_after_its_probe_ends_the_chain(miss_report misses) {
    fake_store store;
    store.misses = misses;
    store_chain(store, 3);
    const auto key = std::string(entry_object_key(test_chain_key(), 3));
    const auto body = encode(make_entry(3));
    store.appended_after_miss[key] = std::string(reinterpret_cast<const char*>(body.data()), body.size());
    const auto result = co_await check(store, covered_us_of(2));
    const auto& chain = accepted(result);
    BOOST_REQUIRE_EQUAL(chain.entries_read, 3);
    BOOST_REQUIRE_EQUAL(chain.segments.size(), 3);
    BOOST_REQUIRE_EQUAL(chain.covered_through_us, covered_us_of(2));
    // The race did happen: the entry is in the store now.
    BOOST_REQUIRE(store.objects.contains(key));
    BOOST_REQUIRE(std::ranges::find(store.exists_calls, key) == store.exists_calls.end());
}

SEASTAR_TEST_CASE(test_an_entry_appended_after_its_s3_probe_ends_the_chain) {
    return check_entry_appended_after_its_probe_ends_the_chain(miss_report::s3_no_such_key);
}

SEASTAR_TEST_CASE(test_an_entry_appended_after_its_gcs_probe_ends_the_chain) {
    return check_entry_appended_after_its_probe_ends_the_chain(miss_report::gcs_not_found);
}

// #131: ending the chain at the probe proves only what the entries read cover,
// so a T the appended entry would have covered is refused, not replayed short.
SEASTAR_TEST_CASE(test_a_t_past_an_entry_appended_after_its_probe_is_refused) {
    fake_store store;
    store.misses = miss_report::s3_no_such_key;
    store_chain(store, 3);
    const auto key = std::string(entry_object_key(test_chain_key(), 3));
    const auto body = encode(make_entry(3));
    store.appended_after_miss[key] = std::string(reinterpret_cast<const char*>(body.data()), body.size());
    const auto result = co_await check(store, covered_us_of(3));
    const auto& r = refused(result, refusal_reason::t_past_coverage);
    BOOST_REQUIRE(r.interval.has_value());
    BOOST_REQUIRE_EQUAL(r.interval->through_us, covered_us_of(2));
}

// #131: an entry missing inside the chain, reported as an absent key, still
// ends the chain before it, so a T that only the entries above it cover is
// refused.
SEASTAR_TEST_CASE(test_an_entry_missing_inside_the_chain_refuses_a_t_past_it) {
    fake_store store;
    store.misses = miss_report::s3_no_such_key;
    store_entry(store, make_entry(0), 0);
    store_entry(store, make_entry(1), 1);
    store_entry(store, make_entry(3), 3);
    store_entry(store, make_entry(4), 4);
    const auto result = co_await check(store, covered_us_of(4));
    const auto& r = refused(result, refusal_reason::t_past_coverage);
    BOOST_REQUIRE(r.interval.has_value());
    BOOST_REQUIRE_EQUAL(r.interval->through_us, covered_us_of(1));
    // Nothing above the hole was read, so nothing above it is trusted.
    BOOST_REQUIRE(std::ranges::find(store.reads, std::string(entry_object_key(test_chain_key(), 3))) == store.reads.end());
}

SEASTAR_TEST_CASE(test_a_long_chain_is_walked_and_yields) {
    fake_store store;
    constexpr uint64_t count = 10000;
    store_chain(store, count);
    fake_client client(store);
    abort_source as;
    auto walking = check_chain(client, bucket, key_prefix, make_bound(), covered_us_of(count - 1), as);
    co_await yield();
    BOOST_REQUIRE_MESSAGE(!walking.available(), "the walk ran to the end of a 10,000-entry chain without giving the shard back");
    const auto result = co_await std::move(walking);
    const auto& chain = accepted(result);
    BOOST_REQUIRE_EQUAL(chain.entries_read, count);
    BOOST_REQUIRE_EQUAL(chain.segments.size(), count);
    BOOST_REQUIRE_EQUAL(chain.segment_crc32s.size(), count);
    BOOST_REQUIRE_EQUAL(chain.covered_through_us, covered_us_of(count - 1));
    // Chunked, so a long chain's list is not one contiguous allocation.
    static_assert(std::is_same_v<decltype(chain.segments), utils::chunked_vector<shipped_segment>>);
}

SEASTAR_TEST_CASE(test_every_refusal_reason_has_a_code_of_its_own) {
    std::set<std::string_view> codes;
    for (const auto reason : all_refusal_reasons) {
        const auto code = reason_code(reason);
        BOOST_REQUIRE_MESSAGE(code != "unknown", fmt::format("a refusal reason has no code: {}", int(reason)));
        BOOST_REQUIRE_MESSAGE(codes.insert(code).second, fmt::format("the code {} is used by more than one reason", code));
    }
    BOOST_REQUIRE_EQUAL(codes.size(), all_refusal_reasons.size());
    co_return;
}

// Every shard of the bound is checked, and the node covers what its least
// covered shard does: a write of any shard can be the one missing above it.
SEASTAR_TEST_CASE(test_every_shard_of_the_bound_is_checked) {
    fake_store store;
    const auto low = covered_us_of(5);
    store_shard_chain(store, 0, 3, low + covered_step_us);
    store_shard_chain(store, 1, 2, low);
    const auto bound = bound_of({bound_row(0), bound_row(1)});
    const auto result = co_await check_all(store, bound, low);
    const auto& history = accepted_history(result);
    BOOST_REQUIRE_EQUAL(history.chains.size(), 2);
    BOOST_REQUIRE_EQUAL(history.covered_through_us, low);
    BOOST_REQUIRE_EQUAL(history.chains[0].entries_read, 3);
    BOOST_REQUIRE_EQUAL(history.chains[1].entries_read, 2);
    // Each shard's own chain, and not shard 0's read twice.
    for (uint32_t shard = 0; shard < 2; ++shard) {
        for (const auto& segment : history.chains[shard].segments) {
            BOOST_REQUIRE_MESSAGE(segment.path.starts_with(chain_key_prefix(shard_chain_key(shard))),
                    fmt::format("shard {} holds a segment of another chain: {}", shard, segment.path));
        }
    }
}

// The lagging shard is the one the user is told about: it is what bounds T, and
// a T at or below its coverage is the one to retry with.
SEASTAR_TEST_CASE(test_the_shard_covering_least_is_the_one_refused) {
    fake_store store;
    const auto low = covered_us_of(5);
    store_shard_chain(store, 0, 2, low + 10 * covered_step_us);
    store_shard_chain(store, 1, 2, low);
    const auto bound = bound_of({bound_row(0), bound_row(1)});
    const auto result = co_await check_all(store, bound, low + 1);
    const auto& r = refused_history(result, refusal_reason::t_past_coverage);
    BOOST_REQUIRE_EQUAL(r.detail, "shard 1");
    BOOST_REQUIRE(r.interval);
    BOOST_REQUIRE_EQUAL(r.interval->through_us, low);
}

// With more than one shard lagging, the one covering least is still the one
// reported: a T the better covered shard allows would be refused again.
SEASTAR_TEST_CASE(test_the_least_covered_of_several_lagging_shards_is_the_one_refused) {
    fake_store store;
    const auto low = covered_us_of(5);
    store_shard_chain(store, 0, 2, low + 2 * covered_step_us);
    store_shard_chain(store, 1, 2, low);
    store_shard_chain(store, 2, 2, low + covered_step_us);
    const auto bound = bound_of({bound_row(0), bound_row(1), bound_row(2)});
    const auto result = co_await check_all(store, bound, low + 3 * covered_step_us);
    const auto& r = refused_history(result, refusal_reason::t_past_coverage);
    BOOST_REQUIRE_EQUAL(r.detail, "shard 1");
    BOOST_REQUIRE_EQUAL(r.interval->through_us, low);
}

// The interval offered starts at the backup and not at the chain: below the
// capture time this backup has nothing to restore to.
SEASTAR_TEST_CASE(test_the_offered_interval_starts_at_the_backup) {
    fake_store store;
    const auto low = covered_us_of(5);
    store_shard_chain(store, 0, 2, low);
    const auto bound = bound_of({bound_row(0)});
    const auto result = co_await check_all(store, bound, low + 1);
    const auto& r = refused_history(result, refusal_reason::t_past_coverage);
    BOOST_REQUIRE(r.interval);
    BOOST_REQUIRE_EQUAL(r.interval->from_us, test_capture_time_us);
    BOOST_REQUIRE_GT(test_capture_time_us, test_chain_start_us);
}

// A shard no lower T would make readable outranks one that only lags: lowering
// T on the advice of a lagging shard would refuse again for the damaged one.
SEASTAR_TEST_CASE(test_a_damaged_chain_outranks_a_lagging_shard) {
    fake_store store;
    const auto low = covered_us_of(5);
    store_shard_chain(store, 0, 2, low);
    store_shard_chain(store, 1, 2, low + 10 * covered_step_us);
    store.put(entry_object_key(shard_chain_key(1), 1), "not a ledger entry at all");
    const auto bound = bound_of({bound_row(0), bound_row(1)});
    const auto result = co_await check_all(store, bound, low + 1);
    const auto& r = refused_history(result, refusal_reason::protection_records_damaged);
    BOOST_REQUIRE(r.detail.find(entry_object_key(shard_chain_key(1), 1)) != sstring::npos);
}

// The bound names the shard, so it was writing when the backup was taken; with
// no chain recorded, nothing says it ever shipped (PM-50).
SEASTAR_TEST_CASE(test_a_shard_the_bound_records_without_a_chain_is_refused) {
    fake_store store;
    const auto low = covered_us_of(5);
    store_shard_chain(store, 0, 2, low);
    const auto bound = bound_of({bound_row(0), bound_row(1, false)});
    const auto result = co_await check_all(store, bound, low);
    const auto& r = refused_history(result, refusal_reason::protection_records_missing);
    BOOST_REQUIRE(r.detail.find("shard 1") != sstring::npos);
}

// The fetch of a checked chain's segments (issue #18, ep.3). It lives here
// because the fake store above is what it needs: a test file of its own waits
// for the next change to configure.py.

// One object in the store, and the entry that describes it.
struct fetchable {
    shipped_segment segment;
    uint32_t crc32 = 0;
    std::string body;
};

fetchable store_segment(fake_store& store, uint32_t shard, std::string body) {
    const auto name = fmt::format("CommitLog-2-{}.log", segment_id_of(0));
    fetchable f{
            .segment = shipped_segment{.path = segment_object_key(shard_chain_key(shard), name), .used_size = body.size()},
            .body = std::move(body),
    };
    utils::crc32 crc;
    crc.process(reinterpret_cast<const uint8_t*>(f.body.data()), f.body.size());
    f.crc32 = crc.get();
    store.put(std::string(f.segment.path), f.body);
    return f;
}

// A file where a run would have left one, written as it is.
future<> write_file(const std::filesystem::path& path, std::string_view body) {
    auto f = co_await seastar::open_file_dma(path.native(), seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate);
    auto out = co_await seastar::make_file_output_stream(std::move(f));
    co_await out.write(body.data(), body.size());
    co_await out.flush();
    co_await out.close();
}

future<std::expected<std::filesystem::path, refusal>> fetch(fake_store& store, const fetchable& f, const std::filesystem::path& dir, uint32_t crc32) {
    fake_client client(store);
    abort_source as;
    co_return co_await fetch_segment(client, bucket, f.segment, crc32, dir, as);
}

SEASTAR_TEST_CASE(test_a_fetched_segment_holds_the_object_s_bytes) {
    fake_store store;
    const auto f = store_segment(store, 0, std::string(100000, 'x') + "tail");
    tmpdir work;
    const auto dir = co_await make_restore_dir(work.path(), test_target);
    const auto fetched = co_await fetch(store, f, dir, f.crc32);
    BOOST_REQUIRE_MESSAGE(fetched.has_value(), fmt::format("the fetch refused a healthy segment: {}", fetched.error()));
    // The source's own file name: replay reads the segment's id and base
    // position out of it.
    BOOST_REQUIRE_EQUAL(fetched->filename().native(), fmt::format("CommitLog-2-{}.log", segment_id_of(0)));
    BOOST_REQUIRE_EQUAL(fetched->parent_path().native(), dir.native());
    const auto landed = co_await seastar::util::read_entire_file_contiguous(*fetched);
    BOOST_REQUIRE_EQUAL(landed.size(), f.body.size());
    BOOST_REQUIRE(landed == f.body);
}

SEASTAR_TEST_CASE(test_a_segment_of_another_size_is_refused_and_removed) {
    fake_store store;
    auto f = store_segment(store, 0, "the bytes the store holds");
    // The record says the segment is longer than what the store holds, which is
    // what a truncated upload leaves behind.
    f.segment.used_size += 1;
    tmpdir work;
    const auto dir = co_await make_restore_dir(work.path(), test_target);
    const auto fetched = co_await fetch(store, f, dir, f.crc32);
    BOOST_REQUIRE_MESSAGE(!fetched.has_value(), "a segment of another size was accepted");
    BOOST_REQUIRE_EQUAL(fetched.error().code(), reason_code(refusal_reason::shipped_object_damaged));
    BOOST_REQUIRE_MESSAGE(
            !co_await seastar::file_exists((dir / fmt::format("CommitLog-2-{}.log", segment_id_of(0))).native()), "the refused segment was left on disk");
}

SEASTAR_TEST_CASE(test_a_segment_with_another_checksum_is_refused_and_removed) {
    fake_store store;
    const auto f = store_segment(store, 0, "the bytes the store holds");
    tmpdir work;
    const auto dir = co_await make_restore_dir(work.path(), test_target);
    const auto fetched = co_await fetch(store, f, dir, f.crc32 ^ 1);
    BOOST_REQUIRE_MESSAGE(!fetched.has_value(), "a segment with another checksum was accepted");
    BOOST_REQUIRE_EQUAL(fetched.error().code(), reason_code(refusal_reason::shipped_object_damaged));
    BOOST_REQUIRE_MESSAGE(
            !co_await seastar::file_exists((dir / fmt::format("CommitLog-2-{}.log", segment_id_of(0))).native()), "the refused segment was left on disk");
}

SEASTAR_TEST_CASE(test_a_segment_the_store_does_not_hold_is_refused) {
    fake_store store;
    auto f = store_segment(store, 0, "the bytes the store holds");
    store.objects.clear();
    tmpdir work;
    const auto dir = co_await make_restore_dir(work.path(), test_target);
    const auto fetched = co_await fetch(store, f, dir, f.crc32);
    BOOST_REQUIRE_MESSAGE(!fetched.has_value(), "a segment the store does not hold was accepted");
    BOOST_REQUIRE_EQUAL(fetched.error().code(), reason_code(refusal_reason::shipped_object_missing));
    BOOST_REQUIRE_MESSAGE(!co_await seastar::file_exists((dir / fmt::format("CommitLog-2-{}.log", segment_id_of(0))).native()),
            "a file was left behind for a segment that was never fetched");
}

// Startup replays whatever it finds in the commitlog directory, so a fetched
// segment there would be replayed into the source table.
SEASTAR_TEST_CASE(test_the_restore_directory_is_not_under_the_commitlog_directory) {
    db::config cfg;
    cfg.work_directory("/var/lib/scylla-pitr-test");
    // Both commitlog options are unset here, as they are on most nodes, and the
    // node fills them from the work directory: the comparison has to be with
    // what it ends up using, not with the empty default.
    cfg.setup_directories();
    const auto work_dir = std::filesystem::path(cfg.work_directory());
    const auto root = restore_root(work_dir).lexically_normal();
    for (const auto& directory : {cfg.commitlog_directory(), cfg.schema_commitlog_directory()}) {
        const auto commitlog = std::filesystem::path(directory).lexically_normal();
        BOOST_REQUIRE(!commitlog.empty());
        BOOST_REQUIRE_MESSAGE(!root.native().starts_with(commitlog.native()), fmt::format("{} is under the commitlog directory {}", root, commitlog));
        BOOST_REQUIRE_MESSAGE(!commitlog.native().starts_with(root.native()), fmt::format("the commitlog directory {} is under {}", commitlog, root));
    }
    BOOST_REQUIRE(restore_dir_path(work_dir, test_target).native().starts_with(root.native()));
    co_return;
}

SEASTAR_TEST_CASE(test_the_restore_directory_is_made_empty_and_removed_and_swept) {
    tmpdir work;
    const auto dir = co_await make_restore_dir(work.path(), test_target);
    const auto leftover = dir / "CommitLog-2-1.log";
    co_await write_file(leftover, "a segment an interrupted run fetched");
    // A second run on the same target starts from nothing: what the first one
    // left is of no use to it.
    BOOST_REQUIRE_EQUAL((co_await make_restore_dir(work.path(), test_target)).native(), dir.native());
    BOOST_REQUIRE(!co_await seastar::file_exists(leftover.native()));

    co_await remove_restore_dir(work.path(), test_target);
    BOOST_REQUIRE(!co_await seastar::file_exists(dir.native()));
    // Removing it twice is what every path out of a run does.
    co_await remove_restore_dir(work.path(), test_target);

    co_await make_restore_dir(work.path(), test_target);
    co_await sweep_restore_dirs(work.path());
    BOOST_REQUIRE(!co_await seastar::file_exists(restore_root(work.path()).native()));
    // A node that never replayed has no root to sweep.
    co_await sweep_restore_dirs(work.path());
}
