/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests the snapshot's point-in-time-recovery bound file (issue #12): its codec
// and the fact that a snapshot writes it exactly when PITR can run. Restore
// takes the set of shards, their replay bounds and their chain ids from this
// file, so a value read back wrong, or a file believed when it is damaged,
// restores to a point-in-time that silently loses writes or brings deleted rows
// back. Every refusal therefore gets a test of its own.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <filesystem>
#include <limits>

#include <boost/test/unit_test.hpp>

#include <seastar/core/smp.hh>
#include <seastar/util/file.hh>

#include "db/config.hh"
#include "db/pitr/bound_file.hh"
#include "replica/database.hh"
#include "replica/distributed_loader.hh"
#include "sstables/sstables.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/test_utils.hh"
#include "utils/crc.hh"

using namespace db::pitr;

namespace {

constexpr auto ks_name = "ks";
constexpr auto cf_name = "cf";
constexpr auto snapshot_name = "test";

const utils::UUID test_host_id{"a1b2c3d4-0000-4000-8000-00000000000f"};
const chain_id test_chain{utils::UUID("11112222-0000-4000-8000-00000000000a")};
const table_schema_version test_schema_version{utils::UUID("33334444-0000-4000-8000-00000000000b")};

shard_chain make_chain(std::optional<uint64_t> floor = 2) {
    return shard_chain{test_host_id, 1762300000000000LL, 1762300001000000LL, test_chain, floor};
}

// Shard 0 has a chain, shard 1 has none: both rows are exercised by every codec
// test that does not need something else. The bounds carry their shard in the
// segment id, the way a real capture does.
bound_file make_file() {
    bound_file f;
    f.capture_time_us = 1762400000000000LL;
    f.schema_version = test_schema_version;
    f.shards.push_back(shard_bound{0, db::replay_position(0u, 3, 128), make_chain()});
    f.shards.push_back(shard_bound{1, db::replay_position(1u, 4, 0), std::nullopt});
    return f;
}

// The bytes `make_file()` must encode to, spelled out: a change to the format
// has to be a deliberate edit here, because an old backup's file still has to
// read on a new node.
constexpr std::string_view golden = "format=pitr-bound\n"
                                    "version=1\n"
                                    "capture_time_us=1762400000000000\n"
                                    "schema_version=33334444-0000-4000-8000-00000000000b\n"
                                    "shard_count=2\n"
                                    "shard=0\n"
                                    "bound_segment_id=3\n"
                                    "bound_position=128\n"
                                    "chain=present\n"
                                    "chain_host_id=a1b2c3d4-0000-4000-8000-00000000000f\n"
                                    "chain_boot_epoch_us=1762300000000000\n"
                                    "chain_start_us=1762300001000000\n"
                                    "chain_id=11112222-0000-4000-8000-00000000000a\n"
                                    "chain_floor_segment_id=2\n"
                                    "shard=1\n"
                                    "bound_segment_id=18014398509481988\n"
                                    "bound_position=0\n"
                                    "chain=none\n"
                                    "crc32=30c3489d\n";

// The length of the checksum line, which several tests cut off.
constexpr size_t crc_line_size = sizeof("crc32=00000000\n") - 1;

// Re-checksums whatever the body has become, so that a test of one rule is not
// passing because it also broke the checksum.
sstring with_fresh_crc(std::string_view body) {
    utils::crc32 crc;
    crc.process(reinterpret_cast<const uint8_t*>(body.data()), body.size());
    return seastar::format("{}crc32={:08x}\n", body, crc.get());
}

// The golden file's body, with one of its lines replaced, and a matching
// checksum. Used by every test of a line-level rule.
sstring golden_body() {
    return sstring(golden.substr(0, golden.size() - crc_line_size));
}

sstring golden_with_line(std::string_view line, std::string_view replacement) {
    sstring body = golden_body();
    auto at = body.find(line);
    BOOST_REQUIRE_NE(at, sstring::npos);
    body = body.substr(0, at) + sstring(replacement) + body.substr(at + line.size());
    return with_fresh_crc(body);
}

void require_refused(std::string_view content, std::string_view what) {
    BOOST_TEST_CONTEXT(what) {
        BOOST_REQUIRE_THROW(decode_bound_file(content), bound_file_format_error);
    }
}

} // namespace

BOOST_AUTO_TEST_SUITE(pitr_bound_file_test)

BOOST_AUTO_TEST_CASE(test_a_file_with_a_chain_and_without_one_round_trips) {
    BOOST_REQUIRE(decode_bound_file(encode(make_file())) == make_file());
}

BOOST_AUTO_TEST_CASE(test_the_encoding_is_the_golden_one) {
    BOOST_REQUIRE_EQUAL(encode(make_file()), sstring(golden));
    BOOST_REQUIRE(decode_bound_file(golden) == make_file());
}

BOOST_AUTO_TEST_CASE(test_a_single_shard_file_round_trips) {
    bound_file f;
    f.capture_time_us = 0;
    f.schema_version = test_schema_version;
    f.shards.push_back(shard_bound{0, db::replay_position(0u, 1, 0), std::nullopt});

    BOOST_REQUIRE(decode_bound_file(encode(f)) == f);
}

// A present chain carries its D27 floor, or records that it has none; both have
// to survive the round trip, because restore treats them differently.
BOOST_AUTO_TEST_CASE(test_a_chain_round_trips_with_a_floor_and_without_one) {
    auto with_floor = make_file();
    BOOST_REQUIRE(decode_bound_file(encode(with_floor)) == with_floor);

    auto without = make_file();
    without.shards[0].chain = make_chain(std::nullopt);
    auto decoded = decode_bound_file(encode(without));
    BOOST_REQUIRE(decoded == without);
    BOOST_REQUIRE(!decoded.shards[0].chain->floor_segment_id);
}

// 0 is no segment's id, so it is never a stand-in for "none": a restore
// comparing a bound against it would accept every chain.
BOOST_AUTO_TEST_CASE(test_a_chain_floor_of_zero_is_refused) {
    auto f = make_file();
    f.shards[0].chain = make_chain(0);
    BOOST_REQUIRE_THROW(encode(f), bound_file_format_error);

    require_refused(golden_with_line("chain_floor_segment_id=2\n", "chain_floor_segment_id=0\n"), "a floor of 0 in the file");
}

// The widest machine a replay position can name, with the widest value every
// field can take: the file is what every reader's size cap is derived from.
BOOST_AUTO_TEST_CASE(test_the_widest_file_a_replay_position_can_name_round_trips) {
    bound_file f;
    f.capture_time_us = std::numeric_limits<int64_t>::max();
    f.schema_version = test_schema_version;
    for (uint32_t shard = 0; shard < max_shards_in_a_bound_file; ++shard) {
        auto c = make_chain(std::numeric_limits<uint64_t>::max());
        c.boot_epoch_us = std::numeric_limits<int64_t>::max();
        c.chain_start_us = std::numeric_limits<int64_t>::max();
        // Every shard's bound is the highest position its own shard can hold.
        f.shards.push_back(shard_bound{shard, db::replay_position(shard, db::replay_position::ts_mask, std::numeric_limits<db::position_type>::max()), c});
    }

    auto content = encode(f);

    BOOST_REQUIRE_LE(content.size(), max_bound_file_size);
    BOOST_REQUIRE(decode_bound_file(content) == f);
}

// Past that the writer refuses: a count no replay position can name must not
// produce a file every reader would then refuse as oversize.
BOOST_AUTO_TEST_CASE(test_encoding_more_shards_than_a_replay_position_can_name_is_refused) {
    bound_file f;
    f.schema_version = test_schema_version;
    for (uint32_t shard = 0; shard <= max_shards_in_a_bound_file; ++shard) {
        f.shards.push_back(shard_bound{shard, db::replay_position(0u, 1, 0), std::nullopt});
    }

    BOOST_REQUIRE_THROW(encode(f), bound_file_format_error);
}

// The writer refuses what the reader would: a row for the wrong shard means a
// shard is missing or recorded twice, and no file may say so.
BOOST_AUTO_TEST_CASE(test_encoding_shard_rows_out_of_order_is_refused) {
    auto swapped = make_file();
    std::swap(swapped.shards[0].shard, swapped.shards[1].shard);
    BOOST_REQUIRE_THROW(encode(swapped), bound_file_format_error);

    bound_file none;
    none.schema_version = test_schema_version;
    BOOST_REQUIRE_THROW(encode(none), bound_file_format_error);
}

// A file that claims no shard at all, with no row to contradict it: a snapshot
// always covers at least one shard, so this is damage and not an empty backup.
BOOST_AUTO_TEST_CASE(test_a_shard_count_of_zero_with_no_rows_is_refused) {
    auto head = golden_body();
    head = head.substr(0, head.find("shard=0\n"));
    head = head.substr(0, head.find("shard_count=2\n")) + "shard_count=0\n";
    require_refused(with_fresh_crc(head), "no shards and no rows");
}

BOOST_AUTO_TEST_CASE(test_a_truncated_file_is_refused) {
    // Cut in the middle of the last line: an upload that stopped half way.
    require_refused(golden.substr(0, golden.size() - 4), "a file with no final newline");
    require_refused(golden.substr(0, 40), "a file cut before the shard rows");
}

BOOST_AUTO_TEST_CASE(test_an_extra_key_is_refused) {
    require_refused(with_fresh_crc(seastar::format("{}extra=1\n", golden_body())), "a key after the rows");
    require_refused(golden_with_line("shard=1\n", "surprise=1\nshard=1\n"), "a key between two rows");
}

BOOST_AUTO_TEST_CASE(test_reordered_keys_are_refused) {
    require_refused(golden_with_line("bound_segment_id=3\nbound_position=128\n", "bound_position=128\nbound_segment_id=3\n"), "two swapped fields");
}

BOOST_AUTO_TEST_CASE(test_a_wrong_checksum_is_refused) {
    sstring bad(golden);
    bad[bad.size() - 2] = bad[bad.size() - 2] == '0' ? '1' : '0';
    require_refused(bad, "one digit of the checksum changed");
    // A body changed without the checksum: what a bit flip in transit looks
    // like, and the case a reader must never accept.
    auto stale = golden_with_line("bound_segment_id=3\n", "bound_segment_id=9\n");
    stale = stale.substr(0, stale.size() - crc_line_size) + sstring(golden.substr(golden.size() - crc_line_size));
    require_refused(stale, "a bound raised, the old checksum left behind");
}

BOOST_AUTO_TEST_CASE(test_a_missing_shard_row_is_refused) {
    auto at = golden.find("shard=1\n");
    require_refused(with_fresh_crc(sstring(golden.substr(0, at))), "the second row dropped, the count left at 2");
}

BOOST_AUTO_TEST_CASE(test_a_duplicated_shard_row_is_refused) {
    require_refused(golden_with_line("shard=1\n", "shard=0\n"), "shard 0 recorded twice");
}

BOOST_AUTO_TEST_CASE(test_shard_rows_out_of_order_are_refused) {
    auto body = golden_body();
    auto first = body.find("shard=0\n");
    auto second = body.find("shard=1\n");
    auto reordered = body.substr(0, first) + body.substr(second) + body.substr(first, second - first);
    require_refused(with_fresh_crc(reordered), "row 1 before row 0");
}

BOOST_AUTO_TEST_CASE(test_a_shard_count_that_does_not_match_the_rows_is_refused) {
    require_refused(golden_with_line("shard_count=2\n", "shard_count=1\n"), "one row too many");
    require_refused(golden_with_line("shard_count=2\n", "shard_count=3\n"), "one row too few");
    require_refused(golden_with_line("shard_count=2\n", "shard_count=4294967295\n"), "a count no node has");
    require_refused(golden_with_line("shard_count=2\n", "shard_count=0\n"), "no shard at all");
}

BOOST_AUTO_TEST_CASE(test_a_signed_or_zero_padded_number_is_refused) {
    require_refused(golden_with_line("bound_position=128\n", "bound_position=+128\n"), "a plus sign");
    require_refused(golden_with_line("bound_position=128\n", "bound_position=-1\n"), "a negative position");
    require_refused(golden_with_line("bound_position=128\n", "bound_position=0128\n"), "a leading zero");
    require_refused(golden_with_line("bound_position=128\n", "bound_position= 128\n"), "a leading space");
    require_refused(golden_with_line("bound_position=128\n", "bound_position=\n"), "no value at all");
}

BOOST_AUTO_TEST_CASE(test_a_number_above_its_field_is_refused) {
    // 2^32 in a 32-bit position, and 2^64 in a 64-bit segment id.
    require_refused(golden_with_line("bound_position=128\n", "bound_position=4294967296\n"), "a position one above the field");
    require_refused(golden_with_line("bound_segment_id=3\n", "bound_segment_id=18446744073709551616\n"), "a segment id one above the field");
    require_refused(golden_with_line("capture_time_us=1762400000000000\n", "capture_time_us=9223372036854775808\n"), "a capture time one above the field");
}

BOOST_AUTO_TEST_CASE(test_a_null_or_non_canonical_uuid_is_refused) {
    require_refused(
            golden_with_line("chain_id=11112222-0000-4000-8000-00000000000a\n", "chain_id=00000000-0000-0000-0000-000000000000\n"), "the null chain id");
    require_refused(golden_with_line("schema_version=33334444-0000-4000-8000-00000000000b\n", "schema_version=00000000-0000-0000-0000-000000000000\n"),
            "the null schema version");
    require_refused(golden_with_line("chain_host_id=a1b2c3d4-0000-4000-8000-00000000000f\n", "chain_host_id=A1B2C3D4-0000-4000-8000-00000000000F\n"),
            "upper case, which UUID's own parser takes");
    require_refused(golden_with_line("chain_host_id=a1b2c3d4-0000-4000-8000-00000000000f\n", "chain_host_id=a1b2c3d4000040008000"
                                                                                             "00000000000f\n"),
            "the 32-hex form with no dashes, which UUID's own parser takes");
    require_refused(golden_with_line("chain_host_id=a1b2c3d4-0000-4000-8000-00000000000f\n", "chain_host_id=not-a-uuid\n"), "not a uuid at all");
}

BOOST_AUTO_TEST_CASE(test_an_oversize_file_is_refused) {
    sstring big(golden);
    big.resize(max_bound_file_size + 1, 'x');
    require_refused(big, "one byte over the maximum");
}

BOOST_AUTO_TEST_CASE(test_bytes_after_the_checksum_are_refused) {
    require_refused(seastar::format("{}trailing=1\n", golden), "a line after the checksum");
    require_refused(seastar::format("{}\n", golden), "an empty line after the checksum");
}

BOOST_AUTO_TEST_CASE(test_a_newer_version_is_refused_as_newer) {
    auto newer = golden_with_line("version=1\n", "version=2\n");
    BOOST_REQUIRE_THROW(decode_bound_file(newer), bound_file_unknown_version);
    // Damage is never reported as a newer version: an intact file is what
    // claims one, so the remedy stays "another backup" and not "upgrade".
    sstring damaged(newer);
    damaged[damaged.size() - 2] = damaged[damaged.size() - 2] == '0' ? '1' : '0';
    try {
        decode_bound_file(damaged);
        BOOST_FAIL("a damaged file was accepted");
    } catch (const bound_file_unknown_version&) {
        BOOST_FAIL("damage was reported as a newer version");
    } catch (const bound_file_format_error&) {
    }
}

// Version 0 was never written, so it is damage: reporting it as newer would
// send an operator upgrading instead of taking another backup.
BOOST_AUTO_TEST_CASE(test_version_zero_is_damage_and_not_a_newer_version) {
    auto zero = golden_with_line("version=1\n", "version=0\n");
    try {
        decode_bound_file(zero);
        BOOST_FAIL("version 0 was accepted");
    } catch (const bound_file_unknown_version&) {
        BOOST_FAIL("version 0 was reported as a newer version");
    } catch (const bound_file_format_error&) {
    }
}

BOOST_AUTO_TEST_CASE(test_a_file_that_is_not_a_bound_file_is_refused) {
    require_refused("", "an empty file");
    require_refused("{\"manifest\": {}}\n", "a manifest");
    require_refused(golden_with_line("format=pitr-bound\n", "format=pitr-ledger\n"), "another pitr format");
}

namespace {

// Every shard's bound, taken the way the snapshot takes it.
std::vector<db::replay_position> capture_bounds(cql_test_env& e) {
    std::vector<db::replay_position> bounds(this_smp_shard_count());
    e.db().invoke_on_all([&bounds](replica::database& db) -> future<> {
              auto& t = db.find_column_family(ks_name, cf_name);
              bounds[this_shard_id()] = (co_await t.snapshot_sstables()).commitlog_bound;
          }).get();
    return bounds;
}

std::filesystem::path snapshot_dir(cql_test_env& e) {
    auto& cf = e.local_db().find_column_family(ks_name, cf_name);
    return tests::table_dir(cf) / sstables::snapshots_dir / snapshot_name;
}

// skip_flush by default, so that the commitlog position the snapshot records
// can be sandwiched between two observations of it.
void take_snapshot(cql_test_env& e, bool skip_flush = true) {
    auto uuid = e.db().local().find_uuid(ks_name, cf_name);
    replica::database::snapshot_table_on_all_shards(e.db(), uuid, snapshot_name, db::snapshot_options{.skip_flush = skip_flush}).get();
}

void create_and_fill(cql_test_env& e) {
    e.execute_cql(seastar::format("create table {}.{} (k text primary key, v int);", ks_name, cf_name)).get();
    for (int i = 0; i < 8; ++i) {
        e.execute_cql(seastar::format("insert into {}.{} (k, v) values ('k{}', {});", ks_name, cf_name, i, i)).get();
    }
}

cql_test_config pitr_config() {
    auto cfg = cql_test_config();
    cfg.db_config->pitr_endpoint.set("http://localhost:9000");
    cfg.db_config->pitr_bucket.set("pitr-bucket");
    return cfg;
}

} // namespace

// The restored snapshot is loaded as a table directory, and the bound file is
// not an sstable component: the lister refuses a name it cannot parse rather
// than ignoring it, so without manifest_json_filter skipping this name the load
// fails with "invalid version for file pitr-bound.txt". Nobody can restore a
// PITR backup if its own bound file stops the load.
SEASTAR_TEST_CASE(test_a_snapshot_holding_the_bound_file_loads_through_the_upload_dir) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                create_and_fill(e);
                // With a flush, so the snapshot holds sstables to load.
                take_snapshot(e, false);
                BOOST_REQUIRE(std::filesystem::exists(snapshot_dir(e) / bound_file_name));

                auto& t = e.local_db().find_column_family(ks_name, cf_name);
                tests::copy_directory(snapshot_dir(e), tests::table_dir(t) / sstables::upload_dir);
                replica::distributed_loader::process_upload_dir(e.db(), e.view_builder(), e.view_building_worker(), ks_name, cf_name, false, false).get();

                assert_that(e.execute_cql(seastar::format("select * from {}.{};", ks_name, cf_name)).get()).is_rows().with_size(8);
            },
            pitr_config());
}

// The file holds one row per shard, and each row's bound is that shard's own:
// a bound taken from another shard, or from another moment, falls outside the
// sandwich, because nothing flushes between the two observations.
SEASTAR_TEST_CASE(test_a_snapshot_with_pitr_configured_records_every_shard_bound) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                create_and_fill(e);
                auto before = capture_bounds(e);

                take_snapshot(e);

                auto after = capture_bounds(e);
                auto content = seastar::util::read_entire_file_contiguous(snapshot_dir(e) / bound_file_name).get();
                auto f = decode_bound_file(content);

                BOOST_REQUIRE_EQUAL(f.shards.size(), this_smp_shard_count());
                BOOST_REQUIRE(f.schema_version == e.local_db().find_column_family(ks_name, cf_name).schema()->version());
                BOOST_REQUIRE_GT(f.capture_time_us, 0);
                for (unsigned shard = 0; shard < this_smp_shard_count(); ++shard) {
                    BOOST_TEST_CONTEXT("shard " << shard) {
                        const auto& row = f.shards[shard];
                        BOOST_REQUIRE_EQUAL(row.shard, shard);
                        BOOST_REQUIRE_EQUAL(row.bound.shard_id(), shard);
                        BOOST_REQUIRE(before[shard] <= row.bound);
                        BOOST_REQUIRE(row.bound <= after[shard]);
                        // Nothing installs a provider in this environment, so
                        // every shard reports no chain (#8, piece 2b).
                        BOOST_REQUIRE(!row.chain);
                    }
                }
            },
            pitr_config());
}

// A node that cannot ship gets the snapshot it got before: not an empty bound
// file, which a restore would have to tell apart from a damaged one.
SEASTAR_TEST_CASE(test_a_snapshot_without_pitr_holds_no_bound_file) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        create_and_fill(e);

        take_snapshot(e);

        BOOST_REQUIRE(!std::filesystem::exists(snapshot_dir(e) / bound_file_name));
        BOOST_REQUIRE(std::filesystem::exists(snapshot_dir(e) / "manifest.json"));
    });
}

// A provider that throws must cost the snapshot, not leave a file with a row
// invented for the shard that failed.
SEASTAR_TEST_CASE(test_a_provider_that_throws_fails_the_snapshot_and_writes_nothing) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                create_and_fill(e);
                e.db().invoke_on_all([](replica::database& db) {
                          db.set_pitr_chain_provider([]() -> std::optional<shard_chain> {
                              throw std::runtime_error("no chain for you");
                          });
                      }).get();

                BOOST_REQUIRE_THROW(take_snapshot(e), std::runtime_error);

                BOOST_REQUIRE(!std::filesystem::exists(snapshot_dir(e) / bound_file_name));
            },
            pitr_config());
}

// The manifest is the last file a snapshot writes, so a bound file that cannot
// be written must stop the snapshot before it: a manifest beside a missing
// bound file would look like a snapshot a restore can replay from. The failure
// is forced through the provider, which hands back a chain no file can record.
SEASTAR_TEST_CASE(test_a_bound_file_that_cannot_be_written_leaves_no_manifest) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                create_and_fill(e);
                e.db().invoke_on_all([](replica::database& db) {
                          db.set_pitr_chain_provider([]() -> std::optional<shard_chain> {
                              // A floor of 0 is refused by the codec.
                              return shard_chain{test_host_id, 1, 1, test_chain, 0};
                          });
                      }).get();

                BOOST_REQUIRE_THROW(take_snapshot(e), bound_file_format_error);

                BOOST_REQUIRE(!std::filesystem::exists(snapshot_dir(e) / "manifest.json"));
                BOOST_REQUIRE(!std::filesystem::exists(snapshot_dir(e) / bound_file_name));
            },
            pitr_config());
}

// The bound file is not data: counting it would make every snapshot of an
// empty table look as if it held some, and nodetool would report it as live.
SEASTAR_TEST_CASE(test_the_bound_file_is_not_counted_as_snapshot_data) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                create_and_fill(e);
                take_snapshot(e);
                BOOST_REQUIRE(std::filesystem::exists(snapshot_dir(e) / bound_file_name));

                auto details = e.local_db().find_column_family(ks_name, cf_name).get_snapshot_details().get();

                BOOST_REQUIRE_EQUAL(details.size(), 1);
                // Nothing was flushed, so the snapshot holds no sstable: every
                // file in it is one of the three that do not count.
                BOOST_REQUIRE_EQUAL(details.at(snapshot_name).total, 0);
                BOOST_REQUIRE_EQUAL(details.at(snapshot_name).live, 0);
            },
            pitr_config());
}

// The chain a shard reports reaches the file whole: the host id, the two times,
// the chain id and the floor, which is all restore needs to find the chain.
SEASTAR_TEST_CASE(test_an_installed_provider_reaches_every_row) {
    return do_with_cql_env_thread(
            [](cql_test_env& e) {
                create_and_fill(e);
                e.db().invoke_on_all([](replica::database& db) {
                          db.set_pitr_chain_provider([]() -> std::optional<shard_chain> {
                              // One floor per shard, so a row copied from
                              // another shard is visible.
                              return make_chain(2 + this_shard_id());
                          });
                      }).get();

                take_snapshot(e);

                auto content = seastar::util::read_entire_file_contiguous(snapshot_dir(e) / bound_file_name).get();
                auto f = decode_bound_file(content);
                BOOST_REQUIRE_EQUAL(f.shards.size(), this_smp_shard_count());
                for (unsigned shard = 0; shard < this_smp_shard_count(); ++shard) {
                    BOOST_TEST_CONTEXT("shard " << shard) {
                        BOOST_REQUIRE(f.shards[shard].chain == make_chain(2 + shard));
                    }
                }
            },
            pitr_config());
}

BOOST_AUTO_TEST_SUITE_END()
