/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the point-in-time-recovery shipping service's configuration and
// lifecycle. Everything here is mock-based: no object storage client exists at
// this point, and the shipper is expected to do nothing at all while shipping
// is off. See https://github.com/ofirwe86-weissberg/scylladb/issues/5.
//
// Assertions that run on a shard other than the test's own use the tests::
// helpers, because the Boost macros are not thread safe (test/lib/test_utils.hh).

#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/format.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "db/pitr/pitr_shipper.hh"
#include "db/pitr/segment_upload.hh"
#include "mutation/timestamp.hh"
#include "service/topology_state_machine.hh"
#include "sstables/object_storage_client.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/tmpdir.hh"
#include "utils/UUID_gen.hh"
#include "utils/crc.hh"
#include "utils/memory_data_sink.hh"
#include "utils/upload_progress.hh"

namespace db::pitr {

// Lets BOOST_REQUIRE_EQUAL report which state was reached.
std::ostream& boost_test_print_type(std::ostream& os, shipping_state state) {
    return os << format_as(state);
}

} // namespace db::pitr

using namespace db::pitr;

namespace {

void throw_on_error(const seastar::sstring& opt, const seastar::sstring& msg, std::optional<utils::config_file::value_status>) {
    throw std::invalid_argument(msg + " : " + opt);
}

using option = std::pair<seastar::sstring, seastar::sstring>;

// Builds a configuration out of YAML, which is how an operator sets these
// options, so that the option names themselves are under test.
std::unique_ptr<db::config> make_config(const std::vector<option>& options) {
    auto cfg = std::make_unique<db::config>();
    if (!options.empty()) {
        seastar::sstring yaml;
        for (const auto& [name, value] : options) {
            yaml += seastar::format("{}: {}\n", name, value);
        }
        cfg->read_from_yaml(yaml.c_str(), throw_on_error);
    }
    return cfg;
}

// Starts a shipper on every shard, hands it to the test body and stops it.
// A stop that throws, or one that does not complete, fails the test.
template <typename Func>
void with_shipper(const db::config& cfg, Func&& f) {
    seastar::sharded<pitr_shipper> shipper;
    shipper.start(std::ref(cfg)).get();
    auto stop = seastar::defer([&shipper]() noexcept {
        try {
            shipper.stop().get();
        } catch (...) {
            BOOST_ERROR(seastar::format("stopping the shipper failed: {}", std::current_exception()));
        }
    });
    shipper.invoke_on_all(&pitr_shipper::start).get();
    f(shipper);
}

// Checks the state on every shard, including the ones that are not the test's.
void require_state_on_all_shards(seastar::sharded<pitr_shipper>& shipper, shipping_state expected) {
    shipper.invoke_on_all([expected](pitr_shipper& s) {
               tests::require_equal(s.state(), expected);
               tests::require_equal(s.active(), expected == shipping_state::active);
           })
            .get();
}

// Reports the cluster size to every shard.
void note_cluster_size_on_all_shards(seastar::sharded<pitr_shipper>& shipper, size_t node_count) {
    shipper.invoke_on_all([node_count](pitr_shipper& s) {
               s.note_cluster_size(node_count);
           })
            .get();
}

// A full, valid configuration, with values that are not the defaults so that a
// parsed value can be told apart from a default one. One option can be
// overridden, which is how the invalid cases below are built.
std::vector<option> full_config(std::optional<option> override_option = std::nullopt) {
    std::vector<option> options = {
            {"pitr_endpoint", "http://localhost:9000"},
            {"pitr_bucket", "pitr-bucket"},
            {"pitr_prefix", "chains"},
            {"pitr_seal_interval_in_seconds", "30"},
            {"pitr_unshipped_limit_in_mb", "64"},
    };
    if (override_option) {
        for (auto& o : options) {
            if (o.first == override_option->first) {
                o.second = override_option->second;
            }
        }
    }
    return options;
}

// What the fake seal and node count do on this shard. Per shard, since the
// shipper runs a seal on every one. Reset by with_probe().
struct probe {
    int seals_started = 0;
    int seals_finished = 0;
    size_t nodes = 1;
    int count_calls = 0;
    enum class count_mode { ok, throws, fails };
    count_mode count = count_mode::ok;
    // When set, a seal does not finish until the future is resolved.
    std::optional<seastar::shared_promise<>> hold;
    // When set, the node count does not resolve until the future is resolved.
    std::optional<seastar::shared_promise<>> count_hold;
    // A seal fails once it is let go, which is how a held, failing seal is built.
    bool seal_fails = false;
    // When each seal started, on a clock precise enough to measure an interval.
    std::vector<std::chrono::steady_clock::time_point> seal_times;
};
thread_local probe the_probe;

pitr_shipper::seal_function fake_seal() {
    return []() -> future<> {
        ++the_probe.seals_started;
        the_probe.seal_times.push_back(std::chrono::steady_clock::now());
        if (the_probe.hold) {
            co_await the_probe.hold->get_shared_future();
        }
        if (the_probe.seal_fails) {
            throw std::runtime_error("seal failed");
        }
        ++the_probe.seals_finished;
    };
}

// The count mode is applied once the hold, if any, is let go, so that a cycle
// can be stopped on the count and then made to fail.
future<size_t> node_count_result() {
    switch (the_probe.count) {
    case probe::count_mode::throws:
        throw std::runtime_error("count threw");
    case probe::count_mode::fails:
        return make_exception_future<size_t>(std::runtime_error("count failed"));
    case probe::count_mode::ok:
        break;
    }
    return make_ready_future<size_t>(the_probe.nodes);
}

pitr_shipper::node_count_function fake_node_count() {
    return []() -> future<size_t> {
        ++the_probe.count_calls;
        if (the_probe.count_hold) {
            return the_probe.count_hold->get_shared_future().then([] {
                return node_count_result();
            });
        }
        return node_count_result();
    };
}

// A full, valid PITR configuration plus one option of its own, which is how the
// cases that turn the commitlog itself off are built.
std::vector<option> full_config_with(option extra) {
    auto options = full_config();
    options.push_back(std::move(extra));
    return options;
}

// Hand-driven tests use the longest interval the options allow, so that the
// real timer cannot fire during a test and hide a dropped tick.
std::vector<option> hand_driven_config() {
    return full_config(option{"pitr_seal_interval_in_seconds", "3600"});
}

// Generous on purpose: it is a deadline for a hang, not a measurement, and a
// loaded or debug-mode machine must not trip it.
constexpr auto cycle_timeout = std::chrono::seconds(30);

void wait_for_cycles_on_all_shards(seastar::sharded<pitr_shipper>& shipper) {
    shipper.invoke_on_all([](pitr_shipper& s) {
               return s.wait_for_cycles(cycle_timeout);
           })
            .get();
}

// One hand-driven tick on every shard, waited out. Used by the tests that need
// a run of cycles, where the real timer is too slow to drive them.
void tick_and_wait_on_all_shards(seastar::sharded<pitr_shipper>& shipper) {
    shipper.invoke_on_all([](pitr_shipper& s) {
               s.seal_tick();
           })
            .get();
    wait_for_cycles_on_all_shards(shipper);
}

// Resets the probe on every shard, runs the body, and resets it again so that
// a failing test cannot leak a held seal into the next one.
template <typename Func>
void with_probe(seastar::sharded<pitr_shipper>& shipper, Func&& f) {
    shipper.invoke_on_all([](pitr_shipper&) {
               the_probe = probe{};
           })
            .get();
    auto reset = seastar::defer([&shipper]() noexcept {
        shipper.invoke_on_all([](pitr_shipper&) {
                   if (the_probe.hold) {
                       the_probe.hold->set_value();
                   }
                   if (the_probe.count_hold) {
                       the_probe.count_hold->set_value();
                   }
                   the_probe = probe{};
               })
                .get();
    });
    f();
}

void start_sealing_on_all_shards(seastar::sharded<pitr_shipper>& shipper) {
    shipper.invoke_on_all([](pitr_shipper& s) {
               s.start_sealing(fake_seal(), fake_node_count());
           })
            .get();
}

// Adds a node to one of the topology state machine's node maps. Only the state
// is filled in: the count looks at nothing else.
void add_node(std::unordered_map<raft::server_id, service::replica_state>& nodes, service::node_state state) {
    nodes[raft::server_id{utils::UUID_gen::get_time_UUID()}] = service::replica_state{.state = state};
}

raft::server_id add_left_node(service::topology& topology) {
    const auto id = raft::server_id{utils::UUID_gen::get_time_UUID()};
    topology.left_nodes.insert(id);
    topology.left_nodes_rs[id] = service::replica_state{.state = service::node_state::left};
    return id;
}

// Polls until the condition holds. No sleeps of a fixed length: a condition
// that never comes true fails the test after the timeout.
template <typename Pred>
void wait_until(Pred&& pred) {
    const auto deadline = seastar::lowres_clock::now() + std::chrono::seconds(30);
    while (!pred()) {
        BOOST_REQUIRE(seastar::lowres_clock::now() < deadline);
        seastar::sleep(std::chrono::milliseconds(5)).get();
    }
}

// Waits on every shard for the same condition on that shard's probe.
template <typename Pred>
void wait_until_on_all_shards(seastar::sharded<pitr_shipper>& shipper, Pred pred) {
    shipper.invoke_on_all([pred](pitr_shipper& s) {
               const auto deadline = seastar::lowres_clock::now() + std::chrono::seconds(30);
               return seastar::do_until(
                       [&] {
                           return pred(s);
                       },
                       [deadline] {
                           tests::require(seastar::lowres_clock::now() < deadline);
                           return seastar::sleep(std::chrono::milliseconds(5));
                       });
           })
            .get();
}

} // anonymous namespace

// A node with no PITR options set must have shipping off, before and after the
// cluster size is counted, and must not keep any shipping configuration.
SEASTAR_THREAD_TEST_CASE(test_pitr_off_by_default) {
    auto cfg = make_config({});

    BOOST_REQUIRE_EQUAL(cfg->pitr_endpoint(), "");
    BOOST_REQUIRE_EQUAL(cfg->pitr_bucket(), "");
    BOOST_REQUIRE_EQUAL(cfg->pitr_seal_interval_in_seconds(), 60);
    BOOST_REQUIRE_EQUAL(cfg->pitr_unshipped_limit_in_mb(), 256);

    auto shipper_cfg = make_shipper_config(*cfg);
    BOOST_REQUIRE(!shipper_cfg.has_value());
    BOOST_REQUIRE_EQUAL(shipper_cfg.error(), shipping_state::off_not_configured);

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        require_state_on_all_shards(shipper, shipping_state::off_not_configured);
        shipper.invoke_on_all([](pitr_shipper& s) {
                   tests::require(!s.configured());
               })
                .get();

        // Even a single-node cluster does not turn shipping on.
        note_cluster_size_on_all_shards(shipper, 1);
        require_state_on_all_shards(shipper, shipping_state::off_not_configured);
    });
}

// Every option is read under the name it is documented with, and the ones left
// out keep their defaults.
SEASTAR_THREAD_TEST_CASE(test_pitr_options_parsed) {
    auto cfg = make_config(full_config());

    BOOST_REQUIRE_EQUAL(cfg->pitr_endpoint(), "http://localhost:9000");
    BOOST_REQUIRE_EQUAL(cfg->pitr_bucket(), "pitr-bucket");
    BOOST_REQUIRE_EQUAL(cfg->pitr_prefix(), "chains");
    BOOST_REQUIRE_EQUAL(cfg->pitr_seal_interval_in_seconds(), 30);
    BOOST_REQUIRE_EQUAL(cfg->pitr_unshipped_limit_in_mb(), 64);

    auto shipper_cfg = make_shipper_config(*cfg);
    BOOST_REQUIRE(shipper_cfg.has_value());
    BOOST_REQUIRE_EQUAL(shipper_cfg->endpoint, "http://localhost:9000");
    BOOST_REQUIRE_EQUAL(shipper_cfg->bucket, "pitr-bucket");
    BOOST_REQUIRE_EQUAL(shipper_cfg->prefix, "chains");
    BOOST_REQUIRE_EQUAL(shipper_cfg->seal_interval.count(), 30);
    BOOST_REQUIRE_EQUAL(shipper_cfg->unshipped_limit_bytes, uint64_t(64) << 20);

    // The prefix has a default, so leaving it out still yields a usable
    // configuration, and the seal interval defaults to 60 seconds.
    auto partial = make_config({{"pitr_endpoint", "http://localhost:9000"}, {"pitr_bucket", "pitr-bucket"}});
    auto partial_shipper_cfg = make_shipper_config(*partial);
    BOOST_REQUIRE(partial_shipper_cfg.has_value());
    BOOST_REQUIRE_EQUAL(partial_shipper_cfg->prefix, "pitr");
    BOOST_REQUIRE_EQUAL(partial_shipper_cfg->seal_interval.count(), 60);
}

// Half a configuration is a mistake, not an instruction to ship: shipping stays
// off and says which half is missing.
SEASTAR_THREAD_TEST_CASE(test_pitr_incomplete_configuration_keeps_shipping_off) {
    const std::vector<std::vector<option>> halves = {
            {{"pitr_endpoint", "http://localhost:9000"}},
            {{"pitr_bucket", "pitr-bucket"}},
    };
    for (const auto& half : halves) {
        auto cfg = make_config(half);
        auto shipper_cfg = make_shipper_config(*cfg);
        BOOST_REQUIRE(!shipper_cfg.has_value());
        BOOST_REQUIRE_EQUAL(shipper_cfg.error(), shipping_state::off_incomplete_config);

        with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
            require_state_on_all_shards(shipper, shipping_state::off_incomplete_config);
            note_cluster_size_on_all_shards(shipper, 1);
            require_state_on_all_shards(shipper, shipping_state::off_incomplete_config);
        });
    }
}

// A seal interval of zero, or one far past any useful RPO, and a zero unshipped
// limit, are refused rather than rounded into something arbitrary.
SEASTAR_THREAD_TEST_CASE(test_pitr_invalid_values_keep_shipping_off) {
    const std::vector<option> bad_options = {
            {"pitr_seal_interval_in_seconds", "0"},
            {"pitr_seal_interval_in_seconds", seastar::format("{}", max_seal_interval.count() + 1)},
            {"pitr_unshipped_limit_in_mb", "0"},
    };
    for (const auto& bad : bad_options) {
        auto cfg = make_config(full_config(bad));
        auto shipper_cfg = make_shipper_config(*cfg);
        BOOST_REQUIRE(!shipper_cfg.has_value());
        BOOST_REQUIRE_EQUAL(shipper_cfg.error(), shipping_state::off_invalid_config);

        with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
            require_state_on_all_shards(shipper, shipping_state::off_invalid_config);
            note_cluster_size_on_all_shards(shipper, 1);
            require_state_on_all_shards(shipper, shipping_state::off_invalid_config);
        });
    }

    // The boundary values on either side of the refusal are accepted.
    for (auto seconds : {1, int(max_seal_interval.count())}) {
        auto cfg = make_config(full_config(option{"pitr_seal_interval_in_seconds", seastar::format("{}", seconds)}));
        auto shipper_cfg = make_shipper_config(*cfg);
        BOOST_REQUIRE(shipper_cfg.has_value());
        BOOST_REQUIRE_EQUAL(shipper_cfg->seal_interval.count(), seconds);
    }
}

// A fully configured single node: shipping waits for the count and only then
// becomes active, on every shard.
SEASTAR_THREAD_TEST_CASE(test_pitr_single_node_becomes_active) {
    auto cfg = make_config(full_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        // start() alone must not ship: the cluster has not been counted yet.
        require_state_on_all_shards(shipper, shipping_state::waiting_for_cluster_size);
        shipper.invoke_on_all([](pitr_shipper& s) {
                   tests::require(s.configured());
                   tests::require_equal(s.shipping_config().bucket, seastar::sstring("pitr-bucket"));
               })
                .get();

        note_cluster_size_on_all_shards(shipper, 1);
        require_state_on_all_shards(shipper, shipping_state::active);

        // A repeated count is harmless.
        note_cluster_size_on_all_shards(shipper, 1);
        require_state_on_all_shards(shipper, shipping_state::active);
    });
}

// Anything but exactly one node is refused, zero included: a count the caller
// could not establish must not read as a single node. The refusal is a latch,
// so a later count of one does not turn shipping on behind the operator's back.
SEASTAR_THREAD_TEST_CASE(test_pitr_node_count_other_than_one_is_refused) {
    auto cfg = make_config(full_config());

    for (size_t node_count : {size_t(0), size_t(2), size_t(3)}) {
        with_shipper(*cfg, [node_count](seastar::sharded<pitr_shipper>& shipper) {
            note_cluster_size_on_all_shards(shipper, node_count);
            require_state_on_all_shards(shipper, shipping_state::off_multi_node);

            note_cluster_size_on_all_shards(shipper, 1);
            require_state_on_all_shards(shipper, shipping_state::off_multi_node);
        });
    }

    // One node, and only one node, ships.
    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        note_cluster_size_on_all_shards(shipper, 1);
        require_state_on_all_shards(shipper, shipping_state::active);
    });
}

// A node that runs without a commitlog has nothing to seal, so shipping is
// refused at startup instead of reporting active while nothing is shipped. The
// reason names the cause and the way back (P32's pattern), and the refusal is
// latched, so a later count of one does not turn shipping on.
SEASTAR_THREAD_TEST_CASE(test_pitr_disabled_commitlog_refuses_shipping) {
    auto cfg = make_config(full_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        shipper.invoke_on_all(&pitr_shipper::note_commitlog_disabled).get();
        require_state_on_all_shards(shipper, shipping_state::off_commitlog_disabled);
        shipper.invoke_on_all([](pitr_shipper& s) {
                   tests::require(s.break_reason().has_value());
                   tests::require(s.break_reason()->find("commitlog_disabled") != seastar::sstring::npos);
                   tests::require(s.break_reason()->find("restart") != seastar::sstring::npos);
               })
                .get();

        note_cluster_size_on_all_shards(shipper, 1);
        require_state_on_all_shards(shipper, shipping_state::off_commitlog_disabled);
    });
}

// A refusal for a disabled commitlog must leave no seal timer behind either:
// nothing is sealed, so nothing may be scheduled or run.
SEASTAR_THREAD_TEST_CASE(test_pitr_disabled_commitlog_seals_nothing) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            shipper.invoke_on_all(&pitr_shipper::note_commitlog_disabled).get();
            // A node doesn't count after this refusal, but if a count of one
            // arrives anyway, the refusal stays latched.
            note_cluster_size_on_all_shards(shipper, 1);
            start_sealing_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper& s) {
                       tests::require(!s.sealing_scheduled());
                       s.seal_tick();
                       tests::require_equal(the_probe.seals_started, 0);
                       tests::require_equal(the_probe.count_calls, 0);
                   })
                    .get();
        });
    });
}

// start() and stop() must both complete without ever yielding: they run in the
// node's startup and shutdown paths, and a shipper that is off must not hold
// the reactor or leave work behind. An unresolved future here would hang the
// test rather than fail it, so the readiness is asserted before it is waited on.
SEASTAR_THREAD_TEST_CASE(test_pitr_starts_and_stops_without_blocking) {
    auto off = make_config({});
    auto on = make_config(full_config());

    for (const auto* cfg : {off.get(), on.get()}) {
        for (int i = 0; i < 3; ++i) {
            pitr_shipper shipper(*cfg);

            auto start = shipper.start();
            BOOST_REQUIRE(start.available());
            start.get();

            shipper.note_cluster_size(1);

            auto stop = shipper.stop();
            BOOST_REQUIRE(stop.available());
            BOOST_REQUIRE(!stop.failed());
            stop.get();
        }
    }

    // The same through sharded<>, including a stop that never saw a start().
    for (int i = 0; i < 3; ++i) {
        with_shipper(*off, [](seastar::sharded<pitr_shipper>&) {});
        with_shipper(*on, [](seastar::sharded<pitr_shipper>& shipper) {
            note_cluster_size_on_all_shards(shipper, 1);
        });

        seastar::sharded<pitr_shipper> shipper;
        shipper.start(std::ref(*on)).get();
        shipper.stop().get();
    }
}

// Shipping that is off must not own a timer: not unconfigured, not half
// configured, not waiting for the count, not refused for its node count.
SEASTAR_THREAD_TEST_CASE(test_pitr_seal_timer_is_not_armed_while_shipping_is_off) {
    auto off = make_config({});
    auto invalid = make_config(full_config(option{"pitr_seal_interval_in_seconds", "0"}));
    auto on = make_config(hand_driven_config());

    for (const auto* cfg : {off.get(), invalid.get()}) {
        with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
            with_probe(shipper, [&] {
                note_cluster_size_on_all_shards(shipper, 1);
                start_sealing_on_all_shards(shipper);
                shipper.invoke_on_all([](pitr_shipper& s) {
                           tests::require(!s.sealing_scheduled());
                           s.seal_tick();
                           tests::require_equal(the_probe.seals_started, 0);
                       })
                        .get();
            });
        });
    }

    with_shipper(*on, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            // Configured, but the cluster has not been counted.
            start_sealing_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper& s) {
                       tests::require(!s.sealing_scheduled());
                   })
                    .get();
        });
    });

    with_shipper(*on, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 2);
            start_sealing_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper& s) {
                       tests::require(!s.sealing_scheduled());
                   })
                    .get();
        });
    });
}

// The timer carries the configured interval, on every shard: neither the
// 1 second a hard-coded interval would give nor a much slower one. Checked on
// the timer's own expiry, so that the test waits for no wall-clock time.
SEASTAR_THREAD_TEST_CASE(test_pitr_seal_timer_uses_the_configured_interval) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 1);
            start_sealing_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper& s) {
                       tests::require(s.sealing_scheduled());
                       const auto due_in = s.next_seal_time() - seastar::steady_clock_type::now();
                       tests::require(due_in > std::chrono::seconds(3500));
                       tests::require(due_in <= std::chrono::seconds(3600));
                   })
                    .get();
        });
    });
}

// The real timer does fire, repeatedly, on every shard. Only a loose lower
// bound on the gap is checked, since nothing here is a measurement: the exact
// interval is pinned by test_pitr_seal_timer_uses_the_configured_interval, and
// a pause anywhere in this test must not make it fail. The gap is taken inside
// the seal, so polling adds no error.
SEASTAR_THREAD_TEST_CASE(test_pitr_real_timer_seals_repeatedly_on_every_shard) {
    auto cfg = make_config(full_config(option{"pitr_seal_interval_in_seconds", "2"}));

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 1);
            start_sealing_on_all_shards(shipper);
            wait_until_on_all_shards(shipper, [](pitr_shipper&) {
                return the_probe.seal_times.size() >= 2;
            });
            shipper.invoke_on_all([](pitr_shipper& s) {
                       tests::require(s.sealing_scheduled());
                       const auto gap = the_probe.seal_times[1] - the_probe.seal_times[0];
                       tests::require(gap >= std::chrono::seconds(1));
                   })
                    .get();
        });
    });
}

// Ticks during a running seal are not lost, and do not stack: however many
// arrive, one follow-up cycle runs once the seal finishes.
SEASTAR_THREAD_TEST_CASE(test_pitr_ticks_during_a_slow_seal_run_one_follow_up) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 1);
            shipper.invoke_on_all([](pitr_shipper& s) {
                       the_probe.hold.emplace();
                       s.start_sealing(fake_seal(), fake_node_count());
                       for (int i = 0; i < 5; ++i) {
                           s.seal_tick();
                       }
                   })
                    .get();
            // The seal starts in the background, after the count is read.
            wait_until_on_all_shards(shipper, [](pitr_shipper&) {
                return the_probe.seals_started >= 1;
            });
            shipper.invoke_on_all([](pitr_shipper&) {
                       tests::require_equal(the_probe.seals_started, 1);
                       the_probe.hold->set_value();
                       the_probe.hold.reset();
                   })
                    .get();
            wait_for_cycles_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper&) {
                       tests::require_equal(the_probe.seals_started, 2);
                       tests::require_equal(the_probe.seals_finished, 2);
                   })
                    .get();
        });
    });
}

// A tick with no seal running seals once and owes nothing afterwards.
SEASTAR_THREAD_TEST_CASE(test_pitr_tick_with_no_seal_running_runs_one_cycle) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 1);
            start_sealing_on_all_shards(shipper);
            for (int i = 1; i <= 3; ++i) {
                shipper.invoke_on_all([](pitr_shipper& s) {
                           s.seal_tick();
                       })
                        .get();
                wait_for_cycles_on_all_shards(shipper);
                shipper.invoke_on_all([i](pitr_shipper& s) {
                           tests::require_equal(the_probe.seals_started, i);
                           tests::require(s.last_outcome() == pitr_shipper::cycle_outcome::sealed);
                       })
                        .get();
            }
        });
    });
}

// A node the topology gained after startup turns shipping off on the next
// cycle, records why, and stops the timer. The count is checked before the
// seal, so the cycle that notices it does not seal.
SEASTAR_THREAD_TEST_CASE(test_pitr_node_added_mid_run_turns_shipping_off) {
    auto cfg = make_config(hand_driven_config());

    for (size_t node_count : {size_t(0), size_t(2), size_t(5)}) {
        with_shipper(*cfg, [node_count](seastar::sharded<pitr_shipper>& shipper) {
            with_probe(shipper, [&] {
                note_cluster_size_on_all_shards(shipper, 1);
                start_sealing_on_all_shards(shipper);
                shipper.invoke_on_all([](pitr_shipper& s) {
                           s.seal_tick();
                       })
                        .get();
                wait_for_cycles_on_all_shards(shipper);
                shipper.invoke_on_all([](pitr_shipper& s) {
                           tests::require_equal(the_probe.seals_finished, 1);
                           tests::require(!s.break_reason());
                       })
                        .get();

                shipper.invoke_on_all([node_count](pitr_shipper& s) {
                           the_probe.nodes = node_count;
                           s.seal_tick();
                       })
                        .get();
                wait_for_cycles_on_all_shards(shipper);
                shipper.invoke_on_all([node_count](pitr_shipper& s) {
                           tests::require_equal(s.state(), shipping_state::off_multi_node);
                           tests::require(s.last_outcome() == pitr_shipper::cycle_outcome::shipping_broke);
                           tests::require(s.break_reason().has_value());
                           tests::require(s.break_reason()->find(seastar::format("{}", node_count)) != seastar::sstring::npos);
                           tests::require(!s.sealing_scheduled());
                           tests::require_equal(the_probe.seals_started, 1);
                           // Latched: a later tick does nothing.
                           the_probe.nodes = 1;
                           s.seal_tick();
                           tests::require_equal(the_probe.seals_started, 1);
                       })
                        .get();
            });
        });
    }
}

// A node count that cannot be read, by throwing or by a failed future, is not a
// count of one: the cycle seals nothing and breaks shipping on that shard at
// once, latched, with the reason the user is told (issue #7).
SEASTAR_THREAD_TEST_CASE(test_pitr_unreadable_node_count_breaks_shipping_at_once) {
    auto cfg = make_config(hand_driven_config());

    for (auto mode : {probe::count_mode::throws, probe::count_mode::fails}) {
        with_shipper(*cfg, [mode](seastar::sharded<pitr_shipper>& shipper) {
            with_probe(shipper, [&] {
                note_cluster_size_on_all_shards(shipper, 1);
                start_sealing_on_all_shards(shipper);
                shipper.invoke_on_all([mode](pitr_shipper& s) {
                           the_probe.count = mode;
                           s.seal_tick();
                       })
                        .get();
                wait_for_cycles_on_all_shards(shipper);
                shipper.invoke_on_all([](pitr_shipper& s) {
                           tests::require_equal(the_probe.count_calls, 1);
                           tests::require_equal(the_probe.seals_started, 0);
                           tests::require_equal(s.state(), shipping_state::off_node_count_unreadable);
                           tests::require(s.last_outcome() == pitr_shipper::cycle_outcome::shipping_broke);
                           tests::require(s.break_reason().has_value());
                           tests::require(s.break_reason()->find("node_count_unreadable") != seastar::sstring::npos);
                           tests::require(!s.sealing_scheduled());
                           // Latched: a readable count does not bring shipping back.
                           the_probe.count = probe::count_mode::ok;
                           s.seal_tick();
                           tests::require_equal(the_probe.seals_started, 0);
                           tests::require_equal(s.state(), shipping_state::off_node_count_unreadable);
                       })
                        .get();
            });
        });
    }
}

// A topology with nothing in it counts no nodes, which is not one and so
// refuses shipping rather than reading as a single node.
SEASTAR_THREAD_TEST_CASE(test_pitr_node_count_of_an_empty_topology_is_zero) {
    const service::topology topology;
    BOOST_REQUIRE_EQUAL(count_cluster_nodes(topology), 0);
}

// Every node the cluster holds counts, whichever map it sits in, so that a
// joining or zero-token peer is seen; the nodes that left do not.
SEASTAR_THREAD_TEST_CASE(test_pitr_node_count_counts_members_but_not_left_nodes) {
    service::topology topology;
    add_node(topology.normal_nodes, service::node_state::normal);
    add_node(topology.normal_nodes, service::node_state::normal);
    add_node(topology.transition_nodes, service::node_state::bootstrapping);
    add_node(topology.new_nodes, service::node_state::none);
    BOOST_REQUIRE_EQUAL(count_cluster_nodes(topology), 4);

    // Left nodes are held in two places, and neither is part of the cluster.
    add_left_node(topology);
    add_left_node(topology);
    BOOST_REQUIRE_EQUAL(count_cluster_nodes(topology), 4);
}

// The decommission of the only peer brings the node back to single, so shipping
// is not refused for a cluster that no longer exists (issue #7, P23).
SEASTAR_THREAD_TEST_CASE(test_pitr_node_count_is_one_again_after_the_peer_left) {
    service::topology topology;
    add_node(topology.normal_nodes, service::node_state::normal);
    BOOST_REQUIRE_EQUAL(count_cluster_nodes(topology), 1);

    add_node(topology.new_nodes, service::node_state::none);
    BOOST_REQUIRE_EQUAL(count_cluster_nodes(topology), 2);

    const auto peer = topology.new_nodes.begin()->first;
    topology.new_nodes.erase(peer);
    topology.left_nodes.insert(peer);
    BOOST_REQUIRE_EQUAL(count_cluster_nodes(topology), 1);
}

// The commitlog decision comes from the configuration, not from the commitlog
// object, which the database creates in every case. An in-memory data store
// writes no commitlog either, as database::make_keyspace_config() has it.
SEASTAR_THREAD_TEST_CASE(test_pitr_commitlog_is_disabled_for_user_tables_by_config) {
    BOOST_REQUIRE(!commitlog_disabled_for_user_tables(true, false));
    BOOST_REQUIRE(commitlog_disabled_for_user_tables(false, false));
    BOOST_REQUIRE(commitlog_disabled_for_user_tables(true, true));
    BOOST_REQUIRE(commitlog_disabled_for_user_tables(false, true));
}

// The defaults ship: a node left alone writes a commitlog, so the refusal is
// not what a default configuration gets.
SEASTAR_THREAD_TEST_CASE(test_pitr_default_config_does_not_disable_the_commitlog) {
    auto cfg = make_config({});
    BOOST_REQUIRE(!commitlog_disabled_for_user_tables(cfg->enable_commitlog(), cfg->enable_in_memory_data_store()));

    auto off = make_config({{"enable_commitlog", "false"}});
    BOOST_REQUIRE(commitlog_disabled_for_user_tables(off->enable_commitlog(), off->enable_in_memory_data_store()));
}

// A real second node breaks shipping at once too, and for its own reason
// rather than the unreadable-count one.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_second_node_breaks_at_once) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 1);
            start_sealing_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper&) {
                       the_probe.nodes = 2;
                   })
                    .get();
            tick_and_wait_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper& s) {
                       tests::require_equal(s.state(), shipping_state::off_multi_node);
                       tests::require(s.break_reason().has_value());
                       tests::require(s.break_reason()->find("node_count_unreadable") == seastar::sstring::npos);
                       tests::require(!s.sealing_scheduled());
                       tests::require_equal(the_probe.seals_started, 0);
                   })
                    .get();
        });
    });
}

// stop() has to wait for a seal that is in flight, and must not return while
// it still runs.
SEASTAR_THREAD_TEST_CASE(test_pitr_stop_waits_for_a_running_seal) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    the_probe = probe{};
    the_probe.hold.emplace();
    shipper.start_sealing(fake_seal(), fake_node_count());
    shipper.seal_tick();
    wait_until([] {
        return the_probe.seals_started == 1;
    });

    auto stopped = shipper.stop();
    BOOST_REQUIRE(!stopped.available());
    BOOST_REQUIRE(!shipper.sealing_scheduled());
    // A tick racing with the stop, with a seal running, owes no cycle that
    // could start after the stop.
    shipper.seal_tick();
    seastar::yield().get();
    BOOST_REQUIRE(!stopped.available());

    the_probe.hold->set_value();
    stopped.get();
    BOOST_REQUIRE_EQUAL(the_probe.seals_started, 1);
    BOOST_REQUIRE_EQUAL(the_probe.seals_finished, 1);
    // The follow-up the racing tick owed was dropped, so no second cycle read
    // the node count after the gate closed.
    BOOST_REQUIRE_EQUAL(the_probe.count_calls, 1);
    the_probe = probe{};
}

// After stop() with nothing running, a tick starts nothing.
SEASTAR_THREAD_TEST_CASE(test_pitr_tick_after_stop_does_nothing) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    the_probe = probe{};
    shipper.start_sealing(fake_seal(), fake_node_count());
    shipper.stop().get();

    shipper.seal_tick();
    shipper.wait_for_cycles(cycle_timeout).get();
    BOOST_REQUIRE_EQUAL(the_probe.seals_started, 0);
    BOOST_REQUIRE_EQUAL(the_probe.count_calls, 0);
    the_probe = probe{};
}

// A tick before start_sealing() has no seal to run and must not crash.
SEASTAR_THREAD_TEST_CASE(test_pitr_tick_before_start_sealing_does_nothing) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    the_probe = probe{};
    shipper.seal_tick();
    shipper.wait_for_cycles(cycle_timeout).get();
    BOOST_REQUIRE(!shipper.sealing_scheduled());
    BOOST_REQUIRE_EQUAL(the_probe.seals_started, 0);
    // Nothing ran at all: without the guard the empty count function is called,
    // and the cycle records a failure.
    BOOST_REQUIRE_EQUAL(the_probe.count_calls, 0);
    BOOST_REQUIRE(shipper.last_outcome() == pitr_shipper::cycle_outcome::none);
    BOOST_REQUIRE(!shipper.last_failure());
    shipper.stop().get();
}

// A second start_sealing() is ignored: arming an armed timer aborts, and the
// seal handed over first stays the one that runs.
SEASTAR_THREAD_TEST_CASE(test_pitr_second_start_sealing_is_ignored) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    the_probe = probe{};
    auto reset = seastar::defer([]() noexcept {
        the_probe = probe{};
    });
    shipper.start_sealing(fake_seal(), fake_node_count());
    const auto first_timeout = shipper.next_seal_time();

    int second_seal_calls = 0;
    shipper.start_sealing(
            [&second_seal_calls]() -> future<> {
                ++second_seal_calls;
                return make_ready_future<>();
            },
            [] {
                return make_ready_future<size_t>(1);
            });
    BOOST_REQUIRE(shipper.sealing_scheduled());
    BOOST_REQUIRE(shipper.next_seal_time() == first_timeout);

    shipper.seal_tick();
    shipper.wait_for_cycles(cycle_timeout).get();
    BOOST_REQUIRE_EQUAL(second_seal_calls, 0);
    BOOST_REQUIRE_EQUAL(the_probe.seals_finished, 1);
    shipper.stop().get();
}

// start_sealing() after stop() must not arm a timer, since no stop() is left to
// cancel it or to wait for what it starts.
SEASTAR_THREAD_TEST_CASE(test_pitr_start_sealing_after_stop_does_not_arm) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    the_probe = probe{};
    auto reset = seastar::defer([]() noexcept {
        the_probe = probe{};
    });
    shipper.stop().get();

    shipper.start_sealing(fake_seal(), fake_node_count());
    BOOST_REQUIRE(!shipper.sealing_scheduled());
    shipper.seal_tick();
    shipper.wait_for_cycles(cycle_timeout).get();
    BOOST_REQUIRE_EQUAL(the_probe.seals_started, 0);
    BOOST_REQUIRE_EQUAL(the_probe.count_calls, 0);
}

// A cycle stopped on its node count must not seal once stop() has begun: by
// then the commitlog is shutting down.
SEASTAR_THREAD_TEST_CASE(test_pitr_cycle_waiting_on_the_count_does_not_seal_after_stop) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    the_probe = probe{};
    auto reset = seastar::defer([]() noexcept {
        the_probe = probe{};
    });
    the_probe.count_hold.emplace();
    shipper.start_sealing(fake_seal(), fake_node_count());
    shipper.seal_tick();
    wait_until([] {
        return the_probe.count_calls == 1;
    });

    auto stopped = shipper.stop();
    BOOST_REQUIRE(!stopped.available());
    the_probe.count_hold->set_value();
    the_probe.count_hold.reset();
    stopped.get();
    BOOST_REQUIRE_EQUAL(the_probe.seals_started, 0);
}

// The coverage a cycle captures is taken before the cycle counts the nodes
// (P50): a count that takes a while, and a join that lands during it, must not
// move the coverage up past the writes the seal is about to close off.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_cycle_captures_its_coverage_before_it_counts) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    the_probe = probe{};
    auto reset = seastar::defer([]() noexcept {
        the_probe = probe{};
    });
    BOOST_REQUIRE_EQUAL(shipper.covered_through_us(), 0);

    const auto before = api::new_timestamp();
    the_probe.count_hold.emplace();
    shipper.start_sealing(fake_seal(), fake_node_count());
    shipper.seal_tick();
    wait_until([] {
        return the_probe.count_calls == 1;
    });

    // The count has not answered yet, so a capture taken after it would still
    // read 0 here.
    const auto captured = shipper.covered_through_us();
    const auto after = api::new_timestamp();
    BOOST_REQUIRE_GT(captured, 0);
    // The margin the design asks for: a capture that skipped it would sit
    // above this, and one that took it twice below the lower bound.
    BOOST_REQUIRE_LE(captured, after - coverage_margin.count());
    BOOST_REQUIRE_GE(captured, before - coverage_margin.count());

    the_probe.count_hold->set_value();
    the_probe.count_hold.reset();
    shipper.wait_for_cycles(cycle_timeout).get();
    shipper.stop().get();
    BOOST_REQUIRE_EQUAL(the_probe.seals_started, 1);
}

// The count fails only after stop() has begun, so shutdown must not latch
// shipping off: the state the node reports is the one it had while it ran.
SEASTAR_THREAD_TEST_CASE(test_pitr_unreadable_count_after_stop_does_not_break) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    the_probe = probe{};
    auto reset = seastar::defer([]() noexcept {
        the_probe = probe{};
    });
    the_probe.count = probe::count_mode::fails;
    shipper.start_sealing(fake_seal(), fake_node_count());

    // The cycle that would break shipping is held on its count until stop().
    the_probe.count_hold.emplace();
    shipper.seal_tick();
    wait_until([] {
        return the_probe.count_calls == 1;
    });
    auto stopped = shipper.stop();
    BOOST_REQUIRE(!stopped.available());
    the_probe.count_hold->set_value();
    the_probe.count_hold.reset();
    stopped.get();

    BOOST_REQUIRE_EQUAL(shipper.state(), shipping_state::active);
    BOOST_REQUIRE(!shipper.break_reason());
}

// The follow-up a tick owes does not run once the cycle that owed it turned
// shipping off: the cycle that breaks shipping is the last one.
SEASTAR_THREAD_TEST_CASE(test_pitr_no_follow_up_after_shipping_breaks) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    the_probe = probe{};
    auto reset = seastar::defer([]() noexcept {
        if (the_probe.count_hold) {
            the_probe.count_hold->set_value();
        }
        the_probe = probe{};
    });
    the_probe.count_hold.emplace();
    shipper.start_sealing(fake_seal(), fake_node_count());
    shipper.seal_tick();
    wait_until([] {
        return the_probe.count_calls == 1;
    });
    // A tick while the cycle is held owes a follow-up, which the break cancels.
    shipper.seal_tick();
    the_probe.nodes = 2;
    the_probe.count_hold->set_value();
    the_probe.count_hold.reset();
    shipper.wait_for_cycles(cycle_timeout).get();

    BOOST_REQUIRE_EQUAL(the_probe.count_calls, 1);
    BOOST_REQUIRE_EQUAL(the_probe.seals_started, 0);
    BOOST_REQUIRE_EQUAL(shipper.state(), shipping_state::off_multi_node);
    shipper.stop().get();
}

// A seal that fails still owes the follow-up a tick asked for: the write that
// landed in the new segment must not wait another interval.
SEASTAR_THREAD_TEST_CASE(test_pitr_follow_up_runs_after_a_failed_seal) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    the_probe = probe{};
    auto reset = seastar::defer([]() noexcept {
        if (the_probe.hold) {
            the_probe.hold->set_value();
        }
        the_probe = probe{};
    });
    the_probe.hold.emplace();
    the_probe.seal_fails = true;
    shipper.start_sealing(fake_seal(), fake_node_count());
    shipper.seal_tick();
    wait_until([] {
        return the_probe.seals_started == 1;
    });
    shipper.seal_tick();
    the_probe.hold->set_value();
    the_probe.hold.reset();
    shipper.wait_for_cycles(cycle_timeout).get();

    BOOST_REQUIRE_EQUAL(the_probe.seals_started, 2);
    BOOST_REQUIRE_EQUAL(the_probe.seals_finished, 0);
    BOOST_REQUIRE(shipper.last_outcome() == pitr_shipper::cycle_outcome::seal_failed);
    BOOST_REQUIRE(shipper.active());
    shipper.stop().get();
}

// A seal that throws is recorded, shipping stays on, and the next cycle runs.
// The fake seal here succeeds on its second call, which the real seal does not:
// force_new_active_segment() marks the segment closed before it syncs, so the
// cycle after a failed close seals nothing and reports success (see
// test_pitr_seal_of_a_shut_down_commitlog_reports_sealed). This test pins what
// the shipper does with a failing seal function, not a retry of a segment.
SEASTAR_THREAD_TEST_CASE(test_pitr_failed_seal_is_recorded_and_sealing_stays_on) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    int calls = 0;
    shipper.start_sealing(
            [&calls]() -> future<> {
                if (++calls == 1) {
                    return make_exception_future<>(std::runtime_error("seal failed"));
                }
                return make_ready_future<>();
            },
            [] {
                return make_ready_future<size_t>(1);
            });
    shipper.seal_tick();
    shipper.wait_for_cycles(cycle_timeout).get();
    BOOST_REQUIRE_EQUAL(calls, 1);
    BOOST_REQUIRE(shipper.last_outcome() == pitr_shipper::cycle_outcome::seal_failed);
    BOOST_REQUIRE(shipper.last_failure().has_value());
    BOOST_REQUIRE(shipper.active());

    shipper.seal_tick();
    shipper.wait_for_cycles(cycle_timeout).get();
    BOOST_REQUIRE_EQUAL(calls, 2);
    BOOST_REQUIRE(shipper.last_outcome() == pitr_shipper::cycle_outcome::sealed);
    BOOST_REQUIRE(!shipper.last_failure());
    shipper.stop().get();
}

namespace {

// A real commitlog, so that the seal under test is the one used in production.
future<db::commitlog> make_log(const seastar::sstring& path, std::vector<db::segment_id_type>& closed) {
    db::commitlog::config c;
    c.commit_log_location = path;
    c.commitlog_segment_size_in_mb = 1;
    c.commitlog_total_space_in_mb = 64 * this_smp_shard_count();
    c.use_o_dsync = false;
    c.segment_closed = [&closed](db::segment_id_type id, uint64_t, bool failed) {
        if (!failed) {
            closed.push_back(id);
        }
    };
    return db::commitlog::create_commitlog(std::move(c));
}

db::replay_position write_one(db::commitlog& log) {
    auto id = table_id(utils::UUID_gen::get_time_UUID());
    auto h = log.add_mutation(id, 64, db::commitlog::force_sync::no, [](db::commitlog::output& dst) {
                    dst.fill('x', 64);
                }).get();
    return h.rp();
}

} // namespace

// Written, sealed, then idle: further seals must create no segment and report
// no close, so that no empty segment is shipped. A sealed log has no active
// segment until the next write, so these seals return at the commitlog's
// "no segment" check. Its "segment still unused" check (position() == 0) can't
// be reached through the public API at rest, and is not covered here.
SEASTAR_THREAD_TEST_CASE(test_pitr_seal_of_idle_commitlog_makes_no_segment) {
    tmpdir dir;
    std::vector<db::segment_id_type> closed;
    auto log = make_log(dir.path().string(), closed).get();
    auto cleanup = seastar::defer([&log]() noexcept {
        log.shutdown().get();
        log.clear().get();
    });

    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    shipper.start_sealing(
            [&] {
                return log.force_new_active_segment();
            },
            [] {
                return make_ready_future<size_t>(1);
            });
    auto seal = [&] {
        shipper.seal_tick();
        shipper.wait_for_cycles(cycle_timeout).get();
    };

    write_one(log);
    seal();
    BOOST_REQUIRE_EQUAL(closed.size(), 1);
    BOOST_REQUIRE(log.get_active_segment_names().empty());
    const auto active_before = log.get_active_segment_names();

    seal();
    seal();
    log.sync_all_segments().get();
    BOOST_REQUIRE(log.get_active_segment_names() == active_before);
    BOOST_REQUIRE_EQUAL(closed.size(), 1);
    shipper.stop().get();
}

// One write is closed by the next seal, which is what makes it shippable.
SEASTAR_THREAD_TEST_CASE(test_pitr_seal_closes_a_segment_with_one_write) {
    tmpdir dir;
    std::vector<db::segment_id_type> closed;
    auto log = make_log(dir.path().string(), closed).get();
    auto cleanup = seastar::defer([&log]() noexcept {
        log.shutdown().get();
        log.clear().get();
    });

    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    shipper.start_sealing(
            [&] {
                return log.force_new_active_segment();
            },
            [] {
                return make_ready_future<size_t>(1);
            });

    const auto rp = write_one(log);
    BOOST_REQUIRE(closed.empty());

    shipper.seal_tick();
    shipper.wait_for_cycles(cycle_timeout).get();
    log.sync_all_segments().get();
    BOOST_REQUIRE_EQUAL(closed.size(), 1);
    BOOST_REQUIRE_EQUAL(closed.front(), rp.id);
    shipper.stop().get();
}

// A segment that exists but was never written to must not be sealed: the
// commitlog's position() == 0 skip. A write whose timeout has already passed
// leaves exactly that behind, because the segment the attempt installed stays
// in place unused. The write only times out when no reserve segment happens to
// be ready, which is a race against the commitlog's own reserve, so a log that
// accepted the write is thrown away and a fresh one is tried. Nothing here
// fails on losing that race: the test reports it and stops.
SEASTAR_THREAD_TEST_CASE(test_pitr_seal_skips_an_unused_segment) {
    tmpdir dir;
    std::vector<db::segment_id_type> closed;
    std::optional<db::commitlog> log;
    auto discard_log = [&log, &closed] {
        if (log) {
            log->shutdown().get();
            log->clear().get();
            log.reset();
            closed.clear();
        }
    };
    auto cleanup = seastar::defer([&discard_log]() noexcept {
        discard_log();
    });

    bool write_timed_out = false;
    for (int attempt = 0; attempt < 20 && !write_timed_out; ++attempt) {
        discard_log();
        log.emplace(make_log(dir.path().string(), closed).get());
        auto id = table_id(utils::UUID_gen::get_time_UUID());
        try {
            log->add_mutation(id, 64, db::timeout_clock::now() - std::chrono::seconds(1), db::commitlog::force_sync::no, [](db::commitlog::output& dst) {
                   dst.fill('x', 64);
               }).get();
        } catch (const seastar::timed_out_error&) {
            write_timed_out = true;
        }
    }
    if (!write_timed_out) {
        BOOST_TEST_MESSAGE("a reserve segment was ready for all 20 attempts, so no unused segment was left behind");
        return;
    }
    // The abandoned allocation still installs a segment; it appears once the
    // file has been created.
    wait_until([&] {
        return log->get_active_segment_names().size() == 1;
    });
    const auto names = log->get_active_segment_names();

    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    shipper.start_sealing(
            [&] {
                return log->force_new_active_segment();
            },
            [] {
                return make_ready_future<size_t>(1);
            });
    shipper.seal_tick();
    shipper.wait_for_cycles(cycle_timeout).get();
    log->sync_all_segments().get();

    // Nothing was closed and no replacement segment was made.
    BOOST_REQUIRE(closed.empty());
    BOOST_REQUIRE(log->get_active_segment_names() == names);
    BOOST_REQUIRE(shipper.last_outcome() == pitr_shipper::cycle_outcome::sealed);
    shipper.stop().get();
}

// A seal of a log that is already shut down returns, seals nothing, and is
// still reported as sealed: cycle_outcome::sealed says the seal function
// returned, no more. This is the state the shipper is in once the node's drain
// has run, and the same report the cycle after a failed close produces, because
// a failed close leaves the segment marked closed and force_new_active_segment()
// then has nothing active to seal. A genuinely failed close is not forced here:
// commitlog's close()/sync() path has no error-injection point, so it would
// take a real I/O failure.
SEASTAR_THREAD_TEST_CASE(test_pitr_seal_of_a_shut_down_commitlog_reports_sealed) {
    tmpdir dir;
    std::vector<db::segment_id_type> closed;
    auto log = make_log(dir.path().string(), closed).get();
    auto cleanup = seastar::defer([&log]() noexcept {
        log.clear().get();
    });

    write_one(log);
    log.shutdown().get();
    const auto closed_by_shutdown = closed.size();

    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    shipper.start_sealing(
            [&] {
                return log.force_new_active_segment();
            },
            [] {
                return make_ready_future<size_t>(1);
            });
    shipper.seal_tick();
    shipper.wait_for_cycles(cycle_timeout).get();

    BOOST_REQUIRE(shipper.last_outcome() == pitr_shipper::cycle_outcome::sealed);
    BOOST_REQUIRE(!shipper.last_failure());
    BOOST_REQUIRE_EQUAL(closed.size(), closed_by_shutdown);
    shipper.stop().get();
}

// Tests for the wiring of the close notification to the upload and the hold
// release (issue #8). The store is a fake object_storage_client whose bytes
// become visible only when the upload is finalized, as a real store's do, and
// the segments are real files on disk, so the upload path under test is the
// real one. The commitlog is not involved: the shipper takes the local file,
// the object name and the hold release as hooks, and the tests drive the
// notification by hand.
namespace {

// Bytes only a recycled tail holds, so one of them in the store proves the
// upload went past the used size.
constexpr char tail_byte = '\xd7';

class store_error : public std::runtime_error {
public:
    store_error()
        : std::runtime_error("injected store failure") {
    }
};

// What the fake store recorded and what it was told to do. Segment objects and
// ledger entries are counted apart, so that a test about one is not moved by
// the other going through the same store.
struct fake_store {
    // Only finalized objects, as a store's listing holds only those.
    std::unordered_map<std::string, std::string> objects;
    unsigned sinks_created = 0;
    unsigned puts = 0;
    // Fail every put of the first this-many sinks, one failed attempt each.
    unsigned fail_first_sinks = 0;
    // While set, a put does not return until it is resolved, which parks an
    // upload mid-flight.
    std::optional<seastar::shared_promise<>> hold_put;
    size_t sinks_open = 0;
    size_t max_sinks_open = 0;
    // The same three for the ledger's entries.
    unsigned entry_sinks_created = 0;
    unsigned entry_puts = 0;
    unsigned fail_first_entry_sinks = 0;
    std::optional<seastar::shared_promise<>> hold_entry_put;
    size_t entry_sinks_open = 0;
    size_t max_entry_sinks_open = 0;
    // Puts of one named object wait for their own promise, which is how one
    // segment's upload is parked while another finishes.
    std::unordered_map<std::string, seastar::shared_promise<>> holds;

    bool is_entry(const std::string& name) const {
        return name.ends_with(db::pitr::entry_key_suffix);
    }
    void hold_object(std::string name) {
        holds.try_emplace(std::move(name));
    }
    void let_object_go(const std::string& name) {
        if (auto it = holds.find(name); it != holds.end()) {
            it->second.set_value();
            holds.erase(it);
        }
    }
    // Lets every hold go, so that nothing is parked when a shipper has to stop.
    void let_everything_go() noexcept {
        for (auto& [name, hold] : holds) {
            hold.set_value();
        }
        holds.clear();
        for (auto* hold : {&hold_put, &hold_entry_put}) {
            if (*hold) {
                (*hold)->set_value();
                hold->reset();
            }
        }
    }
};

class fake_sink_impl final : public data_sink_impl {
    fake_store& _store;
    std::string _name;
    abort_source* _as;
    bool _is_entry;
    unsigned _index;
    std::string _pending;

public:
    fake_sink_impl(fake_store& store, std::string name, abort_source* as)
        : _store(store)
        , _name(std::move(name))
        , _as(as)
        , _is_entry(_store.is_entry(_name))
        , _index(_is_entry ? _store.entry_sinks_created++ : _store.sinks_created++) {
        if (_is_entry) {
            _store.max_entry_sinks_open = std::max(_store.max_entry_sinks_open, ++_store.entry_sinks_open);
        } else {
            _store.max_sinks_open = std::max(_store.max_sinks_open, ++_store.sinks_open);
        }
    }
    future<> put(std::span<temporary_buffer<char>> bufs) override {
        std::string bytes;
        for (auto& buf : bufs) {
            ++(_is_entry ? _store.entry_puts : _store.puts);
            bytes.append(buf.get(), buf.size());
        }
        if (_index < (_is_entry ? _store.fail_first_entry_sinks : _store.fail_first_sinks)) {
            return make_exception_future<>(store_error());
        }
        const auto& held = _is_entry ? _store.hold_entry_put : _store.hold_put;
        auto wait = held ? held->get_shared_future() : make_ready_future<>();
        if (auto it = _store.holds.find(_name); it != _store.holds.end()) {
            wait = wait.then([own = it->second.get_shared_future()]() mutable {
                return std::move(own);
            });
        }
        return wait.then([this, bytes = std::move(bytes)] {
            _pending.append(bytes);
        });
    }
    future<> flush() override {
        return make_ready_future<>();
    }
    future<> close() override {
        --(_is_entry ? _store.entry_sinks_open : _store.sinks_open);
        // GCS-style: the close is what finalizes, unless the upload was
        // aborted, in which case the sink abandons it.
        if (!_as || !_as->abort_requested()) {
            _store.objects[_name] = std::move(_pending);
        }
        return make_ready_future<>();
    }
    size_t buffer_size() const noexcept override {
        return db::pitr::upload_chunk_size;
    }
};

// Only make_upload_sink() is implemented: a test fails loudly if the code under
// test reaches for any other route into the store.
class fake_client final : public sstables::object_storage_client {
    fake_store& _store;

    static std::logic_error unused(const char* what) {
        return std::logic_error(seastar::format("fake_client: {} is not part of segment shipping", what));
    }

public:
    explicit fake_client(fake_store& store)
        : _store(store) {
    }
    data_sink make_upload_sink(sstables::object_name name, sstables::object_storage_attributes, abort_source* as) override {
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

// A segment on disk, as a shipped one is: `used_size` bytes a replay would
// read, followed by `tail_size` bytes of a recycled tail that must not ship.
seastar::sstring make_segment_file(const seastar::sstring& dir, db::segment_id_type id, size_t used_size, size_t tail_size = 0) {
    auto path = seastar::format("{}/CommitLog-4-{}.log", dir, id);
    std::string bytes(used_size, char('a' + id % 23));
    bytes.append(tail_size, tail_byte);
    auto f = seastar::open_file_dma(path, seastar::open_flags::create | seastar::open_flags::wo | seastar::open_flags::truncate).get();
    auto out = seastar::make_file_output_stream(std::move(f)).get();
    out.write(bytes.data(), bytes.size()).get();
    out.flush().get();
    out.close().get();
    return path;
}

std::string segment_object(db::segment_id_type id) {
    return sstables::object_name("pitr-bucket", seastar::format("chains/segments/CommitLog-4-{}.log", id)).str();
}

// A host id and a boot epoch of the wiring's making, fixed so that a chain key
// a test asserts on is stable.
const utils::UUID test_host_id = utils::UUID("b1e3f0d4-1111-4000-8000-000000000001");
constexpr int64_t test_boot_epoch_us = 1762300000000000LL;

db::pitr::chain_origin test_origin(std::optional<db::segment_id_type> floor = std::nullopt) {
    return db::pitr::chain_origin{
            .host_id = test_host_id,
            .boot_epoch_us = test_boot_epoch_us,
            .floor = floor,
    };
}

// The hooks the shipper ships through, plus what they recorded.
struct ship_probe {
    seastar::sstring dir;
    fake_store store;
    // The chain floor the shipper starts with; no floor by default, so that a
    // test that is not about the floor ships every segment it closes.
    std::optional<db::segment_id_type> floor;
    std::vector<db::segment_id_type> released;
    std::unordered_map<db::segment_id_type, seastar::sstring> paths;
    // Called before the id is recorded, so that a test can see what the store
    // held at the moment the hold was released. Must not throw, as the hook
    // must not.
    std::function<void(db::segment_id_type)> on_release;
    // Replaces the object name a segment is stored under, which is also where
    // the file name in its ledger entry comes from.
    std::function<sstables::object_name(db::segment_id_type)> name_of;

    db::pitr::segment_hooks hooks() {
        return db::pitr::segment_hooks{
                .open_segment =
                        [this](db::segment_id_type id) {
                            return seastar::open_file_dma(paths.at(id), seastar::open_flags::ro);
                        },
                .object_name_of =
                        [this](db::segment_id_type id) {
                            if (name_of) {
                                return name_of(id);
                            }
                            return sstables::object_name("pitr-bucket", seastar::format("chains/segments/CommitLog-4-{}.log", id));
                        },
        };
    }
    db::pitr::release_hook release_hook() {
        return [this](db::segment_id_type id) {
            if (on_release) {
                on_release(id);
            }
            released.push_back(id);
        };
    }
    const std::string& object_of(db::segment_id_type id) const {
        auto it = store.objects.find(segment_object(id));
        BOOST_REQUIRE_MESSAGE(it != store.objects.end(), seastar::format("no object for segment {}", id));
        return it->second;
    }
    bool has_object(db::segment_id_type id) const {
        return store.objects.contains(segment_object(id));
    }
};

// The ledger entries the store holds, in the sequence order a restore reads
// them in: the first missing sequence ends the chain, as chain_check does.
std::vector<db::pitr::segment_entry> entries_of(const ship_probe& probe, const db::pitr::chain_key_parts& chain) {
    std::vector<db::pitr::segment_entry> entries;
    for (uint64_t sequence = 0;; ++sequence) {
        const auto key = sstables::object_name("pitr-bucket", db::pitr::entry_object_key(chain, sequence)).str();
        auto it = probe.store.objects.find(key);
        if (it == probe.store.objects.end()) {
            return entries;
        }
        const auto& body = it->second;
        entries.push_back(db::pitr::decode_segment_entry(bytes_view(reinterpret_cast<const int8_t*>(body.data()), body.size())));
    }
}

// The segment ids the chain's entries name, in sequence order.
std::vector<db::segment_id_type> recorded_ids(const ship_probe& probe, const db::pitr::chain_key_parts& chain) {
    std::vector<db::segment_id_type> ids;
    for (const auto& entry : entries_of(probe, chain)) {
        ids.push_back(entry.segment_id);
    }
    return ids;
}

// An active shipper with the fake store installed, handed to the test body.
template <typename Func>
void with_shipping(ship_probe& probe, Func&& f) {
    tmpdir dir;
    probe.dir = dir.path().string();
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    auto stop = seastar::defer([&shipper, &probe]() noexcept {
        try {
            // A parked put would keep the gate from closing.
            probe.store.let_everything_go();
            shipper.stop().get();
        } catch (...) {
            BOOST_ERROR(seastar::format("stopping the shipper failed: {}", std::current_exception()));
        }
    });
    shipper.set_release_hook(probe.release_hook());
    shipper.start_shipping(seastar::make_shared<fake_client>(probe.store), probe.hooks(), test_origin(probe.floor));
    f(shipper);
}

// A few reactor turns, so that anything started in the background has had its
// chance to run. Used where the expectation is that nothing happens, which no
// condition can be polled for.
void settle() {
    for (int i = 0; i < 10; ++i) {
        seastar::yield().get();
    }
}

// Arms a seal cycle that counts a second node, so that the next seal_tick()
// breaks shipping. The tick does it where it is called, since the count is
// ready, which lets a sweep break from inside a reactor task.
void arm_a_break_with_a_second_node(pitr_shipper& shipper) {
    shipper.start_sealing(
            [] {
                return make_ready_future<>();
            },
            [] {
                return make_ready_future<size_t>(2);
            });
}

// Breaks shipping the way a seal cycle does, by counting a second node.
void break_shipping_with_a_second_node(pitr_shipper& shipper) {
    arm_a_break_with_a_second_node(shipper);
    shipper.seal_tick();
    shipper.wait_for_cycles(cycle_timeout).get();
    BOOST_REQUIRE(!shipper.active());
}

// The sweeps below walk one event - a rollback, a break - past the point a
// segment's object is finalized, one reactor turn at a time, so that it lands
// at every point of the upload's unwinding rather than at a timed one.
constexpr size_t sweep_used = 4096;
constexpr int sweep_turns = 10;

// Runs `report` from a task of its own, `turn` turns after the segment's object
// appears in the store. It has to be a task: the event must land between two of
// the upload's own hops, and this thread is scheduled after them. The task is
// stopped and waited for before the caller's locals go out of scope, including
// when an assertion or a wait_until timeout throws past this call.
template <typename Func>
void report_after_turns(ship_probe& probe, db::segment_id_type id, int turn, Func report) {
    bool stop = false;
    auto counting = seastar::do_with(-1, [&, id](int& counted) {
        return seastar::do_until(
                [&, id] {
                    if (stop) {
                        return true;
                    }
                    // The object appears when the sink closes, which is the
                    // last thing the upload does before it returns.
                    if (counted < 0 && probe.has_object(id)) {
                        counted = 0;
                    }
                    if (counted >= 0 && counted++ == turn) {
                        report();
                        return true;
                    }
                    return false;
                },
                [] {
                    return seastar::yield();
                });
    });
    auto finish = seastar::defer([&]() noexcept {
        stop = true;
        try {
            counting.get();
        } catch (...) {
            BOOST_ERROR(seastar::format("the counting task failed: {}", std::current_exception()));
        }
    });
    wait_until([&] {
        return counting.available();
    });
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_pitr_a_closed_segment_is_uploaded_and_then_released) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.paths[11] = make_segment_file(probe.dir, 11, used, 8192);
        // The hold may go only once the bytes are stored, never before.
        bool stored_when_released = false;
        probe.on_release = [&](db::segment_id_type id) {
            stored_when_released = probe.has_object(id);
        };
        shipper.segment_closed(11, used, false);
        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });

        BOOST_REQUIRE(stored_when_released);
        BOOST_REQUIRE_EQUAL(probe.object_of(11).size(), used);
        // The recycled tail is what a size-only check would miss.
        BOOST_REQUIRE(probe.object_of(11).find(tail_byte) == std::string::npos);
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{11});
        BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), 0u);
    });
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_segment_with_no_used_bytes_is_released_without_an_upload) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        probe.paths[12] = make_segment_file(probe.dir, 12, 0, 4096);
        shipper.segment_closed(12, 0, false);

        // The release is synchronous, so there is nothing to wait for: an
        // upload started here would show as a sink.
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{12});
        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
        BOOST_REQUIRE(probe.store.objects.empty());
        BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
    });
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_failed_upload_is_retried_and_then_released) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.store.fail_first_sinks = 2;
        probe.paths[13] = make_segment_file(probe.dir, 13, used);
        shipper.segment_closed(13, used, false);
        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });

        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 3u);
        BOOST_REQUIRE_EQUAL(probe.object_of(13).size(), used);
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{13});
    });
}

SEASTAR_THREAD_TEST_CASE(test_pitr_shutdown_during_an_upload_aborts_it_and_keeps_the_hold) {
    ship_probe probe;
    tmpdir dir;
    probe.dir = dir.path().string();
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    shipper.set_release_hook(probe.release_hook());
    shipper.start_shipping(seastar::make_shared<fake_client>(probe.store), probe.hooks(), test_origin(probe.floor));

    constexpr size_t used = 4096;
    // Every attempt fails, so the upload is waiting out a backoff when the
    // shutdown aborts it: a plain request_abort() there gives sleep_aborted.
    probe.store.fail_first_sinks = 1000;
    probe.paths[14] = make_segment_file(probe.dir, 14, used);
    shipper.segment_closed(14, used, false);
    wait_until([&] {
        return probe.store.sinks_created >= 1;
    });
    BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), 1u);

    shipper.stop().get();

    BOOST_REQUIRE(probe.released.empty());
    BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
    // An abort is not a permanent failure: counting it as one would report a
    // hole in the chain where the shutdown only stopped an upload.
    BOOST_REQUIRE_EQUAL(shipper.segments_unshipped(), 0u);
    BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), 0u);
    BOOST_REQUIRE(!probe.has_object(14));
}

// Shutting down with far more closes than can run at once: the ones holding a
// slot and the ones waiting for one are all aborted, and every one of their
// holds is kept, since the shutdown deletes the held segments anyway. A close
// that arrives once the shutdown has begun ships nothing and is released.
SEASTAR_THREAD_TEST_CASE(test_pitr_shutdown_with_many_uploads_keeps_every_hold) {
    ship_probe probe;
    tmpdir dir;
    probe.dir = dir.path().string();
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    shipper.set_release_hook(probe.release_hook());
    shipper.start_shipping(seastar::make_shared<fake_client>(probe.store), probe.hooks(), test_origin(probe.floor));

    constexpr size_t used = 4096;
    constexpr size_t segments = 32;
    const auto limit = shipper.max_concurrent_uploads();
    BOOST_REQUIRE_LT(limit, segments);
    for (size_t i = 0; i <= segments; ++i) {
        probe.paths[60 + i] = make_segment_file(probe.dir, 60 + i, used);
    }
    probe.store.hold_put.emplace();
    for (size_t i = 0; i < segments; ++i) {
        shipper.segment_closed(60 + i, used, false);
    }
    wait_until([&] {
        return probe.store.sinks_created == limit;
    });
    BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), segments);

    auto stopping = shipper.stop();
    shipper.segment_closed(60 + segments, used, false);
    // The parked put unwinds only once it is let go: an abort cannot cancel a
    // request the store is already serving.
    probe.store.hold_put->set_value();
    probe.store.hold_put.reset();
    stopping.get();

    // Only the close that arrived after the stop had begun: the aborted
    // uploads keep theirs.
    BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{60 + segments});
    BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
    BOOST_REQUIRE_EQUAL(shipper.segments_unshipped(), 0u);
    BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), 0u);
    BOOST_REQUIRE(probe.store.objects.empty());
}

SEASTAR_THREAD_TEST_CASE(test_pitr_segments_closing_out_of_order_are_each_shipped) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        // A natural close is not awaited, so closes arrive in any order.
        const std::vector<db::segment_id_type> order = {23, 21, 22};
        for (auto id : order) {
            probe.paths[id] = make_segment_file(probe.dir, id, 4096 * id);
        }
        for (auto id : order) {
            shipper.segment_closed(id, 4096 * id, false);
        }
        wait_until([&] {
            return shipper.segments_shipped() == order.size();
        });

        for (auto id : order) {
            BOOST_REQUIRE_EQUAL(probe.object_of(id).size(), 4096 * id);
        }
        auto released = probe.released;
        std::ranges::sort(released);
        BOOST_REQUIRE(released == (std::vector<db::segment_id_type>{21, 22, 23}));
    });
}

SEASTAR_THREAD_TEST_CASE(test_pitr_uploads_running_at_once_are_bounded) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        const auto limit = shipper.max_concurrent_uploads();
        const auto segments = limit + 4;
        probe.store.hold_put.emplace();
        for (size_t i = 0; i < segments; ++i) {
            const auto id = 30 + i;
            probe.paths[id] = make_segment_file(probe.dir, id, 4096);
            shipper.segment_closed(id, 4096, false);
        }
        // Every close started an upload; only `limit` of them hold a slot.
        BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), segments);
        wait_until([&] {
            return probe.store.sinks_created == limit;
        });
        BOOST_REQUIRE_EQUAL(probe.store.max_sinks_open, limit);

        probe.store.hold_put->set_value();
        probe.store.hold_put.reset();
        wait_until([&] {
            return shipper.segments_shipped() == segments;
        });
        BOOST_REQUIRE_EQUAL(probe.released.size(), segments);
        BOOST_REQUIRE_EQUAL(probe.store.max_sinks_open, limit);
    });
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_segment_whose_close_failed_is_not_shipped) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        probe.paths[15] = make_segment_file(probe.dir, 15, 4096);
        // The commitlog has dropped the hold itself, so releasing it again
        // would be releasing a segment that no longer exists.
        shipper.segment_closed(15, 4096, true);

        // No upload was started at all, which is what the wait below would
        // otherwise let finish quietly.
        BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), 0u);
        settle();
        BOOST_REQUIRE(probe.released.empty());
        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
        BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
        BOOST_REQUIRE_EQUAL(shipper.segments_unshipped(), 1u);
    });
}

// A rollback erases a segment whose close was already reported, and reports it
// again as failed. The upload still in flight must be aborted: its file may be
// gone, and a success recorded for it would point the chain at nothing
// (commitlog.hh's contract for failed=true).
SEASTAR_THREAD_TEST_CASE(test_pitr_a_rollback_during_an_upload_is_not_recorded) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.paths[19] = make_segment_file(probe.dir, 19, used);
        probe.store.hold_put.emplace();
        shipper.segment_closed(19, used, false);
        wait_until([&] {
            return probe.store.puts >= 1;
        });
        shipper.segment_closed(19, used, true);

        probe.store.hold_put->set_value();
        probe.store.hold_put.reset();
        wait_until([&] {
            return shipper.uploads_in_flight() == 0;
        });
        settle();
        BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
        // The commitlog dropped the hold itself, so there is nothing to release.
        BOOST_REQUIRE(probe.released.empty());
        BOOST_REQUIRE_EQUAL(shipper.segments_unshipped(), 1u);
        BOOST_REQUIRE(!probe.has_object(19));
    });
}

// The same rollback, but arriving in the gap between the upload finishing and
// the shipper resuming to record it, which segment_upload.hh says the caller
// has to check for itself. The sweep walks the rollback one reactor turn at a
// time past the point the object is finalized, so it lands at every point of
// the unwinding rather than at a timed one.
// A fresh shipper per turn, because the rollback ends the chain for the rest of
// the node's life.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_rollback_as_an_upload_finishes_is_not_recorded) {
    unsigned shipped_turns = 0;
    for (int turn = 0; turn < sweep_turns; ++turn) {
        ship_probe probe;
        with_shipping(probe, [&](pitr_shipper& shipper) {
            const db::segment_id_type id = 1000 + turn;
            probe.paths[id] = make_segment_file(probe.dir, id, sweep_used);
            shipper.segment_closed(id, sweep_used, false);
            report_after_turns(probe, id, turn, [&] {
                shipper.segment_closed(id, sweep_used, true);
            });
            wait_until([&] {
                return shipper.uploads_in_flight() == 0 && shipper.segments_pending() == 0;
            });
            settle();

            // The rollback ends the chain whichever side of the recording it
            // fell on: the segment's writes are gone from the commitlog, so
            // nothing above it may claim to cover them.
            BOOST_REQUIRE_EQUAL(shipper.state(), shipping_state::off_chain_broken);
            BOOST_REQUIRE_EQUAL(shipper.segments_unshipped(), 1u);
            // A recorded segment and a released hold go together: either both
            // happened before the rollback was reported, or neither did.
            // Checking only the release would let a success recorded without
            // one pass.
            const auto shipped = unsigned(shipper.segments_shipped());
            BOOST_REQUIRE_LE(shipped, 1u);
            BOOST_REQUIRE_EQUAL(unsigned(probe.released.size()), shipped);
            // The entry's own object can be in the store even when the abort
            // beat the recording, since a put that has finalized cannot be
            // taken back; what must never happen is an entry for another
            // segment, which would read as the chain carrying on past this one.
            const auto recorded = recorded_ids(probe, *shipper.chain());
            BOOST_REQUIRE(recorded.empty() || recorded == std::vector<db::segment_id_type>{id});
            shipped_turns += shipped;
        });
    }
    BOOST_TEST_MESSAGE(seastar::format("turns that recorded the segment before the rollback: {} of {}", shipped_turns, sweep_turns));
    // A sweep where every turn recorded first would never reach the window this
    // test exists for.
    BOOST_REQUIRE_LT(shipped_turns, unsigned(sweep_turns));
}

// The same sweep with a break in place of the rollback, which takes the other
// side of the gap check: whenever the break lands, the hold is released exactly
// once - by the upload if it had already stored the bytes, by the gap check if
// it had not - and a segment the break caught is never recorded as shipped. A
// fresh shipper per turn, because a break latches for the node's life.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_break_as_an_upload_finishes_releases_the_hold_once) {
    unsigned shipped_turns = 0;
    // Turns where the bytes were stored but the break caught the shipper before
    // it recorded them: the gap this check exists for.
    unsigned gap_turns = 0;
    for (int turn = 0; turn < sweep_turns; ++turn) {
        ship_probe probe;
        with_shipping(probe, [&](pitr_shipper& shipper) {
            const db::segment_id_type id = 1100 + turn;
            // Armed from this thread, because waiting out the cycle would
            // itself miss the gap; the tick below breaks where it is called.
            arm_a_break_with_a_second_node(shipper);
            probe.paths[id] = make_segment_file(probe.dir, id, sweep_used);
            shipper.segment_closed(id, sweep_used, false);
            report_after_turns(probe, id, turn, [&] {
                shipper.seal_tick();
            });
            wait_until([&] {
                return !shipper.active() && shipper.uploads_in_flight() == 0;
            });
            settle();

            // Released exactly once, whichever side of the gap the break fell,
            // and nothing else released behind the test's back.
            BOOST_REQUIRE_EQUAL(unsigned(std::ranges::count(probe.released, id)), 1u);
            BOOST_REQUIRE_EQUAL(probe.released.size(), 1u);
            const auto shipped = shipper.segments_shipped();
            BOOST_REQUIRE_LE(shipped, 1u);
            shipped_turns += unsigned(shipped);
            if (shipped == 0 && probe.has_object(id)) {
                ++gap_turns;
            }
        });
    }
    BOOST_TEST_MESSAGE(seastar::format("turns that shipped before the break: {} of {}, in the gap: {}", shipped_turns, sweep_turns, gap_turns));
    // A sweep where every turn shipped first, or none reached the store, would
    // never reach the gap check this test exists for.
    BOOST_REQUIRE_LT(shipped_turns, unsigned(sweep_turns));
    BOOST_REQUIRE_GT(gap_turns, 0u);
}

// An upload that no retry can fix leaves a hole in the chain, so shipping
// breaks and the hold goes rather than pinning the segment for good.
// Provisional, pending the decision on the outage policy (#11).
SEASTAR_THREAD_TEST_CASE(test_pitr_an_upload_that_fails_for_good_breaks_shipping) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        // A rollback can delete a segment whose close was already reported,
        // and a missing file is never retried.
        probe.paths[16] = seastar::format("{}/CommitLog-4-16.log", probe.dir);
        shipper.segment_closed(16, 4096, false);
        wait_until([&] {
            return shipper.segments_unshipped() == 1;
        });

        BOOST_REQUIRE(!shipper.active());
        BOOST_REQUIRE(shipper.state() == shipping_state::off_upload_failed);
        BOOST_REQUIRE(shipper.break_reason());
        BOOST_REQUIRE_EQUAL(shipper.break_reason()->find("upload_failed:"), 0u);
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{16});
        BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
    });
}

// A break ends the chain, so the segments whose uploads it aborts are let go
// instead of staying held for the rest of the node's life. Provisional,
// pending the decision on the outage policy (#11).
SEASTAR_THREAD_TEST_CASE(test_pitr_a_break_during_uploads_releases_their_holds) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.store.hold_put.emplace();
        for (db::segment_id_type id : {40, 41}) {
            probe.paths[id] = make_segment_file(probe.dir, id, used);
            shipper.segment_closed(id, used, false);
        }
        BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), 2u);

        break_shipping_with_a_second_node(shipper);
        probe.store.hold_put->set_value();
        probe.store.hold_put.reset();
        wait_until([&] {
            return probe.released.size() == 2;
        });

        auto released = probe.released;
        std::ranges::sort(released);
        BOOST_REQUIRE(released == (std::vector<db::segment_id_type>{40, 41}));
        BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
        BOOST_REQUIRE(!probe.has_object(40));
        BOOST_REQUIRE(!probe.has_object(41));
    });
}

// A close reported after the break: nothing is uploaded, and the hold goes at
// once, since no chain is left to keep the segment for.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_after_a_break_is_released_without_an_upload) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        break_shipping_with_a_second_node(shipper);
        probe.paths[43] = make_segment_file(probe.dir, 43, 4096);
        shipper.segment_closed(43, 4096, false);

        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{43});
        BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), 0u);
        settle();
        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
        BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
    });
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_repeated_close_does_not_start_a_second_upload) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.paths[17] = make_segment_file(probe.dir, 17, used);
        probe.store.hold_put.emplace();
        shipper.segment_closed(17, used, false);
        shipper.segment_closed(17, used, false);
        BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), 1u);

        probe.store.hold_put->set_value();
        probe.store.hold_put.reset();
        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });
        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 1u);
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{17});
    });
}

// A repeated close that reports no used bytes for a segment whose upload is
// still running must not release its hold: the file would be recycled under
// the upload. The hold goes once, when the bytes are stored.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_repeated_close_with_no_used_bytes_does_not_release_early) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.paths[20] = make_segment_file(probe.dir, 20, used);
        probe.store.hold_put.emplace();
        shipper.segment_closed(20, used, false);
        wait_until([&] {
            return probe.store.puts >= 1;
        });
        shipper.segment_closed(20, 0, false);

        settle();
        BOOST_REQUIRE(probe.released.empty());
        BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), 1u);

        probe.store.hold_put->set_value();
        probe.store.hold_put.reset();
        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{20});
        BOOST_REQUIRE_EQUAL(probe.object_of(20).size(), used);
    });
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_without_a_release_hook_does_nothing) {
    ship_probe probe;
    tmpdir dir;
    probe.dir = dir.path().string();
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    auto stop = seastar::defer([&shipper]() noexcept {
        shipper.stop().get();
    });

    // Neither the hook nor the client is installed, which is a node that takes
    // no hold at all: there is nothing to release and nothing to ship.
    probe.paths[18] = make_segment_file(probe.dir, 18, 4096);
    shipper.segment_closed(18, 4096, false);
    shipper.segment_closed(18, 0, false);

    BOOST_REQUIRE(probe.released.empty());
    BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
}

// A shipper with the release hook installed and no store client, which is what
// the wiring leaves behind from startup until start_shipping() runs. Issue #8:
// the commitlog's hold is fixed when it is built, so these closes carry a hold
// that only this shipper can drop.
template <typename Func>
void with_a_shipper_not_shipping_yet(ship_probe& probe, Func&& f) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    auto stop = seastar::defer([&shipper]() noexcept {
        try {
            shipper.stop().get();
        } catch (...) {
            BOOST_ERROR(seastar::format("stopping the shipper failed: {}", std::current_exception()));
        }
    });
    shipper.set_release_hook(probe.release_hook());
    f(shipper);
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_while_waiting_for_the_cluster_size_releases_the_hold) {
    ship_probe probe;
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        BOOST_REQUIRE_EQUAL(shipper.state(), shipping_state::waiting_for_cluster_size);
        shipper.segment_closed(7, 4096, false);
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{7});
        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
    });
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_while_shipping_is_refused_releases_the_hold) {
    ship_probe probe;
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        shipper.note_cluster_size(2);
        BOOST_REQUIRE_EQUAL(shipper.state(), shipping_state::off_multi_node);
        shipper.segment_closed(8, 4096, false);
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{8});
    });
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_after_a_break_before_shipping_releases_the_hold) {
    ship_probe probe;
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        shipper.note_cluster_size(1);
        break_shipping_with_a_second_node(shipper);
        shipper.segment_closed(9, 4096, false);
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{9});
    });
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_failed_close_before_shipping_releases_nothing) {
    ship_probe probe;
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        // A failed close is the commitlog's to drop, and it has: the shipper
        // has no hold left to release.
        shipper.segment_closed(10, 4096, true);
        BOOST_REQUIRE(probe.released.empty());
    });
}

// A rollback reports a segment that was already reported as closed, and the
// one before shipping started has been released already. Each close releases,
// because commitlog::release_segment() of an id that is not held is a no-op
// and the shipper cannot tell the two apart.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_repeated_close_before_shipping_releases_again) {
    ship_probe probe;
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        shipper.segment_closed(11, 4096, false);
        shipper.segment_closed(11, 4096, false);
        BOOST_REQUIRE((probe.released == std::vector<db::segment_id_type>{11, 11}));
    });
}

// Once shipping starts, a close is shipped rather than released at once, which
// is piece 1's behaviour: installing the hook earlier does not change it.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_once_shipping_started_is_still_uploaded_first) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.paths[12] = make_segment_file(probe.dir, 12, used);
        probe.store.hold_put.emplace();
        shipper.segment_closed(12, used, false);
        wait_until([&] {
            return probe.store.puts >= 1;
        });
        BOOST_REQUIRE(probe.released.empty());

        probe.store.hold_put->set_value();
        probe.store.hold_put.reset();
        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{12});
    });
}

// The chain a shard ships into: its identity, and the floor that keeps a
// segment written before it out of it.

// The identity appears when start_shipping() takes effect, and nothing changes
// it afterwards: a chain belongs to one shard of one boot.
SEASTAR_THREAD_TEST_CASE(test_pitr_the_chain_identity_is_set_when_shipping_starts) {
    ship_probe probe;
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        // A chain here would name coverage nothing is shipping for.
        BOOST_REQUIRE(!shipper.chain());
        BOOST_REQUIRE(!shipper.chain_floor());

        const auto before = api::new_timestamp();
        shipper.note_cluster_size(1);
        shipper.start_shipping(seastar::make_shared<fake_client>(probe.store), probe.hooks(), test_origin(5));
        BOOST_REQUIRE(shipper.chain());
        const auto chain = *shipper.chain();
        BOOST_REQUIRE_EQUAL(chain.prefix, "chains");
        BOOST_REQUIRE_EQUAL(chain.host_id, test_host_id);
        BOOST_REQUIRE_EQUAL(chain.shard, seastar::this_shard_id());
        BOOST_REQUIRE_EQUAL(chain.boot_epoch_us, test_boot_epoch_us);
        BOOST_REQUIRE_GE(chain.chain_start_us, before);
        BOOST_REQUIRE(chain.chain != chain_id::create_null_id());
        BOOST_REQUIRE_EQUAL(shipper.chain_floor().value(), db::segment_id_type(5));

        // A second call is ignored, floor and identity included.
        shipper.start_shipping(seastar::make_shared<fake_client>(probe.store), probe.hooks(), test_origin(9));
        BOOST_REQUIRE(*shipper.chain() == chain);
        BOOST_REQUIRE_EQUAL(shipper.chain_floor().value(), db::segment_id_type(5));
    });
}

// Two shippers never share a chain id: a restore walks one chain, and two
// chains under one id would read as a single unbroken run of segments.
SEASTAR_THREAD_TEST_CASE(test_pitr_two_shippers_get_different_chains) {
    ship_probe first;
    ship_probe second;
    with_shipping(first, [&](pitr_shipper& a) {
        with_shipping(second, [&](pitr_shipper& b) {
            BOOST_REQUIRE(a.chain());
            BOOST_REQUIRE(b.chain());
            BOOST_REQUIRE(a.chain()->chain != b.chain()->chain);
        });
    });
}

// Below the floor is the time before the chain. Closes are not ordered by id,
// so such a close arrives after shipping has started; shipping it would put a
// segment in the chain that the chain's own start does not cover.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_below_the_chain_floor_is_released_and_not_shipped) {
    ship_probe probe;
    probe.floor = 30;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.paths[29] = make_segment_file(probe.dir, 29, used);
        shipper.segment_closed(29, used, false);

        // The release is synchronous, so an upload started here would show.
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{29});
        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
        BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
        BOOST_REQUIRE(!probe.has_object(29));
        // Not in the chain, so no entry claims it either.
        BOOST_REQUIRE(entries_of(probe, *shipper.chain()).empty());
        BOOST_REQUIRE_EQUAL(shipper.segments_pending(), 0u);
    });
}

// The floor itself is in the chain: it is the segment the chain starts at.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_at_the_chain_floor_is_shipped) {
    ship_probe probe;
    probe.floor = 30;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.paths[30] = make_segment_file(probe.dir, 30, used);
        probe.paths[31] = make_segment_file(probe.dir, 31, used);
        shipper.segment_closed(30, used, false);
        shipper.segment_closed(31, used, false);
        // Nothing has yielded since the closes, so a floor that released the
        // segment at the floor shows here, rather than only as a wait that
        // never finishes below.
        BOOST_REQUIRE(probe.released.empty());
        BOOST_REQUIRE_EQUAL(shipper.uploads_in_flight(), 2u);

        wait_until([&] {
            return shipper.segments_shipped() == 2;
        });

        BOOST_REQUIRE_EQUAL(probe.object_of(30).size(), used);
        BOOST_REQUIRE_EQUAL(probe.object_of(31).size(), used);
    });
}

// A failed close below the floor has no hold left either: the commitlog has
// dropped it, so releasing again is not the shipper's to do.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_failed_close_below_the_chain_floor_releases_nothing) {
    ship_probe probe;
    probe.floor = 30;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        shipper.segment_closed(29, 4096, true);

        BOOST_REQUIRE(probe.released.empty());
        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
        // Not a hole in the chain: the segment was written before the chain
        // began, so it was never going to be in it.
        BOOST_REQUIRE_EQUAL(shipper.segments_unshipped(), 0u);
    });
}

// A close that arrives once stop() has begun: the chain ends at the last
// segment shipped before it, and the hold goes rather than being left pinned.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_after_stop_began_is_released_and_not_shipped) {
    ship_probe probe;
    tmpdir dir;
    probe.dir = dir.path().string();
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    shipper.set_release_hook(probe.release_hook());
    shipper.start_shipping(seastar::make_shared<fake_client>(probe.store), probe.hooks(), test_origin());

    // Nothing is in flight, so the stop is done; the release hook outlives it.
    shipper.stop().get();
    constexpr size_t used = 4096;
    probe.paths[40] = make_segment_file(probe.dir, 40, used);
    shipper.segment_closed(40, used, false);

    BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{40});
    BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
    BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
    // Nothing is recorded once stop() has begun: the chain ends at the last
    // entry written before it.
    BOOST_REQUIRE(entries_of(probe, *shipper.chain()).empty());
}

// The ledger entries the recorder writes: one per shipped segment, in segment
// id order, dense from sequence 0, with the hold released once the entry is in
// the store (issue #9).

namespace {

// Drives seal cycles by hand with a seal that can be parked, so that a close
// can be placed inside a cycle or between two of them. One node, so no cycle
// breaks shipping.
struct seal_driver {
    int seals = 0;
    std::optional<seastar::shared_promise<>> hold;

    void install(pitr_shipper& shipper) {
        shipper.start_sealing(
                [this]() -> future<> {
                    ++seals;
                    if (hold) {
                        co_await hold->get_shared_future();
                    }
                },
                [] {
                    return make_ready_future<size_t>(1);
                });
    }
    // One cycle, run to its end, which captures the coverage every close from
    // then on carries.
    int64_t run_a_cycle(pitr_shipper& shipper) {
        const auto before = seals;
        shipper.seal_tick();
        shipper.wait_for_cycles(cycle_timeout).get();
        BOOST_REQUIRE_EQUAL(seals, before + 1);
        BOOST_REQUIRE(shipper.active());
        return shipper.covered_through_us();
    }
    void let_the_seal_go() {
        if (hold) {
            hold->set_value();
            hold.reset();
        }
    }
};

uint32_t crc32_of(const std::string& bytes) {
    utils::crc32 crc;
    crc.process(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    return crc.get();
}

} // namespace

// Everything a restore reads out of one entry: the segment it names, the chain
// it belongs to and the coverage it carries.
SEASTAR_THREAD_TEST_CASE(test_pitr_an_entry_describes_its_segment_and_its_chain) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        seal_driver seals;
        seals.install(shipper);
        const auto coverage = seals.run_a_cycle(shipper);

        constexpr size_t used = 8192;
        probe.paths[61] = make_segment_file(probe.dir, 61, used, 4096);
        shipper.segment_closed(61, used, false);
        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });

        const auto entries = entries_of(probe, *shipper.chain());
        BOOST_REQUIRE_EQUAL(entries.size(), 1u);
        const auto& entry = entries.front();
        BOOST_REQUIRE_EQUAL(entry.sequence, 0u);
        BOOST_REQUIRE_EQUAL(entry.segment_id, 61u);
        // The chain's first entry names no segment before it.
        BOOST_REQUIRE_EQUAL(entry.prev_segment_id, 0u);
        BOOST_REQUIRE_EQUAL(entry.used_size, used);
        // The checksum covers the bytes that were stored, so the recycled tail
        // the file also holds must not be in it.
        BOOST_REQUIRE_EQUAL(entry.segment_crc32, crc32_of(probe.object_of(61)));
        BOOST_REQUIRE_EQUAL(entry.covered_through_us, coverage);
        BOOST_REQUIRE_GE(entry.upload_time_us, coverage);
        // The file name a replay derives the segment's base position from, and
        // which names the segment's own object.
        BOOST_REQUIRE_EQUAL(entry.file_name, "CommitLog-4-61.log");
        // The identity the chain reader checks the key against.
        const auto& chain = *shipper.chain();
        BOOST_REQUIRE_EQUAL(entry.chain, chain.chain);
        BOOST_REQUIRE_EQUAL(entry.host_id, chain.host_id);
        BOOST_REQUIRE_EQUAL(entry.shard, chain.shard);
        BOOST_REQUIRE_EQUAL(entry.boot_epoch_us, chain.boot_epoch_us);
        BOOST_REQUIRE_EQUAL(entry.chain_start_us, chain.chain_start_us);
    });
}

// The sequence is dense from 0 and every entry links to the segment of the one
// before it: a restore has nothing else to tell it what the chain holds.
SEASTAR_THREAD_TEST_CASE(test_pitr_the_sequence_is_dense_and_the_entries_are_linked) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        const std::vector<db::segment_id_type> ids = {70, 73, 74};
        for (auto id : ids) {
            probe.paths[id] = make_segment_file(probe.dir, id, used);
            shipper.segment_closed(id, used, false);
        }
        wait_until([&] {
            return shipper.segments_shipped() == ids.size();
        });

        const auto entries = entries_of(probe, *shipper.chain());
        BOOST_REQUIRE_EQUAL(entries.size(), ids.size());
        uint64_t prev = 0;
        for (size_t i = 0; i < entries.size(); ++i) {
            BOOST_REQUIRE_EQUAL(entries[i].sequence, i);
            BOOST_REQUIRE_EQUAL(entries[i].segment_id, ids[i]);
            // Ids have gaps, so the link is the only thing that says which
            // segment comes before this one.
            BOOST_REQUIRE_EQUAL(entries[i].prev_segment_id, prev);
            prev = entries[i].segment_id;
        }
    });
}

// A segment that filled up and rotated on its own closes with no cycle in
// between, so its entry carries the coverage of the last capture and not a
// newer one: the writes that went into the segment after it are not covered.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_self_rotated_segment_takes_its_predecessors_coverage) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        seal_driver seals;
        seals.install(shipper);
        const auto first = seals.run_a_cycle(shipper);
        for (db::segment_id_type id : {80, 81}) {
            probe.paths[id] = make_segment_file(probe.dir, id, used);
        }
        // 80 closed because the cycle sealed it; 81 because it filled up, with
        // no cycle of its own.
        shipper.segment_closed(80, used, false);
        shipper.segment_closed(81, used, false);
        wait_until([&] {
            return shipper.segments_shipped() == 2;
        });
        // Only now does the next cycle move the coverage on.
        const auto second = seals.run_a_cycle(shipper);
        BOOST_REQUIRE_GT(second, first);
        probe.paths[82] = make_segment_file(probe.dir, 82, used);
        shipper.segment_closed(82, used, false);
        wait_until([&] {
            return shipper.segments_shipped() == 3;
        });

        const auto entries = entries_of(probe, *shipper.chain());
        BOOST_REQUIRE_EQUAL(entries.size(), 3u);
        BOOST_REQUIRE_EQUAL(entries[0].covered_through_us, first);
        BOOST_REQUIRE_EQUAL(entries[1].covered_through_us, first);
        BOOST_REQUIRE_EQUAL(entries[2].covered_through_us, second);
    });
}

// A segment that rotates while a seal cycle is in flight may carry that
// cycle's capture, because the capture was taken before the close: every write
// acknowledged by then is in this segment or in one below it. A segment that
// closed before the cycle may not.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_rotation_while_a_seal_is_pending_carries_that_cycles_capture) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        seal_driver seals;
        seals.install(shipper);
        const auto first = seals.run_a_cycle(shipper);
        probe.paths[90] = make_segment_file(probe.dir, 90, used);
        shipper.segment_closed(90, used, false);
        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });

        // The next cycle is parked inside its seal, so this close lands after
        // the capture and before the cycle ends.
        seals.hold.emplace();
        shipper.seal_tick();
        wait_until([&] {
            return seals.seals == 2;
        });
        const auto second = shipper.covered_through_us();
        BOOST_REQUIRE_GT(second, first);
        probe.paths[91] = make_segment_file(probe.dir, 91, used);
        shipper.segment_closed(91, used, false);
        wait_until([&] {
            return shipper.segments_shipped() == 2;
        });
        seals.let_the_seal_go();
        shipper.wait_for_cycles(cycle_timeout).get();

        const auto entries = entries_of(probe, *shipper.chain());
        BOOST_REQUIRE_EQUAL(entries.size(), 2u);
        // The segment that closed before the cycle keeps the older capture.
        BOOST_REQUIRE_EQUAL(entries[0].covered_through_us, first);
        BOOST_REQUIRE_EQUAL(entries[1].covered_through_us, second);
    });
}

// The coverage an entry carries is the capture its close was reported under,
// not the one in force when the entry is written: a capture taken after the
// close can cover a write that went into the segment above this one, and an
// entry claiming it would leave that write outside a restore that stops here.
SEASTAR_THREAD_TEST_CASE(test_pitr_an_entry_keeps_the_capture_its_close_was_reported_under) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        seal_driver seals;
        seals.install(shipper);
        const auto first = seals.run_a_cycle(shipper);
        probe.paths[95] = make_segment_file(probe.dir, 95, used);
        // Parked in its upload, so the next cycle's capture lands between the
        // close and the entry being built.
        probe.store.hold_object(segment_object(95));
        shipper.segment_closed(95, used, false);
        wait_until([&] {
            return probe.store.puts >= 1;
        });
        const auto second = seals.run_a_cycle(shipper);
        BOOST_REQUIRE_GT(second, first);

        probe.store.let_object_go(segment_object(95));
        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });

        const auto entries = entries_of(probe, *shipper.chain());
        BOOST_REQUIRE_EQUAL(entries.size(), 1u);
        BOOST_REQUIRE_EQUAL(entries[0].covered_through_us, first);
    });
}

// Closes are not ordered by id, so a lower segment can be reported after a
// higher one and under a newer capture. Its entry comes first, and its coverage
// is held down to what the segments above it carry: a restore that stopped at
// it would otherwise miss writes the higher segment holds.
SEASTAR_THREAD_TEST_CASE(test_pitr_coverage_never_passes_a_segment_still_to_be_recorded) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        seal_driver seals;
        seals.install(shipper);
        const auto first = seals.run_a_cycle(shipper);
        for (db::segment_id_type id : {96, 97}) {
            probe.paths[id] = make_segment_file(probe.dir, id, used);
            probe.store.hold_object(segment_object(id));
        }
        // 97 closed under the first capture; nothing is recorded yet, because
        // its upload is parked.
        shipper.segment_closed(97, used, false);
        wait_until([&] {
            return probe.store.puts >= 1;
        });
        const auto second = seals.run_a_cycle(shipper);
        BOOST_REQUIRE_GT(second, first);
        // 96 is reported late, so it carries the newer capture.
        shipper.segment_closed(96, used, false);
        wait_until([&] {
            return probe.store.puts >= 2;
        });

        probe.store.let_everything_go();
        wait_until([&] {
            return shipper.segments_shipped() == 2;
        });

        const auto entries = entries_of(probe, *shipper.chain());
        BOOST_REQUIRE_EQUAL(entries.size(), 2u);
        BOOST_REQUIRE_EQUAL(entries[0].segment_id, 96u);
        BOOST_REQUIRE_EQUAL(entries[1].segment_id, 97u);
        BOOST_REQUIRE_EQUAL(entries[0].covered_through_us, first);
        BOOST_REQUIRE_EQUAL(entries[1].covered_through_us, first);
        // Never backwards, whatever order the closes arrived in.
        BOOST_REQUIRE_LE(entries[0].covered_through_us, entries[1].covered_through_us);
    });
}

// The bytes are in the store and the entry is not: the hold has to stay, or a
// restore would find a segment no entry names.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_hold_is_kept_while_the_entry_put_retries) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.store.fail_first_entry_sinks = 2;
        probe.paths[65] = make_segment_file(probe.dir, 65, used);
        shipper.segment_closed(65, used, false);
        wait_until([&] {
            return probe.store.entry_sinks_created >= 2;
        });

        BOOST_REQUIRE(probe.has_object(65));
        BOOST_REQUIRE(probe.released.empty());
        BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
        BOOST_REQUIRE_EQUAL(shipper.segments_pending(), 1u);

        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });
        BOOST_REQUIRE_EQUAL(probe.store.entry_sinks_created, 3u);
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{65});
        BOOST_REQUIRE_EQUAL(recorded_ids(probe, *shipper.chain()).size(), 1u);
    });
}

// An entry no writer may produce - here a segment whose key carries no file
// name - is not something a retry can fix: the chain ends at the entry before
// it, nothing above it is recorded, and every hold goes (D25).
SEASTAR_THREAD_TEST_CASE(test_pitr_an_entry_that_cannot_be_written_breaks_shipping) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.name_of = [](db::segment_id_type id) {
            return sstables::object_name("pitr-bucket", seastar::format("chains/segments/{}/", id));
        };
        for (db::segment_id_type id : {66, 67}) {
            probe.paths[id] = make_segment_file(probe.dir, id, used);
            shipper.segment_closed(id, used, false);
        }
        wait_until([&] {
            return !shipper.active();
        });
        settle();

        BOOST_REQUIRE_EQUAL(shipper.state(), shipping_state::off_ledger_failed);
        BOOST_REQUIRE(shipper.break_reason());
        BOOST_REQUIRE_EQUAL(shipper.break_reason()->find("ledger_failed:"), 0u);
        BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
        BOOST_REQUIRE_EQUAL(shipper.segments_unshipped(), 1u);
        // Refused before any store request, so nothing half written is left.
        BOOST_REQUIRE_EQUAL(probe.store.entry_sinks_created, 0u);
        BOOST_REQUIRE(entries_of(probe, *shipper.chain()).empty());
        // Both holds go: the one that failed and the one that was waiting for
        // its turn in the order.
        auto released = probe.released;
        std::ranges::sort(released);
        BOOST_REQUIRE(released == (std::vector<db::segment_id_type>{66, 67}));
        BOOST_REQUIRE_EQUAL(shipper.segments_pending(), 0u);
    });
}

// "Shipped, then rolled back": the commitlog reports a segment that is already
// in the chain as failed, so its writes are gone and nothing above it may
// cover them. The entry already written stays - it is correct about bytes that
// are in the store - and no later entry is ever added.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_rollback_of_a_recorded_segment_breaks_the_chain) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.paths[75] = make_segment_file(probe.dir, 75, used);
        shipper.segment_closed(75, used, false);
        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });

        shipper.segment_closed(75, used, true);
        BOOST_REQUIRE_EQUAL(shipper.state(), shipping_state::off_chain_broken);
        BOOST_REQUIRE(shipper.break_reason()->find("chain_broken:") == 0u);

        probe.paths[76] = make_segment_file(probe.dir, 76, used);
        shipper.segment_closed(76, used, false);
        settle();

        BOOST_REQUIRE(recorded_ids(probe, *shipper.chain()) == std::vector<db::segment_id_type>{75});
        BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 1u);
        BOOST_REQUIRE(!probe.has_object(76));
        // 75's hold went with its entry; 76 is released at once after a break.
        BOOST_REQUIRE(probe.released == (std::vector<db::segment_id_type>{75, 76}));
    });
}

// A close that arrives below the last entry recorded cannot be appended in
// order, and leaving it out would let the entries above it cover writes it
// holds: the chain ends instead.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_below_the_last_recorded_entry_breaks_the_chain) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        probe.paths[78] = make_segment_file(probe.dir, 78, used);
        probe.paths[77] = make_segment_file(probe.dir, 77, used);
        shipper.segment_closed(78, used, false);
        wait_until([&] {
            return shipper.segments_shipped() == 1;
        });

        shipper.segment_closed(77, used, false);
        BOOST_REQUIRE_EQUAL(shipper.state(), shipping_state::off_chain_broken);
        settle();

        BOOST_REQUIRE(recorded_ids(probe, *shipper.chain()) == std::vector<db::segment_id_type>{78});
        BOOST_REQUIRE(!probe.has_object(77));
        // Nothing is left to keep the segment for, so its hold goes at once.
        BOOST_REQUIRE(probe.released == (std::vector<db::segment_id_type>{78, 77}));
    });
}

// stop() with an entry put in flight: it waits the put out, writes no entry,
// and keeps the hold, since the shutdown deletes the held segments anyway.
SEASTAR_THREAD_TEST_CASE(test_pitr_stop_during_an_entry_put_writes_no_entry_and_keeps_the_hold) {
    ship_probe probe;
    tmpdir dir;
    probe.dir = dir.path().string();
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    shipper.set_release_hook(probe.release_hook());
    shipper.start_shipping(seastar::make_shared<fake_client>(probe.store), probe.hooks(), test_origin(probe.floor));

    constexpr size_t used = 4096;
    probe.store.hold_entry_put.emplace();
    probe.paths[85] = make_segment_file(probe.dir, 85, used);
    shipper.segment_closed(85, used, false);
    wait_until([&] {
        return probe.store.entry_puts >= 1;
    });
    BOOST_REQUIRE(probe.released.empty());

    auto stopping = shipper.stop();
    settle();
    // The recorder runs inside the gate stop() waits for, so it cannot be left
    // writing into a shipper that is gone.
    BOOST_REQUIRE(!stopping.available());
    probe.store.let_everything_go();
    stopping.get();

    BOOST_REQUIRE_EQUAL(shipper.segments_shipped(), 0u);
    BOOST_REQUIRE(probe.released.empty());
    BOOST_REQUIRE(entries_of(probe, *shipper.chain()).empty());
    BOOST_REQUIRE_EQUAL(shipper.segments_pending(), 0u);
}

// Uploads finish in the order the store answers in, which is not the order the
// segments were written in; the entries must go out in segment id order, since
// a restore reads them as one run with no gap.
SEASTAR_THREAD_TEST_CASE(test_pitr_entries_are_written_in_segment_id_order) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        constexpr size_t used = 4096;
        const std::vector<db::segment_id_type> order = {52, 50, 51};
        for (auto id : order) {
            probe.paths[id] = make_segment_file(probe.dir, id, used);
        }
        // The lowest id's upload is parked, so the two above it finish first
        // and have to wait for it before any entry can be written.
        probe.store.hold_object(segment_object(50));
        for (auto id : order) {
            shipper.segment_closed(id, used, false);
        }
        wait_until([&] {
            return probe.has_object(51) && probe.has_object(52);
        });
        settle();
        BOOST_REQUIRE(recorded_ids(probe, *shipper.chain()).empty());
        BOOST_REQUIRE(probe.released.empty());

        probe.store.let_object_go(segment_object(50));
        wait_until([&] {
            return shipper.segments_shipped() == 3;
        });

        BOOST_REQUIRE(recorded_ids(probe, *shipper.chain()) == (std::vector<db::segment_id_type>{50, 51, 52}));
        // A hold goes with its own entry, so the releases follow the same order.
        BOOST_REQUIRE(probe.released == (std::vector<db::segment_id_type>{50, 51, 52}));
        // One entry at a time: two puts in flight could leave the later entry
        // in the store while the earlier one failed.
        BOOST_REQUIRE_EQUAL(probe.store.max_entry_sinks_open, 1u);
    });
}

// What a snapshot's bound file reads off this shard: the chain, and the floor
// below which a bound is refused (D27, D29).
SEASTAR_THREAD_TEST_CASE(test_pitr_the_chain_provider_answers_from_the_shipper) {
    ship_probe probe;
    probe.floor = 33;
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        auto provider = make_chain_provider(shipper);
        // Nothing before the chain exists: a snapshot taken now is not
        // protected, and the bound file has to say so rather than name a chain.
        BOOST_REQUIRE(!provider());

        shipper.note_cluster_size(1);
        shipper.start_shipping(seastar::make_shared<fake_client>(probe.store), probe.hooks(), test_origin(probe.floor));
        const auto chain = provider();
        BOOST_REQUIRE(chain);
        BOOST_REQUIRE_EQUAL(chain->host_id, test_host_id);
        BOOST_REQUIRE_EQUAL(chain->boot_epoch_us, test_boot_epoch_us);
        BOOST_REQUIRE_EQUAL(chain->chain, shipper.chain()->chain);
        BOOST_REQUIRE_EQUAL(chain->chain_start_us, shipper.chain()->chain_start_us);
        BOOST_REQUIRE(chain->floor_segment_id == std::optional<uint64_t>(33));
    });
}

// No segment was held when shipping started, so the chain has no floor: the
// D27 check is vacuous, and 0 must not stand in for it (D29).
SEASTAR_THREAD_TEST_CASE(test_pitr_the_chain_provider_reports_no_floor_when_nothing_was_held) {
    ship_probe probe;
    with_shipping(probe, [&](pitr_shipper& shipper) {
        const auto chain = make_chain_provider(shipper)();
        BOOST_REQUIRE(chain);
        BOOST_REQUIRE(!chain->floor_segment_id);
    });
}

// The keys a shipped segment is stored under, which a restore has to find.

namespace {

// Stops a shipper exactly once: stop() may not be called twice, and a test
// that stops it itself must still be unwound if an assertion throws first.
class stop_once {
    pitr_shipper& _shipper;
    bool _stopped = false;

public:
    explicit stop_once(pitr_shipper& shipper)
        : _shipper(shipper) {
    }
    void stop() {
        if (!std::exchange(_stopped, true)) {
            _shipper.stop().get();
        }
    }
    // For a test that called stop() itself.
    void took_effect() noexcept {
        _stopped = true;
    }
    ~stop_once() {
        try {
            stop();
        } catch (...) {
            BOOST_ERROR(seastar::format("stopping the shipper failed: {}", std::current_exception()));
        }
    }
};

// An active shipper with a chain and no upload of its own, for the hooks and
// the flush. The body gets the guard, so that it can stop the shipper itself.
template <typename Func>
void with_an_active_shipper(ship_probe& probe, Func&& f) {
    auto cfg = make_config(hand_driven_config());
    pitr_shipper shipper(*cfg);
    shipper.start().get();
    shipper.note_cluster_size(1);
    stop_once stopping(shipper);
    shipper.set_release_hook(probe.release_hook());
    shipper.start_shipping(seastar::make_shared<fake_client>(probe.store), probe.hooks(), test_origin(probe.floor));
    f(shipper, stopping);
}

} // namespace

// A segment is stored under its own chain's key, keeping its file name, which
// is what carries its id to a restore.
SEASTAR_THREAD_TEST_CASE(test_pitr_the_hooks_store_a_segment_under_its_chain) {
    ship_probe probe;
    tmpdir dir;
    probe.dir = dir.path().string();
    with_an_active_shipper(probe, [&](pitr_shipper& shipper, stop_once&) {
        const auto path = make_segment_file(probe.dir, 42, 4096);
        auto hooks = make_segment_hooks(shipper, [&](db::segment_id_type id) -> std::optional<seastar::sstring> {
            if (id != 42) {
                return std::nullopt;
            }
            return path;
        });

        const auto name = hooks.object_name_of(42);
        BOOST_REQUIRE_EQUAL(std::string(name.bucket()), std::string("pitr-bucket"));
        BOOST_REQUIRE_EQUAL(std::string(name.object()), std::string(segment_object_key(*shipper.chain(), "CommitLog-4-42.log")));
        // The key is under this chain's own directory, not just somewhere in
        // the bucket: another chain's restore must not read it.
        BOOST_REQUIRE(name.object().starts_with(chain_key_prefix(*shipper.chain())));

        // The handle is a fresh one on that file, not the commitlog's.
        auto f = hooks.open_segment(42).get();
        auto close_f = seastar::defer([&f]() noexcept {
            f.close().get();
        });
        BOOST_REQUIRE_EQUAL(f.size().get(), 4096u);

        // A segment the commitlog no longer holds cannot be shipped, and both
        // hooks say so rather than making up a name or a path.
        BOOST_REQUIRE_THROW(hooks.object_name_of(43), std::runtime_error);
        BOOST_REQUIRE_THROW(hooks.open_segment(43).get(), std::runtime_error);
    });
}

// The hooks are installed before the chain exists in the wiring, so they must
// refuse to name an object rather than store one outside any chain.
SEASTAR_THREAD_TEST_CASE(test_pitr_the_hooks_refuse_to_name_an_object_without_a_chain) {
    ship_probe probe;
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        auto hooks = make_segment_hooks(shipper, [](db::segment_id_type) {
            return std::optional<seastar::sstring>("/tmp/CommitLog-4-1.log");
        });
        BOOST_REQUIRE(!shipper.chain());
        BOOST_REQUIRE_THROW(hooks.object_name_of(1), std::runtime_error);
    });
}

// The one flush the wiring owes once shipping has started, so that a table
// nothing writes to again cannot keep a later backup's bound below the chain.

SEASTAR_THREAD_TEST_CASE(test_pitr_the_flush_after_start_runs_and_stop_waits_for_it) {
    ship_probe probe;
    with_an_active_shipper(probe, [&](pitr_shipper& shipper, stop_once& stopping) {
        int flushes = 0;
        seastar::shared_promise<> hold;
        shipper.flush_after_start([&]() {
            ++flushes;
            return hold.get_shared_future();
        });
        wait_until([&] {
            return flushes == 1;
        });

        auto stopped = shipper.stop();
        settle();
        // The flush runs inside the gate stop() waits for, so it cannot be
        // left running after the shipper it belongs to is gone.
        BOOST_REQUIRE(!stopped.available());
        hold.set_value();
        stopped.get();
        stopping.took_effect();
        BOOST_REQUIRE_EQUAL(flushes, 1);
    });
}

// A flush is an optimisation for a later backup's bound, not part of the
// chain: a failure is logged and shipping goes on.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_failed_flush_after_start_breaks_nothing) {
    ship_probe probe;
    with_an_active_shipper(probe, [&](pitr_shipper& shipper, stop_once&) {
        bool ran = false;
        shipper.flush_after_start([&]() -> future<> {
            ran = true;
            co_await seastar::yield();
            throw std::runtime_error("flushing failed");
        });
        wait_until([&] {
            return ran;
        });
        settle();

        BOOST_REQUIRE(shipper.active());
        BOOST_REQUIRE(!shipper.break_reason());
    });
}

// Nothing to keep a bound for: a shipper that is not shipping must not run the
// node's startup flush behind the operator's back.
SEASTAR_THREAD_TEST_CASE(test_pitr_the_flush_after_start_is_dropped_while_shipping_is_off) {
    ship_probe probe;
    int flushes = 0;
    auto flush = [&flushes]() {
        ++flushes;
        return make_ready_future<>();
    };
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        // Waiting for the cluster size, then refused for its node count.
        shipper.flush_after_start(flush);
        shipper.note_cluster_size(2);
        shipper.flush_after_start(flush);
        settle();
        BOOST_REQUIRE_EQUAL(flushes, 0);
    });

    // And once the shipper has stopped, where the gate would reject it anyway.
    with_an_active_shipper(probe, [&](pitr_shipper& shipper, stop_once& stopping) {
        stopping.stop();
        shipper.flush_after_start(flush);
        settle();
        BOOST_REQUIRE_EQUAL(flushes, 0);
    });
}

// pitr_endpoint naming an endpoint the node was not configured with: there is
// nowhere to ship, so shipping is refused with a reason naming the option.

SEASTAR_THREAD_TEST_CASE(test_pitr_an_unknown_endpoint_refuses_shipping) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 1);
            start_sealing_on_all_shards(shipper);
            require_state_on_all_shards(shipper, shipping_state::active);

            shipper.invoke_on_all(&pitr_shipper::note_endpoint_unknown).get();
            require_state_on_all_shards(shipper, shipping_state::off_unknown_endpoint);
            shipper.invoke_on_all([](pitr_shipper& s) {
                       tests::require(s.break_reason().has_value());
                       tests::require(s.break_reason()->find("pitr_endpoint") != seastar::sstring::npos);
                       tests::require(s.break_reason()->find("restart") != seastar::sstring::npos);
                       // Nothing to seal for, and the refusal is latched.
                       tests::require(!s.sealing_scheduled());
                       s.note_cluster_size(1);
                       tests::require_equal(s.state(), shipping_state::off_unknown_endpoint);
                   })
                    .get();
            tick_and_wait_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper&) {
                       tests::require_equal(the_probe.seals_started, 0);
                   })
                    .get();
        });
    });
}

// A configured endpoint whose client cannot be built: shipping is off with the
// cause in the reason, and the node is left to carry on.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_store_client_that_cannot_be_built_refuses_shipping) {
    ship_probe probe;
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        shipper.note_cluster_size(1);
        shipper.note_store_client_failed("endpoint http://localhost:9000 not found");

        BOOST_REQUIRE_EQUAL(shipper.state(), shipping_state::off_store_client_failed);
        BOOST_REQUIRE(shipper.break_reason().has_value());
        BOOST_REQUIRE(shipper.break_reason()->find("not found") != seastar::sstring::npos);
        BOOST_REQUIRE(shipper.break_reason()->find("restart") != seastar::sstring::npos);
        // Latched, and a close after it releases its hold at once.
        shipper.note_cluster_size(1);
        BOOST_REQUIRE_EQUAL(shipper.state(), shipping_state::off_store_client_failed);
        shipper.segment_closed(45, 4096, false);
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{45});
        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
    });
}

// A refusal that came first stands: the reason the operator is shown must be
// the one that stopped shipping.
SEASTAR_THREAD_TEST_CASE(test_pitr_an_unknown_endpoint_does_not_overwrite_an_earlier_refusal) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        note_cluster_size_on_all_shards(shipper, 2);
        require_state_on_all_shards(shipper, shipping_state::off_multi_node);
        shipper.invoke_on_all(&pitr_shipper::note_endpoint_unknown).get();
        require_state_on_all_shards(shipper, shipping_state::off_multi_node);
    });
}

// A close after the refusal is released at once, as after any other break:
// there is no chain left to pin a segment for.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_close_after_an_unknown_endpoint_releases_the_hold) {
    ship_probe probe;
    with_a_shipper_not_shipping_yet(probe, [&](pitr_shipper& shipper) {
        shipper.note_cluster_size(1);
        shipper.note_endpoint_unknown();
        shipper.segment_closed(44, 4096, false);
        BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{44});
        BOOST_REQUIRE_EQUAL(probe.store.sinks_created, 0u);
    });
}

// The slot the commitlog's close notification goes through.

namespace {

// What the slot did: the ids it released directly, and the closes it forwarded.
struct slot_probe {
    std::vector<db::segment_id_type> released;
    std::vector<std::tuple<db::segment_id_type, uint64_t, bool>> forwarded;

    db::pitr::segment_closed_slot make_slot() {
        return db::pitr::segment_closed_slot([this](db::segment_id_type id) {
            released.push_back(id);
        });
    }
    segment_closed_slot::handler handler() {
        return [this](db::segment_id_type id, uint64_t used_size, bool failed) {
            forwarded.emplace_back(id, used_size, failed);
        };
    }
};

} // namespace

SEASTAR_THREAD_TEST_CASE(test_pitr_an_empty_slot_releases_the_hold_directly) {
    slot_probe probe;
    auto slot = probe.make_slot();
    BOOST_REQUIRE(!slot.filled());
    slot.segment_closed(3, 4096, false);
    BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{3});
    BOOST_REQUIRE(probe.forwarded.empty());
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_failed_close_with_an_empty_slot_releases_nothing) {
    slot_probe probe;
    auto slot = probe.make_slot();
    slot.segment_closed(4, 4096, true);
    BOOST_REQUIRE(probe.released.empty());
}

SEASTAR_THREAD_TEST_CASE(test_pitr_a_filled_slot_forwards_the_close_and_releases_nothing) {
    slot_probe probe;
    auto slot = probe.make_slot();
    slot.fill(probe.handler());
    BOOST_REQUIRE(slot.filled());
    slot.segment_closed(5, 8192, false);
    BOOST_REQUIRE(probe.released.empty());
    BOOST_REQUIRE_EQUAL(probe.forwarded.size(), 1u);
    BOOST_REQUIRE(probe.forwarded[0] == std::make_tuple(db::segment_id_type(5), uint64_t(8192), false));
}

// A rollback has to reach the consumer: it is what aborts the upload of a
// segment whose file the rollback may have erased.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_filled_slot_forwards_a_failed_close) {
    slot_probe probe;
    auto slot = probe.make_slot();
    slot.fill(probe.handler());
    slot.segment_closed(6, 4096, true);
    BOOST_REQUIRE(probe.released.empty());
    BOOST_REQUIRE_EQUAL(probe.forwarded.size(), 1u);
    BOOST_REQUIRE(probe.forwarded[0] == std::make_tuple(db::segment_id_type(6), uint64_t(4096), true));
}

// Cleared before the consumer stops, which is where a close reported during
// the shutdown lands.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_cleared_slot_falls_back_to_the_direct_release) {
    slot_probe probe;
    auto slot = probe.make_slot();
    slot.fill(probe.handler());
    slot.clear();
    BOOST_REQUIRE(!slot.filled());
    slot.segment_closed(7, 4096, false);
    BOOST_REQUIRE(probe.released == std::vector<db::segment_id_type>{7});
    BOOST_REQUIRE(probe.forwarded.empty());
}

// Whether the commitlog holds its segments at all. It is decided once, when the
// commitlog is built, so anything but a usable store with a commitlog to ship
// from must leave the commitlog as it was.
SEASTAR_THREAD_TEST_CASE(test_pitr_segments_are_held_only_for_a_usable_store_and_commitlog) {
    struct test_case {
        const char* what;
        std::vector<option> options;
        bool expected;
    };
    const std::vector<test_case> cases = {
            {"no PITR options at all", {}, false},
            {"only the endpoint", {{"pitr_endpoint", "http://localhost:9000"}}, false},
            {"a seal interval out of range", full_config(option{"pitr_seal_interval_in_seconds", "0"}), false},
            {"a valid store", full_config(), true},
            {"a valid store with the commitlog off", full_config_with(option{"enable_commitlog", "false"}), false},
            {"a valid store with an in-memory data store", full_config_with(option{"enable_in_memory_data_store", "true"}), false},
    };
    for (const auto& c : cases) {
        auto cfg = make_config(c.options);
        BOOST_REQUIRE_MESSAGE(db::pitr::should_hold_segments(*cfg) == c.expected, c.what);
    }
}
