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
#include <chrono>
#include <optional>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <seastar/core/format.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "db/pitr/pitr_shipper.hh"
#include "service/topology_state_machine.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/tmpdir.hh"
#include "utils/UUID_gen.hh"

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

// A node count that cannot be read, by throwing or by a failed future, seals
// nothing, leaves shipping on, records the failure, and the next cycle works.
SEASTAR_THREAD_TEST_CASE(test_pitr_unreadable_node_count_does_not_seal_or_turn_off) {
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
                           tests::require(s.active());
                           tests::require(s.sealing_scheduled());
                           tests::require(!s.break_reason());
                           tests::require(s.last_outcome() == pitr_shipper::cycle_outcome::count_failed);
                           tests::require(s.last_failure().has_value());
                           // The next cycle recovers, and clears the failure.
                           the_probe.count = probe::count_mode::ok;
                           s.seal_tick();
                       })
                        .get();
                wait_for_cycles_on_all_shards(shipper);
                shipper.invoke_on_all([](pitr_shipper& s) {
                           tests::require_equal(the_probe.seals_started, 1);
                           tests::require(s.last_outcome() == pitr_shipper::cycle_outcome::sealed);
                           tests::require(!s.last_failure());
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

// A run of unreadable counts shorter than the allowance costs the user no
// protection: shipping stays on, and the run is reported as it grows.
SEASTAR_THREAD_TEST_CASE(test_pitr_unreadable_counts_below_the_allowance_keep_shipping) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 1);
            start_sealing_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper&) {
                       the_probe.count = probe::count_mode::fails;
                   })
                    .get();

            for (unsigned cycle = 1; cycle < max_unreadable_count_cycles; ++cycle) {
                tick_and_wait_on_all_shards(shipper);
                shipper.invoke_on_all([cycle](pitr_shipper& s) {
                           tests::require(s.active());
                           tests::require(s.sealing_scheduled());
                           tests::require(!s.break_reason());
                           tests::require(s.last_outcome() == pitr_shipper::cycle_outcome::count_failed);
                           tests::require_equal(s.unreadable_count_cycles(), cycle);
                           tests::require_equal(the_probe.seals_started, 0);
                       })
                        .get();
            }
        });
    });
}

// Once the count has been unreadable for the whole allowance, the chain can no
// longer be shown to come from one node, so shipping breaks and says so.
SEASTAR_THREAD_TEST_CASE(test_pitr_unreadable_count_breaks_shipping_at_the_allowance) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 1);
            start_sealing_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper&) {
                       the_probe.count = probe::count_mode::fails;
                   })
                    .get();

            for (unsigned cycle = 0; cycle < max_unreadable_count_cycles; ++cycle) {
                tick_and_wait_on_all_shards(shipper);
            }
            shipper.invoke_on_all([](pitr_shipper& s) {
                       tests::require_equal(s.state(), shipping_state::off_node_count_unreadable);
                       tests::require(s.last_outcome() == pitr_shipper::cycle_outcome::shipping_broke);
                       tests::require(s.break_reason().has_value());
                       tests::require(s.break_reason()->find("node_count_unreadable") != seastar::sstring::npos);
                       tests::require(!s.sealing_scheduled());
                       tests::require_equal(the_probe.seals_started, 0);
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

// Only an unbroken run breaks shipping: one cycle that reads the count puts the
// allowance back, so failures spread over a long run never add up.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_readable_count_resets_the_unreadable_run) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 1);
            start_sealing_on_all_shards(shipper);

            for (int run = 0; run < 2; ++run) {
                shipper.invoke_on_all([](pitr_shipper&) {
                           the_probe.count = probe::count_mode::fails;
                       })
                        .get();
                for (unsigned cycle = 1; cycle < max_unreadable_count_cycles; ++cycle) {
                    tick_and_wait_on_all_shards(shipper);
                }
                shipper.invoke_on_all([](pitr_shipper& s) {
                           tests::require_equal(s.unreadable_count_cycles(), max_unreadable_count_cycles - 1);
                           tests::require(s.active());
                           the_probe.count = probe::count_mode::ok;
                       })
                        .get();
                tick_and_wait_on_all_shards(shipper);
                shipper.invoke_on_all([](pitr_shipper& s) {
                           tests::require_equal(s.unreadable_count_cycles(), 0u);
                           tests::require(s.active());
                           tests::require(s.last_outcome() == pitr_shipper::cycle_outcome::sealed);
                       })
                        .get();
            }
        });
    });
}

// A real second node is not covered by the allowance for an unreadable count:
// the cycle that counts it breaks shipping at once, for the right reason.
SEASTAR_THREAD_TEST_CASE(test_pitr_a_second_node_breaks_during_an_unreadable_run) {
    auto cfg = make_config(hand_driven_config());

    with_shipper(*cfg, [](seastar::sharded<pitr_shipper>& shipper) {
        with_probe(shipper, [&] {
            note_cluster_size_on_all_shards(shipper, 1);
            start_sealing_on_all_shards(shipper);
            shipper.invoke_on_all([](pitr_shipper&) {
                       the_probe.count = probe::count_mode::fails;
                   })
                    .get();
            tick_and_wait_on_all_shards(shipper);

            shipper.invoke_on_all([](pitr_shipper& s) {
                       tests::require(s.active());
                       the_probe.count = probe::count_mode::ok;
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

// The count that would reach the allowance fails only after stop() has begun,
// so shutdown must not latch shipping off: the state the node reports is the
// one it had while it was running.
SEASTAR_THREAD_TEST_CASE(test_pitr_unreadable_count_at_the_allowance_after_stop_does_not_break) {
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
    for (unsigned cycle = 1; cycle < max_unreadable_count_cycles; ++cycle) {
        shipper.seal_tick();
        shipper.wait_for_cycles(cycle_timeout).get();
    }
    BOOST_REQUIRE_EQUAL(shipper.unreadable_count_cycles(), max_unreadable_count_cycles - 1);

    // The cycle that would break shipping is held on its count until stop().
    the_probe.count_hold.emplace();
    shipper.seal_tick();
    wait_until([] {
        return the_probe.count_calls == int(max_unreadable_count_cycles);
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

// A node count that could not be read also leaves the follow-up owed: the next
// cycle may well read it, and a write is waiting on it.
SEASTAR_THREAD_TEST_CASE(test_pitr_follow_up_runs_after_a_failed_count) {
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
    shipper.seal_tick();
    the_probe.count = probe::count_mode::fails;
    the_probe.count_hold->set_value();
    the_probe.count_hold.reset();
    shipper.wait_for_cycles(cycle_timeout).get();

    BOOST_REQUIRE_EQUAL(the_probe.count_calls, 2);
    BOOST_REQUIRE_EQUAL(the_probe.seals_started, 0);
    BOOST_REQUIRE(shipper.last_outcome() == pitr_shipper::cycle_outcome::count_failed);
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
