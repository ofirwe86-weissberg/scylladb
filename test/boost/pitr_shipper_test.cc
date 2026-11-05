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
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include <seastar/core/format.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "db/config.hh"
#include "db/pitr/pitr_shipper.hh"
#include "test/lib/test_utils.hh"

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
