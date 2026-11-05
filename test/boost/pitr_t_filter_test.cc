/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the PITR "T filter" (issue #16, requirement RST-2): the per-cell
// filter which reduces a mutation to the writes at or below T.
//
// The matrix cases below cover every kind of timestamped item separately, at
// T - 1, T and T + 1. The property test at the end compares the filter against
// an answer computed independently of the filter's own traversal: it is built
// by visiting the source partition with mutation_partition::accept() and
// re-applying, through the public mutation API, every item whose timestamp is
// at or below T.

// Defines the test entry point; must come before anything that pulls in
// seastar's own testing main.
#include "test/lib/scylla_test_case.hh"

#include <algorithm>
#include <vector>

#include <boost/test/unit_test.hpp>

#include <seastar/core/thread.hh>

#include "db/pitr/t_filter.hh"
#include "keys/keys.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition.hh"
#include "mutation/mutation_partition_visitor.hh"
#include "schema/schema_builder.hh"
#include "test/lib/log.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/random_utils.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/user.hh"
#include "utils/UUID_gen.hh"

using namespace db::pitr;

namespace {

constexpr api::timestamp_type T = 1000;
static const gc_clock::time_point dt = gc_clock::time_point(gc_clock::duration(12345));

static data_type udt_type() {
    return user_type_impl::get_instance("ks", to_bytes("ut"), { to_bytes("f1"), to_bytes("f2") },
            { utf8_type, utf8_type }, true);
}

static schema_ptr test_schema() {
    return schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck1", utf8_type, column_kind::clustering_key)
            .with_column("ck2", utf8_type, column_kind::clustering_key)
            .with_column("st", utf8_type, column_kind::static_column)
            .with_column("stm", map_type_impl::get_instance(utf8_type, utf8_type, true), column_kind::static_column)
            .with_column("v", utf8_type)
            .with_column("m", map_type_impl::get_instance(utf8_type, utf8_type, true))
            .with_column("u", udt_type())
            .build();
}

static schema_ptr counter_schema() {
    return schema_builder(this_smp_shard_count(), "ks", "cnt")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("c", counter_type)
            .build();
}

static dht::decorated_key test_key(const schema& s) {
    return dht::decorate_key(s, partition_key::from_single_value(s, serialized(sstring("pk1"))));
}

static clustering_key ck(const schema& s, const char* a, const char* b = "b") {
    return clustering_key::from_exploded(s, { serialized(sstring(a)), serialized(sstring(b)) });
}

static mutation empty_mutation(schema_ptr s) {
    return mutation(s, test_key(*s));
}

static const column_definition& col(const schema& s, const char* name) {
    auto* cdef = s.get_column_definition(to_bytes(name));
    BOOST_REQUIRE(cdef);
    return *cdef;
}

static collection_mutation map_cell(tombstone tomb, std::vector<std::pair<sstring, api::timestamp_type>> members) {
    collection_mutation_writer w(tomb);
    // The writer expects members in key order; the keys below are single
    // characters, so sorting by the raw key is enough.
    std::ranges::sort(members, [] (auto& a, auto& b) { return a.first < b.first; });
    for (auto& [key, ts] : members) {
        auto cell = atomic_cell::make_live(*utf8_type, ts, serialized(sstring("val")),
                atomic_cell::collection_member::yes);
        auto key_bytes = serialized(key);
        w.push_back(bytes_view(key_bytes), atomic_cell_view(cell));
    }
    return std::move(w).finish();
}

static collection_mutation udt_cell(tombstone tomb, std::vector<std::pair<size_t, api::timestamp_type>> fields) {
    collection_mutation_writer w(tomb);
    std::ranges::sort(fields, [] (auto& a, auto& b) { return a.first < b.first; });
    for (auto& [idx, ts] : fields) {
        auto cell = atomic_cell::make_live(*utf8_type, ts, serialized(sstring("fv")),
                atomic_cell::collection_member::yes);
        auto key_bytes = serialize_field_index(idx);
        w.push_back(bytes_view(key_bytes), atomic_cell_view(cell));
    }
    return std::move(w).finish();
}

static filter_result filter(const mutation& m, api::timestamp_type t) {
    return filter_at_timestamp(m, t).get();
}

// ---------------------------------------------------------------------------
// The independent answer used by the property test.
//
// It shares only the specification with the filter ("keep an item iff its
// timestamp is at or below T"); the traversal, the decision points and the
// construction of the result are all separate from db/pitr/t_filter.cc.
// ---------------------------------------------------------------------------

class independent_filter final : public mutation_partition_visitor {
    const schema& _s;
    api::timestamp_type _t;
    mutation& _out;
    deletable_row* _row = nullptr;

    std::optional<collection_mutation> keep_collection(collection_mutation_view cmv) {
        auto tomb = cmv.tomb();
        collection_mutation_writer w(tomb.timestamp <= _t ? tomb : tombstone());
        for (auto&& kv : cmv) {
            if (kv.second.timestamp() <= _t) {
                w.push_back(kv.first, kv.second);
            }
        }
        if (w.empty()) {
            return std::nullopt;
        }
        return std::move(w).finish();
    }

    void put(row& r, column_kind kind, column_id id, atomic_cell_view cell) {
        if (cell.timestamp() <= _t) {
            r.append_cell(id, atomic_cell_or_collection(*_s.column_at(kind, id).type, cell));
        }
    }

    void put(row& r, column_kind kind, column_id id, collection_mutation_view cmv) {
        if (auto c = keep_collection(cmv)) {
            r.append_cell(id, atomic_cell_or_collection(std::move(*c)));
        }
    }

public:
    independent_filter(const schema& s, api::timestamp_type t, mutation& out) : _s(s), _t(t), _out(out) { }

    void accept_partition_tombstone(tombstone tomb) override {
        if (tomb && tomb.timestamp <= _t) {
            _out.partition().apply(tomb);
        }
    }
    void accept_static_cell(column_id id, atomic_cell_view cell) override {
        put(_out.partition().static_row().maybe_create(), column_kind::static_column, id, cell);
    }
    void accept_static_cell(column_id id, collection_mutation_view cmv) override {
        put(_out.partition().static_row().maybe_create(), column_kind::static_column, id, cmv);
    }
    void accept_row_tombstone(const range_tombstone& rt) override {
        if (rt.tomb.timestamp <= _t) {
            _out.partition().apply_row_tombstone(_s, range_tombstone(rt));
        }
    }
    void accept_row(position_in_partition_view key, const row_tombstone& deleted_at, const row_marker& rm,
            is_dummy dummy, is_continuous continuous) override {
        _row = &_out.partition().clustered_row(_s, key, dummy, continuous);
        if (!rm.is_missing() && rm.timestamp() <= _t) {
            _row->apply(rm);
        }
        // The two halves are decided on their own timestamps. Applying the
        // regular half alone also lowers the shadowable half to it, which is
        // what "drop the shadowable half" means.
        if (deleted_at.regular() && deleted_at.regular().timestamp <= _t) {
            _row->apply(deleted_at.regular());
        }
        if (deleted_at.is_shadowable() && deleted_at.shadowable().tomb().timestamp <= _t) {
            _row->apply(deleted_at.shadowable());
        }
    }
    void accept_row_cell(column_id id, atomic_cell_view cell) override {
        put(_row->cells(), column_kind::regular_column, id, cell);
    }
    void accept_row_cell(column_id id, collection_mutation_view cmv) override {
        put(_row->cells(), column_kind::regular_column, id, cmv);
    }
};

static mutation independent_answer(const mutation& m, api::timestamp_type t) {
    mutation out(m.schema(), m.decorated_key());
    out.partition().set_static_row_continuous(m.partition().static_row_continuous());
    independent_filter v(*m.schema(), t, out);
    m.partition().accept(*m.schema(), v);
    // A clustering row which kept nothing is not a write and must not appear
    // in the result; clustered_row() above created one for every visited row.
    auto& rows = out.partition().mutable_clustered_rows();
    auto deleter = current_deleter<rows_entry>();
    for (auto it = rows.begin(); it != rows.end(); ) {
        if (it->empty() && !it->dummy()) {
            it = rows.erase_and_dispose(it, deleter);
        } else {
            ++it;
        }
    }
    return out;
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE(pitr_t_filter_test)

// Every kind of timestamped item, on its own, at T - 1, T and T + 1. The item
// is written at T, so it must survive T and T + 1 and be gone at T - 1.
SEASTAR_THREAD_TEST_CASE(test_matrix_per_item_kind) {
    auto s = test_schema();

    struct test_case {
        const char* name;
        std::function<void(mutation&)> write;
        // What the filter must report as kept when the item survives.
        std::function<bool(const filter_stats&)> kept_counted;
    };

    std::vector<test_case> cases;

    cases.push_back({"live atomic cell", [&] (mutation& m) {
        m.set_clustered_cell(ck(*s, "a"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x"))));
    }, [] (const filter_stats& st) { return st.cells_kept == 1; }});

    cases.push_back({"dead atomic cell", [&] (mutation& m) {
        m.set_clustered_cell(ck(*s, "a"), col(*s, "v"), atomic_cell::make_dead(T, dt));
    }, [] (const filter_stats& st) { return st.cells_kept == 1; }});

    cases.push_back({"expiring atomic cell", [&] (mutation& m) {
        m.set_clustered_cell(ck(*s, "a"), col(*s, "v"),
                atomic_cell::make_live(*utf8_type, T, serialized(sstring("x")), dt, gc_clock::duration(60)));
    }, [] (const filter_stats& st) { return st.cells_kept == 1; }});

    cases.push_back({"static cell", [&] (mutation& m) {
        m.set_static_cell(col(*s, "st"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x"))));
    }, [] (const filter_stats& st) { return st.cells_kept == 1; }});

    cases.push_back({"static collection member", [&] (mutation& m) {
        m.set_static_cell(col(*s, "stm"), map_cell({}, {{"k", T}}));
    }, [] (const filter_stats& st) { return st.cells_kept == 1; }});

    cases.push_back({"row marker", [&] (mutation& m) {
        m.partition().clustered_row(*s, ck(*s, "a")).apply(row_marker(T));
    }, [] (const filter_stats& st) { return st.row_markers_kept == 1; }});

    cases.push_back({"dead row marker", [&] (mutation& m) {
        m.partition().clustered_row(*s, ck(*s, "a")).apply(row_marker(tombstone(T, dt)));
    }, [] (const filter_stats& st) { return st.row_markers_kept == 1; }});

    cases.push_back({"regular row tombstone", [&] (mutation& m) {
        m.partition().clustered_row(*s, ck(*s, "a")).apply(tombstone(T, dt));
    }, [] (const filter_stats& st) { return st.row_tombstones_kept == 1; }});

    cases.push_back({"shadowable row tombstone", [&] (mutation& m) {
        m.partition().clustered_row(*s, ck(*s, "a")).apply(shadowable_tombstone(tombstone(T, dt)));
    }, [] (const filter_stats& st) { return st.row_tombstones_kept == 1; }});

    cases.push_back({"range tombstone", [&] (mutation& m) {
        m.partition().apply_row_tombstone(*s,
                range_tombstone(clustering_key_prefix::from_single_value(*s, serialized(sstring("a"))),
                        bound_kind::incl_start,
                        clustering_key_prefix::from_single_value(*s, serialized(sstring("z"))),
                        bound_kind::incl_end,
                        tombstone(T, dt)));
    }, [] (const filter_stats& st) { return st.range_tombstones_kept == 1; }});

    cases.push_back({"partition tombstone", [&] (mutation& m) {
        m.partition().apply(tombstone(T, dt));
    }, [] (const filter_stats& st) { return st.partition_tombstones_kept == 1; }});

    cases.push_back({"collection member", [&] (mutation& m) {
        m.set_clustered_cell(ck(*s, "a"), col(*s, "m"), map_cell({}, {{"k", T}}));
    }, [] (const filter_stats& st) { return st.cells_kept == 1; }});

    cases.push_back({"collection tombstone", [&] (mutation& m) {
        m.set_clustered_cell(ck(*s, "a"), col(*s, "m"), map_cell(tombstone(T, dt), {}));
    }, [] (const filter_stats& st) { return st.collection_tombstones_kept == 1; }});

    cases.push_back({"udt field", [&] (mutation& m) {
        m.set_clustered_cell(ck(*s, "a"), col(*s, "u"), udt_cell({}, {{1, T}}));
    }, [] (const filter_stats& st) { return st.cells_kept == 1; }});

    cases.push_back({"udt tombstone", [&] (mutation& m) {
        m.set_clustered_cell(ck(*s, "a"), col(*s, "u"), udt_cell(tombstone(T, dt), {}));
    }, [] (const filter_stats& st) { return st.collection_tombstones_kept == 1; }});

    for (auto& c : cases) {
        testlog.info("matrix case: {}", c.name);
        auto m = empty_mutation(s);
        c.write(m);
        BOOST_REQUIRE(!m.partition().empty());

        // T - 1: the write is above T, so nothing is left.
        {
            auto res = filter(m, T - 1);
            BOOST_REQUIRE_MESSAGE(res.m.partition().empty(),
                    format("{}: expected an empty result at T-1, got {}", c.name, res.m));
            BOOST_REQUIRE_MESSAGE(res.stats.total_kept() == 0,
                    format("{}: nothing should be kept at T-1, got {}", c.name, res.stats));
            BOOST_REQUIRE_MESSAGE(res.stats.total_dropped() == 1,
                    format("{}: exactly one item should be dropped at T-1, got {}", c.name, res.stats));
        }
        // T exactly, and T + 1: the whole write survives.
        for (auto t : { T, T + 1 }) {
            auto res = filter(m, t);
            assert_that(res.m).is_equal_to(m);
            BOOST_REQUIRE_MESSAGE(res.stats.total_dropped() == 0,
                    format("{}: nothing should be dropped at {}, got {}", c.name, t, res.stats));
            BOOST_REQUIRE_MESSAGE(c.kept_counted(res.stats),
                    format("{}: unexpected counts at {}: {}", c.name, t, res.stats));
        }
    }
}

// A delete and an insert tied at T both stay, so the target resolves the tie
// exactly as the source did.
SEASTAR_THREAD_TEST_CASE(test_delete_and_insert_tied_at_t) {
    auto s = test_schema();
    auto key = ck(*s, "a");

    auto m = empty_mutation(s);
    m.set_clustered_cell(key, col(*s, "v"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x"))));
    m.partition().clustered_row(*s, key).apply(tombstone(T, dt));

    auto res = filter(m, T);
    assert_that(res.m).is_equal_to(m);
    BOOST_REQUIRE_EQUAL(res.stats.cells_kept, 1u);
    BOOST_REQUIRE_EQUAL(res.stats.row_tombstones_kept, 1u);
    BOOST_REQUIRE_EQUAL(res.stats.total_dropped(), 0u);

    // The tie resolves the same way in the input and in the result: at an
    // equal timestamp the tombstone wins, so the row is not live either way.
    const auto& in_row = *m.partition().clustered_rows().begin();
    const auto& out_row = *res.m.partition().clustered_rows().begin();
    BOOST_REQUIRE_EQUAL(bool(in_row.row().is_live(*s, column_kind::regular_column)),
            bool(out_row.row().is_live(*s, column_kind::regular_column)));
    BOOST_REQUIRE(!out_row.row().is_live(*s, column_kind::regular_column));
}

// The two halves of a row tombstone are filtered on their own timestamps; the
// tombstone is never kept or dropped as one unit.
SEASTAR_THREAD_TEST_CASE(test_row_tombstone_halves_filtered_separately) {
    auto s = test_schema();
    auto key = ck(*s, "a");

    auto m = empty_mutation(s);
    auto& r = m.partition().clustered_row(*s, key);
    r.apply(tombstone(T - 10, dt));                             // regular
    r.apply(shadowable_tombstone(tombstone(T + 10, dt)));       // shadowable, above the regular one
    BOOST_REQUIRE(bool(r.deleted_at().is_shadowable()));

    // Between the two: the regular half stays, the shadowable half collapses
    // onto it, so the row is no longer shadowable.
    {
        auto res = filter(m, T);
        const auto& out = res.m.partition().clustered_rows().begin()->row();
        BOOST_REQUIRE(out.deleted_at().regular() == tombstone(T - 10, dt));
        BOOST_REQUIRE(!bool(out.deleted_at().is_shadowable()));
        BOOST_REQUIRE(out.deleted_at().shadowable().tomb() == tombstone(T - 10, dt));
        BOOST_REQUIRE_EQUAL(res.stats.row_tombstones_kept, 1u);
        BOOST_REQUIRE_EQUAL(res.stats.row_tombstones_dropped, 1u);
    }
    // Below both: the row is gone.
    {
        auto res = filter(m, T - 11);
        BOOST_REQUIRE(res.m.partition().empty());
        BOOST_REQUIRE_EQUAL(res.stats.row_tombstones_dropped, 2u);
        BOOST_REQUIRE_EQUAL(res.stats.rows_dropped, 1u);
    }
    // At or above both: unchanged.
    {
        auto res = filter(m, T + 10);
        assert_that(res.m).is_equal_to(m);
        BOOST_REQUIRE_EQUAL(res.stats.row_tombstones_kept, 2u);
        BOOST_REQUIRE_EQUAL(res.stats.row_tombstones_dropped, 0u);
    }
}

// A batch with mixed timestamps is split by T.
SEASTAR_THREAD_TEST_CASE(test_mixed_timestamp_batch_split_by_t) {
    auto s = test_schema();

    auto m = empty_mutation(s);
    m.partition().apply(tombstone(T - 100, dt));                                       // kept
    m.set_static_cell(col(*s, "st"), atomic_cell::make_live(*utf8_type, T + 1, serialized(sstring("s"))));  // dropped
    m.set_static_cell(col(*s, "stm"), map_cell({}, {{"a", T - 1}, {"b", T + 1}}));      // one of two kept
    m.set_clustered_cell(ck(*s, "r1"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x")))); // kept (tie)
    m.set_clustered_cell(ck(*s, "r2"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T + 1, serialized(sstring("y")))); // dropped
    m.partition().clustered_row(*s, ck(*s, "r2")).apply(row_marker(T + 5));             // dropped
    m.set_clustered_cell(ck(*s, "r3"), col(*s, "m"), map_cell(tombstone(T + 2, dt), {{"k", T - 2}})); // tombstone dropped, member kept

    auto expected = empty_mutation(s);
    expected.partition().apply(tombstone(T - 100, dt));
    expected.set_static_cell(col(*s, "stm"), map_cell({}, {{"a", T - 1}}));
    expected.set_clustered_cell(ck(*s, "r1"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x"))));
    expected.set_clustered_cell(ck(*s, "r3"), col(*s, "m"), map_cell({}, {{"k", T - 2}}));

    auto res = filter(m, T);
    assert_that(res.m).is_equal_to(expected);
    BOOST_REQUIRE_EQUAL(res.stats.cells_kept, 3u);              // stm[a], r1.v, r3.m[k]
    BOOST_REQUIRE_EQUAL(res.stats.cells_dropped, 3u);           // st, stm[b], r2.v
    BOOST_REQUIRE_EQUAL(res.stats.collection_tombstones_dropped, 1u);
    BOOST_REQUIRE_EQUAL(res.stats.row_markers_dropped, 1u);
    BOOST_REQUIRE_EQUAL(res.stats.partition_tombstones_kept, 1u);
    BOOST_REQUIRE_EQUAL(res.stats.rows_dropped, 1u);            // r2
}

// The two ends: everything kept, and an empty result.
SEASTAR_THREAD_TEST_CASE(test_everything_kept_and_empty_result) {
    auto s = test_schema();

    auto m = empty_mutation(s);
    m.partition().apply(tombstone(T - 1, dt));
    m.set_static_cell(col(*s, "st"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("s"))));
    m.set_clustered_cell(ck(*s, "a"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T + 1, serialized(sstring("x"))));
    m.set_clustered_cell(ck(*s, "a"), col(*s, "m"), map_cell(tombstone(T, dt), {{"k", T + 2}}));
    m.partition().clustered_row(*s, ck(*s, "b")).apply(row_marker(T + 3));

    {
        auto res = filter(m, api::max_timestamp);
        assert_that(res.m).is_equal_to(m);
        BOOST_REQUIRE_EQUAL(res.stats.total_dropped(), 0u);
        BOOST_REQUIRE_EQUAL(res.stats.rows_dropped, 0u);
    }
    {
        auto res = filter(m, api::min_timestamp);
        BOOST_REQUIRE(res.m.partition().empty());
        BOOST_REQUIRE_EQUAL(res.stats.total_kept(), 0u);
        BOOST_REQUIRE_EQUAL(res.stats.rows_dropped, 2u);
    }
}

// An empty mutation stays empty, and nothing is counted.
SEASTAR_THREAD_TEST_CASE(test_empty_input) {
    auto s = test_schema();
    auto m = empty_mutation(s);
    auto res = filter(m, T);
    BOOST_REQUIRE(res.m.partition().empty());
    BOOST_REQUIRE_EQUAL(res.stats.total_kept(), 0u);
    BOOST_REQUIRE_EQUAL(res.stats.total_dropped(), 0u);
    BOOST_REQUIRE(res.m.decorated_key().equal(*s, m.decorated_key()));
}

// A kept cell keeps its original TTL and expiry: the filter rewinds the write
// history, not the clock.
SEASTAR_THREAD_TEST_CASE(test_ttl_and_expiry_are_not_rewound) {
    auto s = test_schema();
    const auto expiry = gc_clock::time_point(gc_clock::duration(999999));
    const auto ttl = gc_clock::duration(3600);

    auto m = empty_mutation(s);
    m.set_clustered_cell(ck(*s, "a"), col(*s, "v"),
            atomic_cell::make_live(*utf8_type, T, serialized(sstring("x")), expiry, ttl));
    m.partition().clustered_row(*s, ck(*s, "a")).apply(row_marker(T, ttl, expiry));

    auto res = filter(m, T);
    assert_that(res.m).is_equal_to(m);

    const auto& out = res.m.partition().clustered_rows().begin()->row();
    auto cell = out.cells().cell_at(col(*s, "v").id).as_atomic_cell(col(*s, "v"));
    BOOST_REQUIRE(cell.is_live_and_has_ttl());
    BOOST_REQUIRE(cell.expiry() == expiry);
    BOOST_REQUIRE(cell.ttl() == ttl);
    BOOST_REQUIRE(out.marker().is_expiring());
    BOOST_REQUIRE(out.marker().expiry() == expiry);
    BOOST_REQUIRE(out.marker().ttl() == ttl);
}

// Counters are out of scope: the restore path refuses counter tables, and if a
// counter cell reaches the filter anyway it fails loudly rather than returning
// a value that was never written.
SEASTAR_THREAD_TEST_CASE(test_counter_cell_is_refused) {
    auto s = counter_schema();
    auto m = mutation(s, test_key(*s));
    m.set_clustered_cell(clustering_key::make_empty(), col(*s, "c"), atomic_cell::make_dead(T, dt));

    BOOST_REQUIRE_THROW(filter(m, T), counter_cells_unsupported);
    BOOST_REQUIRE_THROW(filter(m, T - 1), counter_cells_unsupported);

    // A counter table with no counter cell set is filtered without complaint.
    auto empty = mutation(s, test_key(*s));
    empty.partition().apply(tombstone(T, dt));
    auto res = filter(empty, T);
    BOOST_REQUIRE_EQUAL(res.stats.partition_tombstones_kept, 1u);
}

// Seeded property test: the filter must agree with an independently computed
// answer for every T around the generated timestamps.
SEASTAR_THREAD_TEST_CASE(test_random_mutations_against_independent_answer) {
    const auto seed = tests::random::get_int<uint32_t>();
    testlog.info("test_random_mutations_against_independent_answer seed: {}", seed);

    for (auto uncompactable : { random_mutation_generator::generate_uncompactable::no,
                                random_mutation_generator::generate_uncompactable::yes }) {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no,
                local_shard_only::yes, uncompactable, seed);
        auto s = gen.schema();

        for (int i = 0; i < 4; ++i) {
            auto m = gen();
            // The generator's timestamps sit in a narrow band, so walking a
            // window around api::min_timestamp covers below, tied and above
            // for every item it produced.
            std::vector<api::timestamp_type> ts_points { api::min_timestamp - 1, api::max_timestamp };
            for (api::timestamp_type offset = 0; offset <= 75; ++offset) {
                ts_points.push_back(api::min_timestamp + offset);
            }
            // Every item is either kept or dropped, never both or neither.
            const auto all = filter(m, api::max_timestamp).stats;
            const auto none = filter(m, api::min_timestamp - 1).stats;
            BOOST_REQUIRE_EQUAL(all.total_kept(), none.total_dropped());
            BOOST_REQUIRE_EQUAL(all.total_dropped(), 0u);
            BOOST_REQUIRE_EQUAL(none.total_kept(), 0u);

            for (auto t : ts_points) {
                auto res = filter(m, t);
                auto expected = independent_answer(m, t);
                BOOST_REQUIRE_MESSAGE(res.m.partition().equal(*s, expected.partition()),
                        format("seed {}, uncompactable {}, mutation {}, T {}:\nfiltered: {}\nexpected: {}",
                                seed, bool(uncompactable), i, t, res.m, expected));
                BOOST_REQUIRE_MESSAGE(res.stats.total_kept() + res.stats.total_dropped() == all.total_kept(),
                        format("seed {}, T {}: {} does not account for all {} items",
                                seed, t, res.stats, all.total_kept()));
            }
        }
    }
}

BOOST_AUTO_TEST_SUITE_END()
