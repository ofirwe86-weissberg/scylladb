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
#include <functional>
#include <random>
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
    return user_type_impl::get_instance("ks", to_bytes("ut"), {to_bytes("f1"), to_bytes("f2")}, {utf8_type, utf8_type}, true);
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
    return schema_builder(this_smp_shard_count(), "ks", "cnt").with_column("pk", utf8_type, column_kind::partition_key).with_column("c", counter_type).build();
}

static dht::decorated_key test_key(const schema& s) {
    return dht::decorate_key(s, partition_key::from_single_value(s, serialized(sstring("pk1"))));
}

static clustering_key ck(const schema& s, const char* a, const char* b = "b") {
    return clustering_key::from_exploded(s, {serialized(sstring(a)), serialized(sstring(b))});
}

static mutation empty_mutation(schema_ptr s) {
    return mutation(s, test_key(*s));
}

static const column_definition& col(const schema& s, const char* name) {
    auto* cdef = s.get_column_definition(to_bytes(name));
    BOOST_REQUIRE(cdef);
    return *cdef;
}

// One member of a non-frozen collection: its key, its write timestamp, and
// whether it is a value or a deletion of that key.
struct member_spec {
    sstring key;
    api::timestamp_type ts;
    bool live = true;
    bool expiring = false;
};

static collection_mutation map_cell_ex(tombstone tomb, std::vector<member_spec> members) {
    collection_mutation_writer w(tomb);
    // The writer expects members in key order; the keys used here are either
    // single characters or fixed-width, so sorting by the raw key is enough.
    std::ranges::sort(members, [](const member_spec& a, const member_spec& b) {
        return a.key < b.key;
    });
    for (auto& ms : members) {
        atomic_cell cell = !ms.live      ? atomic_cell::make_dead(ms.ts, dt)
                           : ms.expiring ? atomic_cell::make_live(*utf8_type, ms.ts, serialized(sstring("val")), dt + gc_clock::duration(45),
                                                   gc_clock::duration(45), atomic_cell::collection_member::yes)
                                         : atomic_cell::make_live(*utf8_type, ms.ts, serialized(sstring("val")), atomic_cell::collection_member::yes);
        auto key_bytes = serialized(ms.key);
        w.push_back(bytes_view(key_bytes), atomic_cell_view(cell));
    }
    return std::move(w).finish();
}

static collection_mutation map_cell(tombstone tomb, std::vector<std::pair<sstring, api::timestamp_type>> members) {
    std::vector<member_spec> specs;
    specs.reserve(members.size());
    for (auto& [key, ts] : members) {
        specs.push_back({key, ts, true});
    }
    return map_cell_ex(tomb, std::move(specs));
}

static collection_mutation udt_cell(tombstone tomb, std::vector<std::pair<size_t, api::timestamp_type>> fields) {
    collection_mutation_writer w(tomb);
    std::ranges::sort(fields, [](auto& a, auto& b) {
        return a.first < b.first;
    });
    for (auto& [idx, ts] : fields) {
        auto cell = atomic_cell::make_live(*utf8_type, ts, serialized(sstring("fv")), atomic_cell::collection_member::yes);
        auto key_bytes = serialize_field_index(idx);
        w.push_back(bytes_view(key_bytes), atomic_cell_view(cell));
    }
    return std::move(w).finish();
}

static filter_result filter(const mutation& m, api::timestamp_type t) {
    return filter_at_timestamp(m, t).get();
}

// ---------------------------------------------------------------------------
// Answer #1, used by the per-item property test: a restatement of the
// per-item rule over a separate traversal (mutation_partition::accept()),
// with its own decision points and its own construction of the result.
//
// It restates the specification rather than deriving it, so it cannot catch a
// wrong *grouping* of items into statements; the statement-level property test
// further down exists for that. What it does catch is traversal and
// representation bugs over the whole space random_mutation_generator covers.
// ---------------------------------------------------------------------------

class independent_filter final : public mutation_partition_visitor {
    const schema& _s;
    api::timestamp_type _t;
    mutation& _out;
    deletable_row* _row = nullptr;

    std::optional<collection_mutation> keep_collection(collection_mutation_view cmv) {
        auto tomb = cmv.tomb();
        // A collection tombstone with a member one microsecond above it was
        // written by that member's statement (see the header).
        auto tomb_statement_ts = tomb.timestamp;
        if (tomb && tomb.timestamp != api::max_timestamp) {
            for (auto&& kv : cmv) {
                if (kv.second.timestamp() == tomb.timestamp + 1) {
                    tomb_statement_ts = tomb.timestamp + 1;
                    break;
                }
            }
        }
        collection_mutation_writer w(tomb_statement_ts <= _t ? tomb : tombstone());
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
    independent_filter(const schema& s, api::timestamp_type t, mutation& out)
        : _s(s)
        , _t(t)
        , _out(out) {
    }

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
    void accept_row(position_in_partition_view key, const row_tombstone& deleted_at, const row_marker& rm, is_dummy dummy, is_continuous continuous) override {
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
    for (auto it = rows.begin(); it != rows.end();) {
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

    cases.push_back({"live atomic cell",
            [&](mutation& m) {
                m.set_clustered_cell(ck(*s, "a"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x"))));
            },
            [](const filter_stats& st) {
                return st.cells_kept == 1;
            }});

    cases.push_back({"dead atomic cell",
            [&](mutation& m) {
                m.set_clustered_cell(ck(*s, "a"), col(*s, "v"), atomic_cell::make_dead(T, dt));
            },
            [](const filter_stats& st) {
                return st.cells_kept == 1;
            }});

    cases.push_back({"expiring atomic cell",
            [&](mutation& m) {
                m.set_clustered_cell(ck(*s, "a"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x")), dt, gc_clock::duration(60)));
            },
            [](const filter_stats& st) {
                return st.cells_kept == 1;
            }});

    cases.push_back({"static cell",
            [&](mutation& m) {
                m.set_static_cell(col(*s, "st"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x"))));
            },
            [](const filter_stats& st) {
                return st.cells_kept == 1;
            }});

    cases.push_back({"static collection member",
            [&](mutation& m) {
                m.set_static_cell(col(*s, "stm"), map_cell({}, {{"k", T}}));
            },
            [](const filter_stats& st) {
                return st.cells_kept == 1;
            }});

    cases.push_back({"row marker",
            [&](mutation& m) {
                m.partition().clustered_row(*s, ck(*s, "a")).apply(row_marker(T));
            },
            [](const filter_stats& st) {
                return st.row_markers_kept == 1;
            }});

    cases.push_back({"dead row marker",
            [&](mutation& m) {
                m.partition().clustered_row(*s, ck(*s, "a")).apply(row_marker(tombstone(T, dt)));
            },
            [](const filter_stats& st) {
                return st.row_markers_kept == 1;
            }});

    cases.push_back({"regular row tombstone",
            [&](mutation& m) {
                m.partition().clustered_row(*s, ck(*s, "a")).apply(tombstone(T, dt));
            },
            [](const filter_stats& st) {
                return st.row_tombstones_kept == 1;
            }});

    cases.push_back({"shadowable row tombstone",
            [&](mutation& m) {
                m.partition().clustered_row(*s, ck(*s, "a")).apply(shadowable_tombstone(tombstone(T, dt)));
            },
            [](const filter_stats& st) {
                return st.row_tombstones_kept == 1;
            }});

    cases.push_back({"range tombstone",
            [&](mutation& m) {
                m.partition().apply_row_tombstone(
                        *s, range_tombstone(clustering_key_prefix::from_single_value(*s, serialized(sstring("a"))), bound_kind::incl_start,
                                    clustering_key_prefix::from_single_value(*s, serialized(sstring("z"))), bound_kind::incl_end, tombstone(T, dt)));
            },
            [](const filter_stats& st) {
                return st.range_tombstones_kept == 1;
            }});

    cases.push_back({"partition tombstone",
            [&](mutation& m) {
                m.partition().apply(tombstone(T, dt));
            },
            [](const filter_stats& st) {
                return st.partition_tombstones_kept == 1;
            }});

    cases.push_back({"collection member",
            [&](mutation& m) {
                m.set_clustered_cell(ck(*s, "a"), col(*s, "m"), map_cell({}, {{"k", T}}));
            },
            [](const filter_stats& st) {
                return st.cells_kept == 1;
            }});

    cases.push_back({"collection tombstone",
            [&](mutation& m) {
                m.set_clustered_cell(ck(*s, "a"), col(*s, "m"), map_cell(tombstone(T, dt), {}));
            },
            [](const filter_stats& st) {
                return st.collection_tombstones_kept == 1;
            }});

    cases.push_back({"udt field",
            [&](mutation& m) {
                m.set_clustered_cell(ck(*s, "a"), col(*s, "u"), udt_cell({}, {{1, T}}));
            },
            [](const filter_stats& st) {
                return st.cells_kept == 1;
            }});

    cases.push_back({"udt tombstone",
            [&](mutation& m) {
                m.set_clustered_cell(ck(*s, "a"), col(*s, "u"), udt_cell(tombstone(T, dt), {}));
            },
            [](const filter_stats& st) {
                return st.collection_tombstones_kept == 1;
            }});

    for (auto& c : cases) {
        testlog.info("matrix case: {}", c.name);
        auto m = empty_mutation(s);
        c.write(m);
        BOOST_REQUIRE(!m.partition().empty());

        // T - 1: the write is above T, so nothing is left.
        {
            auto res = filter(m, T - 1);
            BOOST_REQUIRE_MESSAGE(res.m.partition().empty(), format("{}: expected an empty result at T-1, got {}", c.name, res.m));
            BOOST_REQUIRE_MESSAGE(res.stats.total_kept() == 0, format("{}: nothing should be kept at T-1, got {}", c.name, res.stats));
            BOOST_REQUIRE_MESSAGE(res.stats.total_dropped() == 1, format("{}: exactly one item should be dropped at T-1, got {}", c.name, res.stats));
        }
        // T exactly, and T + 1: the whole write survives.
        for (auto t : {T, T + 1}) {
            auto res = filter(m, t);
            assert_that(res.m).is_equal_to(m);
            BOOST_REQUIRE_MESSAGE(res.stats.total_dropped() == 0, format("{}: nothing should be dropped at {}, got {}", c.name, t, res.stats));
            BOOST_REQUIRE_MESSAGE(c.kept_counted(res.stats), format("{}: unexpected counts at {}: {}", c.name, t, res.stats));
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
    BOOST_REQUIRE_EQUAL(bool(in_row.row().is_live(*s, column_kind::regular_column)), bool(out_row.row().is_live(*s, column_kind::regular_column)));
    BOOST_REQUIRE(!out_row.row().is_live(*s, column_kind::regular_column));
}

// The two halves of a row tombstone are filtered on their own timestamps; the
// tombstone is never kept or dropped as one unit.
SEASTAR_THREAD_TEST_CASE(test_row_tombstone_halves_filtered_separately) {
    auto s = test_schema();
    auto key = ck(*s, "a");

    auto m = empty_mutation(s);
    auto& r = m.partition().clustered_row(*s, key);
    r.apply(tombstone(T - 10, dt));                       // regular
    r.apply(shadowable_tombstone(tombstone(T + 10, dt))); // shadowable, above the regular one
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
    m.partition().apply(tombstone(T - 100, dt));                                                                           // kept
    m.set_static_cell(col(*s, "st"), atomic_cell::make_live(*utf8_type, T + 1, serialized(sstring("s"))));                 // dropped
    m.set_static_cell(col(*s, "stm"), map_cell({}, {{"a", T - 1}, {"b", T + 1}}));                                         // one of two kept
    m.set_clustered_cell(ck(*s, "r1"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x"))));     // kept (tie)
    m.set_clustered_cell(ck(*s, "r2"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T + 1, serialized(sstring("y")))); // dropped
    m.partition().clustered_row(*s, ck(*s, "r2")).apply(row_marker(T + 5));                                                // dropped
    m.set_clustered_cell(ck(*s, "r3"), col(*s, "m"), map_cell(tombstone(T + 2, dt), {{"k", T - 2}}));                      // tombstone dropped, member kept

    auto expected = empty_mutation(s);
    expected.partition().apply(tombstone(T - 100, dt));
    expected.set_static_cell(col(*s, "stm"), map_cell({}, {{"a", T - 1}}));
    expected.set_clustered_cell(ck(*s, "r1"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x"))));
    expected.set_clustered_cell(ck(*s, "r3"), col(*s, "m"), map_cell({}, {{"k", T - 2}}));

    auto res = filter(m, T);
    assert_that(res.m).is_equal_to(expected);
    BOOST_REQUIRE_EQUAL(res.stats.cells_kept, 3u);    // stm[a], r1.v, r3.m[k]
    BOOST_REQUIRE_EQUAL(res.stats.cells_dropped, 3u); // st, stm[b], r2.v
    BOOST_REQUIRE_EQUAL(res.stats.collection_tombstones_dropped, 1u);
    BOOST_REQUIRE_EQUAL(res.stats.row_markers_dropped, 1u);
    BOOST_REQUIRE_EQUAL(res.stats.partition_tombstones_kept, 1u);
    BOOST_REQUIRE_EQUAL(res.stats.rows_dropped, 1u); // r2
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
    m.set_clustered_cell(ck(*s, "a"), col(*s, "v"), atomic_cell::make_live(*utf8_type, T, serialized(sstring("x")), expiry, ttl));
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

// Overwriting a non-frozen collection or UDT ("SET c = ...") writes its
// tombstone one microsecond before the statement's timestamp and the new
// members at it, so the tombstone belongs to the statement above it. Filtering
// it on its own timestamp would keep the delete and drop the data it came with,
// emptying a collection that no statement ever emptied.
SEASTAR_THREAD_TEST_CASE(test_collection_overwrite_is_one_statement) {
    auto s = test_schema();
    const auto key = ck(*s, "a");

    // SET m = {'k': 'v'} at T, as cql3/maps.cc writes it.
    auto overwrite = empty_mutation(s);
    overwrite.set_clustered_cell(key, col(*s, "m"), map_cell(tombstone(T - 1, dt), {{"k", T}}));
    // SET u = (...) at T, as cql3/user_types.cc writes it.
    overwrite.set_clustered_cell(key, col(*s, "u"), udt_cell(tombstone(T - 1, dt), {{0, T}}));

    // Below the statement: the whole statement, tombstone included, is gone.
    for (auto t : {T - 1, T - 2}) {
        auto res = filter(overwrite, t);
        BOOST_REQUIRE_MESSAGE(res.m.partition().empty(), format("T {}: the overwrite's tombstone outlived its members: {}", t, res.m));
        BOOST_REQUIRE_EQUAL(res.stats.collection_tombstones_kept, 0u);
        BOOST_REQUIRE_EQUAL(res.stats.collection_tombstones_dropped, 2u);
    }
    // At the statement: unchanged.
    {
        auto res = filter(overwrite, T);
        assert_that(res.m).is_equal_to(overwrite);
        BOOST_REQUIRE_EQUAL(res.stats.collection_tombstones_kept, 2u);
        BOOST_REQUIRE_EQUAL(res.stats.cells_kept, 2u);
    }

    // A plain "DELETE m" writes the tombstone at its own timestamp with no
    // members (cql3/constants.cc), and is kept at T.
    {
        auto del = empty_mutation(s);
        del.set_clustered_cell(key, col(*s, "m"), map_cell(tombstone(T, dt), {}));
        auto res = filter(del, T);
        assert_that(res.m).is_equal_to(del);
        BOOST_REQUIRE_EQUAL(res.stats.collection_tombstones_kept, 1u);
    }

    // The documented ambiguity: an overwrite with an empty collection is
    // written exactly like a delete one microsecond earlier, so it is kept as
    // a delete.
    {
        auto empty_overwrite = empty_mutation(s);
        empty_overwrite.set_clustered_cell(key, col(*s, "m"), map_cell(tombstone(T - 1, dt), {}));
        auto res = filter(empty_overwrite, T - 1);
        assert_that(res.m).is_equal_to(empty_overwrite);
        BOOST_REQUIRE_EQUAL(res.stats.collection_tombstones_kept, 1u);
    }

    // A member further above the tombstone does not group with it: that is two
    // statements, not one.
    {
        auto two = empty_mutation(s);
        two.set_clustered_cell(key, col(*s, "m"), map_cell(tombstone(T, dt), {{"k", T + 2}}));
        auto res = filter(two, T);
        BOOST_REQUIRE_EQUAL(res.stats.collection_tombstones_kept, 1u);
        BOOST_REQUIRE_EQUAL(res.stats.cells_dropped, 1u);
        auto expected = empty_mutation(s);
        expected.set_clustered_cell(key, col(*s, "m"), map_cell(tombstone(T, dt), {}));
        assert_that(res.m).is_equal_to(expected);
    }
}

// STATED LIMIT, not a bug being asserted as correct behaviour.
//
// A BATCH merges its statements into one mutation per key, each statement
// keeping its own timestamp (eventual_consistency::batch_executor.cc). A batch
// of "DELETE m" at X and "m['k'] = v" at X + 1 therefore writes exactly what a
// single overwrite at X + 1 writes, and the filter cannot tell them apart. It
// applies the overwrite rule, so at T = X the delete goes out with the member
// and the collection's older members, which the delete at X should have
// removed, survive in the restored table.
//
// This test pins what actually happens so that the limit is visible and any
// change to it is deliberate. The decision on whether to carry statement
// boundaries through the ledger is Ofir's.
SEASTAR_THREAD_TEST_CASE(test_batched_delete_then_member_is_a_stated_limit) {
    auto s = test_schema();
    const auto key = ck(*s, "a");

    // An older member, then the batch: DELETE m at T, m['k'] = v at T + 1.
    auto m = empty_mutation(s);
    m.set_clustered_cell(key, col(*s, "m"), map_cell_ex(tombstone(T, dt), {{"old", T - 5}, {"k", T + 1}}));

    auto res = filter(m, T);

    // What a correct answer would be, if the statement boundary were known:
    // the delete at T survives and wipes the older member.
    auto correct = empty_mutation(s);
    correct.set_clustered_cell(key, col(*s, "m"), map_cell_ex(tombstone(T, dt), {{"old", T - 5}}));
    BOOST_REQUIRE_MESSAGE(
            !res.m.partition().equal(*s, correct.partition()), "the batch ambiguity is gone; update this test and the header if that was deliberate");

    // What actually happens: the tombstone is taken for the overwrite's and
    // dropped with the member above it, leaving the older member uncovered.
    auto actual = empty_mutation(s);
    actual.set_clustered_cell(key, col(*s, "m"), map_cell_ex({}, {{"old", T - 5}}));
    assert_that(res.m).is_equal_to(actual);
    BOOST_REQUIRE_EQUAL(res.stats.collection_tombstones_dropped, 1u);
    BOOST_REQUIRE_EQUAL(res.stats.cells_kept, 1u);
    BOOST_REQUIRE_EQUAL(res.stats.cells_dropped, 1u);

    // At T + 1 the whole batch is below T and everything is kept, so the limit
    // only shows between the two timestamps.
    assert_that(filter(m, T + 1).m).is_equal_to(m);
}

// A collection larger than the filter's yield batch, so the preemption point
// inside the member walk is actually taken.
SEASTAR_THREAD_TEST_CASE(test_large_collection_is_filtered_in_batches) {
    auto s = test_schema();
    const auto key = ck(*s, "a");
    constexpr size_t members = 3000; // > collection_members_per_yield (1024)

    std::vector<std::pair<sstring, api::timestamp_type>> all;
    std::vector<std::pair<sstring, api::timestamp_type>> below;
    for (size_t i = 0; i < members; ++i) {
        // Fixed-width keys so that the byte order is the numeric order.
        auto k = format("{:06d}", i);
        auto ts = (i % 2) ? T + 1 : T;
        all.emplace_back(k, ts);
        if (ts <= T) {
            below.emplace_back(k, ts);
        }
    }

    auto m = empty_mutation(s);
    m.set_clustered_cell(key, col(*s, "m"), map_cell({}, all));

    auto expected = empty_mutation(s);
    expected.set_clustered_cell(key, col(*s, "m"), map_cell({}, below));

    auto res = filter(m, T);
    assert_that(res.m).is_equal_to(expected);
    BOOST_REQUIRE_EQUAL(res.stats.cells_kept, members / 2);
    BOOST_REQUIRE_EQUAL(res.stats.cells_dropped, members / 2);

    // The same, overwritten as CQL does it: the tombstone one microsecond
    // below the members, with the grouping decided across many yield batches.
    auto overwritten = empty_mutation(s);
    std::vector<std::pair<sstring, api::timestamp_type>> at_t;
    for (size_t i = 0; i < members; ++i) {
        at_t.emplace_back(format("{:06d}", i), T);
    }
    overwritten.set_clustered_cell(key, col(*s, "m"), map_cell(tombstone(T - 1, dt), at_t));

    auto res2 = filter(overwritten, T - 1);
    BOOST_REQUIRE(res2.m.partition().empty());
    BOOST_REQUIRE_EQUAL(res2.stats.collection_tombstones_dropped, 1u);
    BOOST_REQUIRE_EQUAL(res2.stats.cells_dropped, members);

    auto res3 = filter(overwritten, T);
    assert_that(res3.m).is_equal_to(overwritten);
    BOOST_REQUIRE_EQUAL(res3.stats.cells_kept, members);
}

// Seeded property test: the filter must agree with an independently computed
// answer for every T around the generated timestamps.
SEASTAR_THREAD_TEST_CASE(test_random_mutations_against_independent_answer) {
    // Fixed so that a failure reproduces; change it by hand to widen the net.
    constexpr uint32_t seed = 0x5eed16;
    testlog.info("test_random_mutations_against_independent_answer seed: {}", seed);

    for (auto uncompactable : {random_mutation_generator::generate_uncompactable::no, random_mutation_generator::generate_uncompactable::yes}) {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no, local_shard_only::yes, uncompactable, seed);
        auto s = gen.schema();

        for (int i = 0; i < 4; ++i) {
            auto m = gen();
            // The generator's timestamps sit in a narrow band, so walking a
            // window around api::min_timestamp covers below, tied and above
            // for every item it produced.
            std::vector<api::timestamp_type> ts_points{api::min_timestamp - 1, api::max_timestamp};
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
                        format("seed {}, uncompactable {}, mutation {}, T {}:\nfiltered: {}\nexpected: {}", seed, bool(uncompactable), i, t, res.m, expected));
                BOOST_REQUIRE_MESSAGE(res.stats.total_kept() + res.stats.total_dropped() == all.total_kept(),
                        format("seed {}, T {}: {} does not account for all {} items", seed, t, res.stats, all.total_kept()));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Answer #2, the statement-level property test.
//
// This answer never looks at an individual item: it applies whole statements
// whose timestamp is at or below T and compares that with filtering their
// merge. It therefore checks what the filter is for ("the target holds the
// statements up to T") rather than restating the per-item rule, and it is what
// catches a tombstone being grouped with the wrong statement.
//
// Each statement writes to a target no other statement touches (its own cell,
// row marker, row tombstone, static column or clustering range, and at most
// one partition tombstone), so merging them loses nothing and the two sides
// stay structurally comparable.
// ---------------------------------------------------------------------------

namespace {

struct statement {
    api::timestamp_type ts;
    mutation m;
};

static schema_ptr property_schema() {
    auto b = schema_builder(this_smp_shard_count(), "ks", "prop")
                     .with_column("pk", utf8_type, column_kind::partition_key)
                     .with_column("ck1", utf8_type, column_kind::clustering_key)
                     .with_column("ck2", utf8_type, column_kind::clustering_key);
    for (int i = 0; i < 4; ++i) {
        b.with_column(to_bytes(format("v{}", i)), utf8_type);
        b.with_column(to_bytes(format("m{}", i)), map_type_impl::get_instance(utf8_type, utf8_type, true));
    }
    for (int i = 0; i < 2; ++i) {
        b.with_column(to_bytes(format("u{}", i)), udt_type());
        b.with_column(to_bytes(format("st{}", i)), utf8_type, column_kind::static_column);
        b.with_column(to_bytes(format("sm{}", i)), map_type_impl::get_instance(utf8_type, utf8_type, true), column_kind::static_column);
    }
    return b.build();
}

constexpr api::timestamp_type stmt_base_ts = 2000;

static std::vector<statement> make_statements(schema_ptr s, std::mt19937& gen) {
    std::uniform_int_distribution<api::timestamp_type> ts_dist(0, 6);
    std::uniform_int_distribution<int> coin(0, 1);
    std::uniform_int_distribution<int> kind3(0, 2);
    std::uniform_int_distribution<int> kind5(0, 4);
    const char* rows[] = {"r0", "r1", "r2", "r3"};

    std::vector<statement> out;
    auto add = [&](api::timestamp_type ts, std::function<void(mutation&, api::timestamp_type)> write) {
        mutation m(s, test_key(*s));
        write(m, ts);
        out.push_back(statement{ts, std::move(m)});
    };
    auto next_ts = [&] {
        return stmt_base_ts + ts_dist(gen);
    };

    for (auto r : rows) {
        // INSERT: one row marker per row, sometimes expiring.
        if (coin(gen)) {
            const bool expiring = coin(gen);
            add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                m.partition()
                        .clustered_row(*s, ck(*s, r))
                        .apply(expiring ? row_marker(ts, gc_clock::duration(60), dt + gc_clock::duration(60)) : row_marker(ts));
            });
        }
        // DELETE a row: one regular row tombstone per row.
        if (coin(gen)) {
            add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                m.partition().clustered_row(*s, ck(*s, r)).apply(tombstone(ts, dt));
            });
        }
        // One statement per regular atomic cell.
        for (int i = 0; i < 4; ++i) {
            if (!coin(gen)) {
                continue;
            }
            const auto name = format("v{}", i);
            switch (kind3(gen)) {
            case 0:
                add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                    m.set_clustered_cell(ck(*s, r), col(*s, name.c_str()), atomic_cell::make_live(*utf8_type, ts, serialized(sstring("x"))));
                });
                break;
            case 1: // the same, with a TTL
                add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                    m.set_clustered_cell(ck(*s, r), col(*s, name.c_str()),
                            atomic_cell::make_live(*utf8_type, ts, serialized(sstring("x")), dt + gc_clock::duration(30), gc_clock::duration(30)));
                });
                break;
            default: // DELETE the column
                add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                    m.set_clustered_cell(ck(*s, r), col(*s, name.c_str()), atomic_cell::make_dead(ts, dt));
                });
                break;
            }
        }
        // One statement per non-frozen map cell.
        for (int i = 0; i < 4; ++i) {
            if (!coin(gen)) {
                continue;
            }
            const auto name = format("m{}", i);
            switch (kind5(gen)) {
            case 0: // SET m = {...}: tombstone one microsecond below the members
                add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                    m.set_clustered_cell(ck(*s, r), col(*s, name.c_str()), map_cell_ex(tombstone(ts - 1, dt), {{"a", ts}, {"b", ts}}));
                });
                break;
            case 1: // m['a'] = ...
                add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                    m.set_clustered_cell(ck(*s, r), col(*s, name.c_str()), map_cell_ex({}, {{"a", ts}}));
                });
                break;
            case 2: // DELETE m['a']
                add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                    m.set_clustered_cell(ck(*s, r), col(*s, name.c_str()), map_cell_ex({}, {{"a", ts, false}}));
                });
                break;
            case 3: // m['a'] = ... USING TTL: an expiring member
                add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                    m.set_clustered_cell(ck(*s, r), col(*s, name.c_str()), map_cell_ex({}, {{"a", ts, true, true}}));
                });
                break;
            default: // DELETE m
                add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                    m.set_clustered_cell(ck(*s, r), col(*s, name.c_str()), map_cell_ex(tombstone(ts, dt), {}));
                });
                break;
            }
        }
        // One statement per non-frozen UDT cell.
        for (int i = 0; i < 2; ++i) {
            if (!coin(gen)) {
                continue;
            }
            const auto name = format("u{}", i);
            if (coin(gen)) { // SET u = (...)
                add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                    m.set_clustered_cell(ck(*s, r), col(*s, name.c_str()), udt_cell(tombstone(ts - 1, dt), {{0, ts}, {1, ts}}));
                });
            } else { // SET u.f1 = ...
                add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                    m.set_clustered_cell(ck(*s, r), col(*s, name.c_str()), udt_cell({}, {{0, ts}}));
                });
            }
        }
    }

    // One statement per static column.
    for (int i = 0; i < 2; ++i) {
        if (coin(gen)) {
            const auto name = format("st{}", i);
            const bool dead = coin(gen);
            add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                m.set_static_cell(
                        col(*s, name.c_str()), dead ? atomic_cell::make_dead(ts, dt) : atomic_cell::make_live(*utf8_type, ts, serialized(sstring("s"))));
            });
        }
        if (coin(gen)) {
            const auto name = format("sm{}", i);
            const bool overwrite = coin(gen);
            add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                m.set_static_cell(col(*s, name.c_str()), overwrite ? map_cell_ex(tombstone(ts - 1, dt), {{"a", ts}}) : map_cell_ex({}, {{"a", ts}}));
            });
        }
    }

    // DELETE over a clustering range. The ranges are disjoint and separated, so
    // the range tombstone list keeps them as distinct entries.
    const std::pair<const char*, const char*> ranges[] = {{"x0", "x1"}, {"x3", "x4"}, {"x6", "x7"}};
    for (auto& [from, to] : ranges) {
        if (coin(gen)) {
            add(next_ts(), [=, &s](mutation& m, api::timestamp_type ts) {
                m.partition().apply_row_tombstone(
                        *s, range_tombstone(clustering_key_prefix::from_single_value(*s, serialized(sstring(from))), bound_kind::incl_start,
                                    clustering_key_prefix::from_single_value(*s, serialized(sstring(to))), bound_kind::incl_end, tombstone(ts, dt)));
            });
        }
    }

    // DELETE the partition, at most once.
    if (coin(gen)) {
        add(next_ts(), [](mutation& m, api::timestamp_type ts) {
            m.partition().apply(tombstone(ts, dt));
        });
    }

    std::shuffle(out.begin(), out.end(), gen);
    return out;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_statements_merged_then_filtered) {
    // Fixed so that a failure reproduces; change it by hand to widen the net.
    constexpr uint32_t seed = 0x57a7e3;
    testlog.info("test_statements_merged_then_filtered seed: {}", seed);
    std::mt19937 gen(seed);
    auto s = property_schema();

    for (int round = 0; round < 20; ++round) {
        auto statements = make_statements(s, gen);

        mutation merged(s, test_key(*s));
        for (auto& st : statements) {
            merged.apply(st.m);
        }

        for (api::timestamp_type t = stmt_base_ts - 5; t <= stmt_base_ts + 10; ++t) {
            mutation expected(s, test_key(*s));
            for (auto& st : statements) {
                if (st.ts <= t) {
                    expected.apply(st.m);
                }
            }
            auto res = filter(merged, t);
            BOOST_REQUIRE_MESSAGE(res.m.partition().equal(*s, expected.partition()),
                    format("seed {}, round {}, {} statements, T {}:\nfiltered: {}\nexpected: {}", seed, round, statements.size(), t, res.m, expected));
        }
    }
}

BOOST_AUTO_TEST_SUITE_END()
