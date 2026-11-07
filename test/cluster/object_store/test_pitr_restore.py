#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""PITR replay that ends `done`: the target is exact at T, deleted rows stay deleted, and a compaction during replay never
brings a row back. T1-T3 of docs-work/test-plan-slim.md (issue #22)."""

import asyncio
import logging

from datetime import datetime, timezone

import pytest

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    assert_every_shard, assert_exact, change_statements, deadline, DONE, FAILED, first_history_read_after_load, gated_store,
    keys_on_every_shard, needs_phase, now_us, old_ts, pitr_node, pks_of, PURGEABLE_OPTIONS, read_exact, Reason, report_stalls,
    result_line, seed_statements, simple_rows, STEP_US, TABLE_SCHEMA, W, write_and_purge, WRONG_WAY_BACK
)
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

# commitlog_segment_size_in_mb defaults to 32, and a mutation may be up to half a segment. 1 MiB is left for the rest of it.
BIG_VALUE_BYTES = 15 * 1024 * 1024
PURGEABLE_SCHEMA = f'{TABLE_SCHEMA} {PURGEABLE_OPTIONS}'


@needs_phase('ep.3')
async def test_restore_is_exact_at_t_on_every_shard(manager: ScyllaClusterManager, s3_storage):
    """T1: RST-1, RST-2, limit 26, RTN-1, P52. Every kind of change on every shard, writes 1 microsecond either side of T on one
    cell and on different cells, one write near the largest mutation, then writes above T. Table u has the same keys."""
    pks = keys_on_every_shard(2)
    async with pitr_node(manager, s3_storage) as p:
        await p.cql.run_async(f'CREATE TABLE {p.ks}.u {TABLE_SCHEMA}')
        seed = seed_statements(pks, old_ts())
        await p.write(seed)
        await p.backup()

        base = now_us()
        changes, _ = change_statements(pks, base)
        t = base + 20 * STEP_US
        around = []
        for pk in pks:
            for d in (-1, 0, 1):
                around.append(W(t + d, f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET v = 'at {d}' WHERE pk = {pk} AND ck = 40"))
                around.append(W(t + d, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, {42 + d}, 'cell') USING TIMESTAMP {{ts}}"))
        big = W(t - 2, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pks[-1]}, 60, ?) USING TIMESTAMP {{ts}}", ('b' * BIG_VALUE_BYTES,))
        above = [s for pk in pks for s in (W(t + 1000, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 50, 'late') USING TIMESTAMP {{ts}}"),
                                           W(t + 2000, f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET v = 'late' WHERE pk = {pk} AND ck = 0"))]
        history = changes + around + [big] + above
        await p.write(history)
        await p.write([W(base + 500, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, {ck}, 'from u') USING TIMESTAMP {{ts}}")
                       for pk in pks for ck in (0, 1, 20, 40, 41, 42, 60)], table='u')
        await p.covered(t + 1_000_000)

        outcome, target = await p.replay_done(t)
        await assert_exact(p.cql, p.ks, target, seed + history, t)
        await assert_every_shard(p.cql, p.ks, target, pks)
        assert not [r for r in await read_exact(p.cql, p.ks, target) if 'from u' in r], 'a value from table u was applied'
        late = {r.pk: r.writetime_v for r in await p.cql.run_async(f'SELECT pk, WRITETIME(v) AS writetime_v FROM {p.ks}.t WHERE ck = 0 ALLOW FILTERING')
                if r.pk in pks}
        assert late == {pk: t + 2000 for pk in pks}, f'the source must keep every write above T: {late}'
        assert len(await p.cql.run_async(f'SELECT pk FROM {p.ks}.t WHERE ck = 50 ALLOW FILTERING')) == len(pks)
        assert await result_line(p), f'the node must log the done result: {outcome}'


@needs_phase('ep.3')
@pytest.mark.xfail(reason='tasks carry no success text, so RTN-1 is not in the result (#100, #102)', strict=False)
async def test_done_result_carries_the_wrong_way_back_warning(manager: ScyllaClusterManager, s3_storage):
    """RTN-1, verbatim, in the result. A gap in the product today (#102): kept visible rather than dropped."""
    async with pitr_node(manager, s3_storage) as p:
        await p.write(simple_rows([1], old_ts()))
        await p.backup()
        t = now_us()
        await p.covered(t + 1_000_000)
        outcome, _ = await p.replay_done(t)
        assert WRONG_WAY_BACK in outcome.result_text, f'the result must carry RTN-1 verbatim: {outcome}'


@needs_phase('ep.3')
async def test_deleted_row_stays_deleted(manager: ScyllaClusterManager, s3_storage):
    """T2: RST-3. Row A is purged, with its tombstone, before the backup. Row B is in the backup and deleted after it at D, and
    row C is deleted at D2 > T. C is the control: it shows the test can see a row."""
    a, b, c = 1, 2, 3
    async with pitr_node(manager, s3_storage, schema=PURGEABLE_SCHEMA) as p:
        await write_and_purge(manager, p.server, p.cql, p.ks, table='t', purged_pk=a, kept_pks=(b, c))
        await p.backup()
        d = now_us()
        await p.write([W(d, f'DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {b}'),
                       W(d + STEP_US, f'DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {c}')])
        await p.covered()

        _, target = await p.replay_done(d)
        seen = await pks_of(p.cql, p.ks, target)
        assert seen == {c}, f'expected only row {c}: {a} was purged before the backup and {b} deleted at T. Saw {seen}'


@needs_phase('ep.3')
@pytest.mark.parametrize('compaction', ['nodetool_compact', 'alter_compaction_enabled'])
async def test_compaction_during_replay_never_gives_done_with_a_row_back(manager: ScyllaClusterManager, gated_store, compaction):
    """T3: RST-6, RST-3, P52. B is in the backup, deleted after it, then written with a timestamp older than the delete, so the
    delete hides it. The replay is held after it read the backup and before it read the history, and the target is compacted.
    A compaction that purged the tombstone mid-run would let the older write land. Either outcome is fine but that one."""
    b = 2
    async with pitr_node(manager, gated_store, schema=PURGEABLE_SCHEMA) as p:
        await p.write(simple_rows([1, b], old_ts()))
        await p.backup()
        d = now_us()
        await p.write([W(d, f'DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {b}')])
        await p.write([W(d - 500, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({b}, 0, 'older') USING TIMESTAMP {{ts}}")])
        await p.covered()
        target = await p.new_target()

        started = datetime.fromtimestamp(now_us() / 1e6 - 1, timezone.utc).replace(tzinfo=None)
        gated_store.hold_when(first_history_read_after_load())
        replay = asyncio.create_task(p.replay(d + STEP_US, target))

        async def held():
            return True if gated_store.holding.is_set() or replay.done() else None
        await wait_for(held, deadline())
        assert gated_store.holding.is_set(), f'the replay ended before the history was read: {replay.result()}'
        logger.info(f'At the hold, the target holds {await pks_of(p.cql, p.ks, target)}')
        try:
            if compaction == 'nodetool_compact':
                await manager.api.keyspace_compaction(p.server.ip_addr, p.ks, target)
            else:
                await p.cql.run_async(f"ALTER TABLE {p.ks}.{target} WITH compaction = {{'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'}}")
        except Exception as e:
            # Refusing the compaction is one way to keep the tombstone.
            logger.info(f'The compaction was refused: {e}')
        gated_store.release()
        outcome, _ = await replay

        seen = await pks_of(p.cql, p.ks, target)
        assert not (outcome.state == DONE and b in seen), f'done with row {b} back: {outcome}'
        if outcome.state == FAILED:
            assert outcome.reason is not None, f'a refusal carries a real reason code (none exists yet for a compaction): {outcome}'
            await p.assert_incomplete(target)
        else:
            assert outcome.state == DONE, outcome
            ran = await p.cql.run_async(f"SELECT compacted_at FROM system.compaction_history WHERE keyspace_name = '{p.ks}' "
                                        f"AND columnfamily_name = '{target}' ALLOW FILTERING")
            ran = [r.compacted_at for r in ran if r.compacted_at >= started]
            assert not ran, f'done after a compaction ran on the target during replay: {ran}'
