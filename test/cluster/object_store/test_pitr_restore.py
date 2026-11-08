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
    keys_on_every_shard, needs_phase, now_us, objects_of, old_ts, pitr_node, pks_of, PURGEABLE_OPTIONS, read_exact, Reason, report_stalls,
    result_line, seed_statements, SEGMENT_KEY_RE, SHIPPING_PREFIX, simple_rows, STEP_US, TABLE_SCHEMA, W, write_and_purge, WRONG_WAY_BACK
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
@pytest.mark.parametrize('compaction', [
    'nodetool_compact',
    pytest.param('alter_compaction_enabled', marks=pytest.mark.skip_bug(
        link="https://github.com/ofirwe86-weissberg/scylladb/issues/111", reason="the refusal carries no reason code for a compaction (#111)")),
])
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


@needs_phase('ep.3')
async def test_restore_reports_done_and_leaves_the_source_untouched(manager: ScyllaClusterManager, s3_storage):
    """R06-P1: RST-1. Rows Y are written on every shard after the backup. Then one Y partition is deleted at a timestamp above T
    and the delete, with its tombstone, is purged from the source. A replay that wrote into the source would bring that partition
    back, as last-write-wins hides nothing. The restore ends `done`, the target holds all of Y, and the source is as it was."""
    pks = keys_on_every_shard(4)
    deleted = pks[0]
    async with pitr_node(manager, s3_storage, schema=PURGEABLE_SCHEMA) as p:
        seed = seed_statements(pks, old_ts())
        await p.write(seed)
        await p.backup()
        y = simple_rows(pks, now_us(), 'Y')
        await p.write(y)
        t = y[-1].ts
        await p.covered()

        await p.write([W(t + STEP_US, f'DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {deleted}')])
        await manager.api.keyspace_flush(p.server.ip_addr, p.ks, p.cf)

        async def purged():
            await manager.api.keyspace_compaction(p.server.ip_addr, p.ks, p.cf)
            fragments = await p.cql.run_async(f'SELECT pk FROM MUTATION_FRAGMENTS({p.ks}.{p.cf})')
            return True if deleted not in {f.pk for f in fragments} else None
        await wait_for(purged, deadline())
        assert deleted not in await pks_of(p.cql, p.ks, p.cf)

        before = await read_exact(p.cql, p.ks, p.cf)
        _, target = await p.replay_done(t)
        assert await read_exact(p.cql, p.ks, p.cf) == before, 'the replay changed the source table'
        assert deleted not in await pks_of(p.cql, p.ks, p.cf), f'partition {deleted} came back in the source'
        await assert_exact(p.cql, p.ks, target, seed + y, t)


@needs_phase('ep.3')
async def test_a_t_between_two_backups_restores_exactly_from_the_earlier_one(manager: ScyllaClusterManager, s3_storage):
    """R21-S1: L3's safe path. T is after backup 1 and before backup 2, so the restore names backup 1. It holds the seed and A
    and not B. The backup-2 control shows only that B is reachable from backup 2, so the absence of B comes from naming backup 1."""
    pks = keys_on_every_shard(2)
    async with pitr_node(manager, s3_storage) as p:
        seed = seed_statements(pks, old_ts())
        await p.write(seed)
        first = await p.backup()
        first_prefix = p.prefix

        a = simple_rows(pks, now_us(), 'A')
        await p.write(a)
        t_a = now_us()
        b_ts = t_a + STEP_US
        b = [W(b_ts, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 5, 'B') USING TIMESTAMP {{ts}}") for pk in pks]
        await p.write(b)
        await p.backup()
        assert p.snap != first
        await p.covered()

        # Backup 2 holds B: restoring it to the time of the last B write must show B.
        _, control = await p.replay_done(b[-1].ts)
        assert {r.pk for r in await p.cql.run_async(f"SELECT pk FROM {p.ks}.{control} WHERE ck = 5 ALLOW FILTERING")} == set(pks), \
            'backup 2 should hold B'

        p.snap, p.prefix = first, first_prefix
        _, target = await p.replay_done(t_a)
        await assert_exact(p.cql, p.ks, target, seed + a, t_a)
        assert not await p.cql.run_async(f"SELECT pk FROM {p.ks}.{target} WHERE ck = 5 ALLOW FILTERING"), 'a B row is in the restore'


@needs_phase('ep.3')
async def test_after_a_truncate_a_restart_and_a_new_backup_restore_exactly(manager: ScyllaClusterManager, s3_storage):
    """R18-S1: L7's safe path (README 16). R is written and shipped, then the table is truncated. After a restart, a new backup
    and a replay from it give S and U and no R. Controls: R's segments reach the bucket before the truncate, and the restart
    starts a new chain directory while the old chain's segments stay."""
    r_pks, s_pks, u_pks = keys_on_every_shard(2, 100), keys_on_every_shard(2, 200), keys_on_every_shard(2, 300)

    async def segment_keys():
        return {k for k in await objects_of(s3_storage, SHIPPING_PREFIX) if SEGMENT_KEY_RE.search(k)}

    def chains(keys):
        return {SEGMENT_KEY_RE.search(k).group(2) for k in keys}

    async with pitr_node(manager, s3_storage, auto_snapshot=False) as p:
        shipped_before_r = await segment_keys()
        r = simple_rows(r_pks, now_us(), 'R')
        await p.write(r)
        await p.backup()
        await p.covered()
        old_segments = await segment_keys()
        assert old_segments - shipped_before_r, 'R must be in the shipped history of the first chain'

        await p.cql.run_async(f'TRUNCATE TABLE {p.ks}.{p.cf}')
        await p.restart()
        s = simple_rows(s_pks, now_us(), 'S')
        await p.write(s)
        await p.backup()
        u = simple_rows(u_pks, now_us(), 'U')
        await p.write(u)
        await p.covered()
        now_segments = await segment_keys()
        assert chains(now_segments) - chains(old_segments), 'the restart must start a new chain directory'
        assert old_segments <= now_segments, 'the old chain\'s segments must stay in the bucket'

        _, target = await p.replay_done(u[-1].ts)
        seen = await pks_of(p.cql, p.ks, target)
        assert not seen & set(r_pks), f'truncated rows came back: {seen & set(r_pks)}'
        await assert_exact(p.cql, p.ks, target, s + u, u[-1].ts)
