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
import random
import re

from datetime import datetime, timezone

import pytest

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    assert_every_shard, assert_exact, change_statements, collection_edge_statements, create_target, deadline, DEADLINE_SECONDS, DONE, expected_at, FAILED,
    first_history_read_after_load, gated_store, iso, keys_on_every_shard, needs_phase, now_us, objects_of, old_ts, pitr_node, pks_of,
    PURGEABLE_OPTIONS, read_exact, Reason, report_stalls, result_line, RESULT_MARK, run_nodetool, seed_statements, SEGMENT_KEY_RE, SHIPPING_PREFIX,
    simple_rows, STEP_US, TABLE_SCHEMA, W, write_and_purge, WRONG_WAY_BACK
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


async def compaction_asked_during_replay(manager, gated_store, compaction):
    """B is in the backup, deleted after it, then written with a timestamp older than the delete, so the delete hides it. The
    replay is held after it read the backup and before it read the history, and the target is compacted or made compactable.
    A compaction that purged the tombstone mid-run would let the older write land. Returns `(outcome, refused)`: how the replay
    ended, and the exception if the compaction request itself was refused, else None."""
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
        refused = None
        try:
            if compaction == 'nodetool_compact':
                await manager.api.keyspace_compaction(p.server.ip_addr, p.ks, target)
            else:
                await p.cql.run_async(f"ALTER TABLE {p.ks}.{target} WITH compaction = {{'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'}}")
        except Exception as e:
            refused = e
            logger.info(f'The compaction was refused: {e}')
        gated_store.release()
        outcome, _ = await replay
        seen = await pks_of(p.cql, p.ks, target)
        assert not (outcome.state == DONE and b in seen), f'done with row {b} back: {outcome}'
        if outcome.state == FAILED:
            await p.assert_incomplete(target)
        else:
            ran = await p.cql.run_async(f"SELECT compacted_at FROM system.compaction_history WHERE keyspace_name = '{p.ks}' "
                                        f"AND columnfamily_name = '{target}' ALLOW FILTERING")
            ran = [r.compacted_at for r in ran if r.compacted_at >= started]
            assert not ran, f'done after a compaction ran on the target during replay: {ran}'
        return outcome, refused


@needs_phase('ep.3')
async def test_a_major_compaction_asked_during_the_replay_never_gives_done_with_a_row_back(manager: ScyllaClusterManager, gated_store):
    """R08-P2: RST-6, RST-3, P52. A `nodetool compact` of the target while the replay is held. The request may be refused or the
    run may fail: either way the deleted row stays out and `done` is never reported after a compaction ran."""
    outcome, _ = await compaction_asked_during_replay(manager, gated_store, 'nodetool_compact')
    if outcome.state == FAILED:
        assert outcome.reason is not None, f'a refusal carries a real reason code: {outcome}'


@needs_phase('ep.3')
async def test_enabling_compaction_during_the_replay_fails_the_restore(manager: ScyllaClusterManager, gated_store):
    """R08-P3: RST-3. Auto-compaction is enabled on the target while the replay is held. The restore must fail and never report
    done, the deleted row stays out, and the message names the compaction. The refusal has no reason code of its own yet (#111),
    so the reason isn't asserted (decision D2, provisional)."""
    outcome, refused = await compaction_asked_during_replay(manager, gated_store, 'alter_compaction_enabled')
    assert refused is None, f'the ALTER must go through, or the test shows nothing: {refused}'
    assert outcome.state == FAILED, f'enabling compaction during the run must fail it: {outcome}'
    assert 'compaction' in outcome.message.lower(), f'the failure must name the compaction: {outcome}'


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
        # The row cache may still hold the tombstone, which would hide a replay into the source as well.
        await manager.api.drop_sstable_caches(p.server.ip_addr)
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


async def segments_shipped_after_the_writes(p, before):
    """The shipped segment objects not in `before`, once every shard has one: a segment sealed after the caller's writes. A shard
    seals only when it is written to, so the wait writes on every shard."""
    async def shipped():
        await p.pulse()
        found = {k for k in await objects_of(p.storage, SHIPPING_PREFIX) if SEGMENT_KEY_RE.search(k)} - before
        return found if {int(SEGMENT_KEY_RE.search(k).group(1)) for k in found} >= {0, 1} else None
    return await wait_for(shipped, deadline(), label='a shipped segment on every shard')


async def shipped_segments(storage):
    return {k for k in await objects_of(storage, SHIPPING_PREFIX) if SEGMENT_KEY_RE.search(k)}


@needs_phase('ep.3')
async def test_writes_on_every_shard_come_back_from_the_shipped_history(manager: ScyllaClusterManager, s3_storage):
    """R01-P1: SHP-1. 8 keys on each shard are written after the backup and reach the target only through the shipped history.
    Control: a second target one microsecond below the rows' timestamp (after the backup) holds the seed and none of the rows."""
    # Not from 100: seed_statements also writes pk + 100 for each seed key.
    pks = keys_on_every_shard(8, 1000)
    async with pitr_node(manager, s3_storage) as p:
        seed_ts = old_ts()
        seed = seed_statements(keys_on_every_shard(2), seed_ts)
        await p.write(seed)
        await p.backup()
        rows = simple_rows(pks, now_us())
        await p.write(rows)
        await p.covered()

        _, target = await p.replay_done(rows[-1].ts)
        await assert_exact(p.cql, p.ks, target, seed + rows, rows[-1].ts)
        await assert_every_shard(p.cql, p.ks, target, pks)
        _, seed_only = await p.replay_done(rows[-1].ts - 1)
        await assert_exact(p.cql, p.ks, seed_only, seed + rows, rows[-1].ts - 1)
        assert not await pks_of(p.cql, p.ks, seed_only) & set(pks), 'the control target holds rows written after the backup'


@needs_phase('ep.3')
async def test_keys_owned_by_every_shard_read_back_from_the_target(manager: ScyllaClusterManager, s3_storage):
    """R06-P2: RST-1. 16 keys per shard are in the backup and 16 more per shard are written after it, so the replay has to apply
    each to the shard that owns it. A key applied to the wrong shard reads as missing."""
    seed_pks, after_pks = keys_on_every_shard(16), keys_on_every_shard(16, 1000)
    async with pitr_node(manager, s3_storage) as p:
        seed = seed_statements(seed_pks, old_ts())
        await p.write(seed)
        await p.backup()
        after = simple_rows(after_pks, now_us())
        await p.write(after)
        await p.covered()

        _, target = await p.replay_done(after[-1].ts)
        await assert_every_shard(p.cql, p.ks, target, after_pks)
        assert await pks_of(p.cql, p.ks, target) >= set(seed_pks) | set(after_pks) | {pk + 100 for pk in seed_pks}
        await assert_exact(p.cql, p.ks, target, seed + after, after[-1].ts)


@needs_phase('ep.3')
async def test_the_readme_quick_start_run_literally_gives_an_exact_copy(manager: ScyllaClusterManager, s3_storage):
    """R06-P3: RST-1, PM-82. README Quick start, step by step: nodetool snapshot and backup, a write X, the target from the
    backup's schema.cql, curl to replay_to, `nodetool tasks wait`, then compaction turned on. The README has no way to wait for
    coverage, so the test uses the helper's probe for that step. Two steps stand in for the literal ones: `create_target` does the
    README's edit of schema.cql in Python, and `curl` is the host's, run as a subprocess. Control: the same T through the helper
    gives the same rows."""
    pks = keys_on_every_shard(2)
    async with pitr_node(manager, s3_storage) as p:
        seed = seed_statements(pks, old_ts())
        await p.write(seed)
        prefix = f'backups/{p.ks}/t/snap1'
        await run_nodetool(manager, p.server, ['nodetool', 'snapshot', '--tag', 'snap1', '--table', 't', p.ks])
        await run_nodetool(manager, p.server, ['nodetool', 'backup', '--endpoint', s3_storage.address, '--bucket', s3_storage.bucket_name,
                                               '--prefix', prefix, '--keyspace', p.ks, '--table', 't', '--snapshot', 'snap1'])
        p.snap, p.prefix = 'snap1', prefix
        x = simple_rows(keys_on_every_shard(2, 1000), now_us(), 'X')
        await p.write(x)
        t = x[-1].ts

        await create_target(p.cql, s3_storage, p.ks, 't_restored', prefix)
        await p.covered()
        url = (f'http://{p.server.ip_addr}:10000/storage_service/pitr/replay_to?endpoint={s3_storage.address}&bucket={s3_storage.bucket_name}'
               f'&prefix={prefix}&keyspace={p.ks}&table=t_restored&snapshot=snap1&t={iso(t)}')
        curl = await asyncio.create_subprocess_exec('curl', '-sS', '-X', 'POST', url, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await curl.communicate()
        assert curl.returncode == 0, f'curl failed: {out} {err}'
        task_id = out.decode().strip().strip('"')
        assert re.fullmatch(r'[0-9a-f-]{36}', task_id), f'replay_to must return a task id: {out}'
        await asyncio.wait_for(run_nodetool(manager, p.server, ['nodetool', 'tasks', 'wait', task_id]), DEADLINE_SECONDS)
        task = await manager.api.get_task_status(p.server.ip_addr, task_id)
        assert task['state'] == DONE, f'the replay must end done: {task}'

        await assert_exact(p.cql, p.ks, 't_restored', seed + x, t)
        await p.cql.run_async(f"ALTER TABLE {p.ks}.t_restored WITH compaction = {{'class': 'SizeTieredCompactionStrategy'}}")
        options = (await p.cql.run_async(f"SELECT compaction FROM system_schema.tables WHERE keyspace_name = '{p.ks}' AND table_name = 't_restored'"))[0].compaction
        assert options.get('enabled', 'true') != 'false', f'compaction must be on after the last step: {options}'
        _, control = await p.replay_done(t)
        assert await read_exact(p.cql, p.ks, control) == await read_exact(p.cql, p.ks, 't_restored')


@needs_phase('ep.3')
async def test_writes_before_the_backup_replayed_again_leave_the_target_exact(manager: ScyllaClusterManager, s3_storage):
    """R06-S1: L14's boundary. Every kind of change is written on every shard before the backup, and they are in the shipped history
    too, so the replay applies them a second time on top of the backup. Among them is a write issued after a newer one but with an
    older timestamp, so a replay that let the last applied write win would show it. The target must equal the answer at T, and the
    result line must count more applied cells than the post-backup writes hold, which shows the pre-backup writes were replayed
    (if the replay stops starting from the beginning of the chain, this fails and the test needs another form). Control: the
    changes' segments are in the bucket before the backup."""
    pks = keys_on_every_shard(2)
    async with pitr_node(manager, s3_storage) as p:
        before_writes = await shipped_segments(s3_storage)
        seed = seed_statements(pks, old_ts())
        changes, ts = change_statements(pks, now_us())
        newer, stale = ts['last'] + STEP_US, ts['insert']
        out_of_order = [W(newer, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 60, 'newer') USING TIMESTAMP {{ts}}") for pk in pks] + \
                       [W(stale, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 60, 'stale') USING TIMESTAMP {{ts}}") for pk in pks]
        changes = changes + out_of_order
        await p.write(seed + changes)
        k0 = await segments_shipped_after_the_writes(p, before_writes)
        assert k0, 'the pre-backup writes must be in the shipped history'
        await p.backup()
        z = simple_rows(keys_on_every_shard(2, 1000), ts['last'] + 10 * STEP_US, 'Z')
        await p.write(z)
        await p.covered(z[-1].ts + 1_000_000)

        _, target = await p.replay_done(z[-1].ts)
        await assert_exact(p.cql, p.ks, target, seed + changes + z, z[-1].ts)
        assert {r.v for r in await p.cql.run_async(f'SELECT v FROM {p.ks}.{target} WHERE ck = 60 ALLOW FILTERING')} == {'newer'}
        line = await result_for(p, target)
        applied = count_in_result(line, 'cell(s)')
        assert applied is not None and applied > len(z), f'the pre-backup writes must be replayed too ({len(z)} post-backup writes): {line}'


@needs_phase('ep.3')
async def test_every_cell_kind_is_kept_at_t_and_dropped_one_microsecond_before(manager: ScyllaClusterManager, s3_storage):
    """R07-P1: RST-2. Every kind of change (regular, static, map, set, deletes of cell, row, range and partition, a batch, a tie, a
    TTL cell) at its own timestamp. For each one, replays to its timestamp T and to T-1 are exact. Control: the answers at T-1 and
    T differ for every stage but the tie (an insert and its delete at one timestamp show nothing), so each stage is seen. T-1 of
    the map overwrite is left out: README limit 4's collection edge."""
    pks = keys_on_every_shard(2)
    async with pitr_node(manager, s3_storage) as p:
        seed = seed_statements(pks, old_ts())
        await p.write(seed)
        await p.backup()
        changes, ts = change_statements(pks, now_us())
        ttl = [W(ts['update'], f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 50, 'ttl') USING TTL 3600 AND TIMESTAMP {{ts}}") for pk in pks]
        history = changes + ttl
        await p.write(history)
        await p.covered()

        for stage, t in ts.items():
            for t_replay in (t - 1, t):
                if stage == 'map_overwrite' and t_replay == t - 1:
                    continue
                _, target = await p.replay_done(t_replay)
                await assert_exact(p.cql, p.ks, target, seed + history, t_replay)
            if stage != 'tie':
                assert await expected_at(p.cql, p.ks, seed + history, t - 1) != await expected_at(p.cql, p.ks, seed + history, t), \
                    f'stage {stage}: the answers at T-1 and T must differ'


@needs_phase('ep.3')
@pytest.mark.parametrize('kind', ['row', 'range', 'partition'])
async def test_a_delete_at_exactly_t_lands(manager: ScyllaClusterManager, s3_storage, kind):
    """R07-P2: RST-2. Rows on every shard are written after the backup and then deleted at ts1. A replay to ts1 has the delete
    applied and the rows gone. Control: a replay to ts1-1 still has them."""
    pks = keys_on_every_shard(2, 1000)
    where = {'row': 'ck = 1', 'range': 'ck >= 1 AND ck < 3', 'partition': None}[kind]
    async with pitr_node(manager, s3_storage) as p:
        seed = seed_statements(keys_on_every_shard(2), old_ts())
        await p.write(seed)
        await p.backup()
        ts0 = now_us()
        rows = [W(ts0, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, {ck}, 'x') USING TIMESTAMP {{ts}}") for pk in pks for ck in range(4)]
        ts1 = ts0 + STEP_US
        delete = [W(ts1, f"DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {pk}" + (f' AND {where}' if where else '')) for pk in pks]
        await p.write(rows + delete)
        await p.covered()

        _, target = await p.replay_done(ts1)
        _, before = await p.replay_done(ts1 - 1)
        deleted = {'row': {1}, 'range': {1, 2}, 'partition': {0, 1, 2, 3}}[kind]
        for table, gone in ((target, deleted), (before, set())):
            seen = {(r.pk, r.ck) for r in await p.cql.run_async(f'SELECT pk, ck FROM {p.ks}.{table}') if r.pk in pks}
            assert seen == {(pk, ck) for pk in pks for ck in range(4) if ck not in gone}, f'{table}: {sorted(seen)}'
        await assert_exact(p.cql, p.ks, target, seed + rows + delete, ts1)
        await assert_exact(p.cql, p.ks, before, seed + rows + delete, ts1 - 1)


@needs_phase('ep.3')
async def test_a_shipped_write_after_t_is_not_restored(manager: ScyllaClusterManager, s3_storage):
    """R07-P3: RST-2. A is written at ta and B at tb above it, both after the backup and both shipped. A replay to ta has A and
    not B. Control: a replay to tb holds B, so B was in the shipped history."""
    a_pks, b_pks = keys_on_every_shard(2, 1000), keys_on_every_shard(2, 2000)
    async with pitr_node(manager, s3_storage) as p:
        seed = seed_statements(keys_on_every_shard(2), old_ts())
        await p.write(seed)
        await p.backup()
        ta = now_us()
        a, b = simple_rows(a_pks, ta, 'A'), simple_rows(b_pks, ta + STEP_US, 'B')
        await p.write(a + b)
        await p.covered()

        _, target = await p.replay_done(ta)
        seen = await pks_of(p.cql, p.ks, target)
        assert set(a_pks) <= seen and not seen & set(b_pks), f'A must be present and B absent: {sorted(seen)}'
        await assert_exact(p.cql, p.ks, target, seed + a + b, ta)
        _, control = await p.replay_done(ta + STEP_US)
        assert set(b_pks) <= await pks_of(p.cql, p.ks, control), 'a replay to tb must hold B'


@needs_phase('ep.3')
async def test_seeded_random_writes_and_deletes_match_the_computed_answer(manager: ScyllaClusterManager, s3_storage):
    """R07-P4: RST-2. 200 random statements (inserts, updates, map and set elements, row, range and partition deletes, TTL cells, a
    static column) over 16 keys at random timestamps, written out of timestamp order. Up to five replays (equal times collapse), to a
    statement's timestamp or one microsecond either side of it, each equal the answer computed on a reference table. The seed is fixed and logged. The
    statements avoid README limit 4's edge forms: no collection overwrite or empty, and no batch."""
    seed_value = 20261108
    rng = random.Random(seed_value)
    logger.info(f'R07-P4 seed {seed_value}')
    pks = keys_on_every_shard(8, 1000)
    base = now_us()

    def statement():
        pk, ck, ts = rng.choice(pks), rng.randrange(6), base + rng.randrange(1, 50_000)
        kind = rng.choices(['insert', 'ttl', 'update_v', 'update_w', 'map', 'set_add', 'set_remove', 'static', 'row_delete', 'range_delete',
                            'partition_delete'], [6, 2, 4, 3, 3, 3, 2, 2, 2, 2, 1])[0]
        key = f'pk = {pk} AND ck = {ck}'
        text = {
            'insert': f"INSERT INTO {{t}} (pk, ck, v, w) VALUES ({pk}, {ck}, 'i{rng.randrange(100)}', {rng.randrange(100)}) USING TIMESTAMP {{ts}}",
            'ttl': f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, {ck}, 'ttl') USING TTL 3600 AND TIMESTAMP {{ts}}",
            'update_v': f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET v = 'u{rng.randrange(100)}' WHERE {key}",
            'update_w': f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET w = {rng.randrange(100)} WHERE {key}",
            'map': f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET m[{rng.choice((1, 2))}] = 'e{rng.randrange(100)}' WHERE {key}",
            'set_add': f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET st = st + {{{{{rng.choice((1, 2))}}}}} WHERE {key}",
            'set_remove': f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET st = st - {{{{{rng.choice((1, 2))}}}}} WHERE {key}",
            'static': f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET s = {rng.randrange(100)} WHERE pk = {pk}",
            'row_delete': f"DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE {key}",
            'range_delete': f"DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {pk} AND ck >= {ck} AND ck < {ck + 2}",
            'partition_delete': f"DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {pk}",
        }[kind]
        return W(ts, text)

    statements = [statement() for _ in range(200)]
    async with pitr_node(manager, s3_storage) as p:
        seed = seed_statements(keys_on_every_shard(2), old_ts())
        await p.write(seed)
        await p.backup()
        await p.write(statements)
        await p.covered()

        times = sorted({s.ts + rng.choice((-1, 0, 1)) for s in rng.sample(statements, 5)})
        logger.info(f'R07-P4 replays to {times}')
        answers = []
        for t in times:
            _, target = await p.replay_done(t)
            answers.append(tuple(await assert_exact(p.cql, p.ks, target, seed + statements, t)))
        assert len(set(answers)) > 1, 'the five answers are all equal, so the setup does not tell the times apart'


def count_in_result(line, phrase):
    """The number in front of `phrase` in the result line, or None if the line doesn't have it."""
    m = re.search(rf'(\d+) {re.escape(phrase)}', line or '')
    return int(m[1]) if m else None


def not_verified_count(line):
    # The result line says `N cell(s) could not be verified`.
    return count_in_result(line, 'cell(s) could not be verified')


async def result_for(p, target):
    """The `done` line of the replay into this target, from the node's log."""
    log = await p.manager.server_open_log(server_id=p.server.server_id)
    found = [line for line, _ in await log.grep(re.escape(RESULT_MARK)) if f'{p.ks}.{target} ' in line]
    return found[-1] if found else None


@needs_phase('ep.3')
async def test_at_a_collection_edge_t_the_result_counts_the_collection_as_not_verified(manager: ScyllaClusterManager, s3_storage):
    """R07-S1: README limit 4. A delete of a collection is batched with an element added 1 microsecond later. At exactly the
    delete's T the result line counts collections as not verified. One microsecond either side the restore is exact and
    counts none, which shows the setup is right."""
    pks = keys_on_every_shard(1)
    async with pitr_node(manager, s3_storage) as p:
        seed, changes, ts = collection_edge_statements(pks, old_ts(), now_us())
        await p.write(seed)
        await p.backup()
        await p.write(changes)
        await p.covered()

        edge = ts['delete']
        for t in (edge - 1, edge + 1):
            _, target = await p.replay_done(t)
            await assert_exact(p.cql, p.ks, target, seed + changes, t)
            line = await result_for(p, target)
            assert not_verified_count(line) == 0, f'T={t}: a collection counted as not verified one microsecond off the edge: {line}'
        _, target = await p.replay_done(edge)
        line = await result_for(p, target)
        assert not_verified_count(line) is not None, f'the result line must carry the not-verified count: {line}'
        assert not_verified_count(line) >= 1, f'the result at the edge T must count a collection as not verified: {line}'


@needs_phase('ep.3')
async def test_a_delete_within_gc_grace_stays_deleted_after_the_source_compacts(manager: ScyllaClusterManager, s3_storage):
    """R08-S1: L13's boundary. Partitions are deleted at ts2 and only then written at an older ts1, so only a delete that is kept,
    with timestamps respected, hides them. The source is flushed and compacted: the rows are gone and the tombstones kept (default
    gc_grace, and tombstone_gc `timeout`, as `repair` purges at once on a keyspace with one replica). The backup holds the
    tombstones and the shipped history holds both writes. The target must not have the rows back. Controls: the source's data
    shows tombstones and no rows for the deleted keys, and a never-deleted row written with them is in the target. While the
    replay starts from the beginning of the chain (L14), the history's own delete also hides the rows, so this test cannot tell
    a lost backup tombstone from a kept one."""
    gone, kept = keys_on_every_shard(1, 1000), keys_on_every_shard(1, 2000)
    async with pitr_node(manager, s3_storage, schema=f"{TABLE_SCHEMA} WITH tombstone_gc = {{'mode': 'timeout'}}") as p:
        ts1 = old_ts()
        before_writes = await shipped_segments(s3_storage)
        deletes = [W(ts1 + STEP_US, f'DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {pk}') for pk in gone]
        inserts = simple_rows(gone + kept, ts1)
        await p.write(deletes + inserts)
        await manager.api.keyspace_flush(p.server.ip_addr, p.ks, p.cf)
        async def compacted():
            # The row cache keeps listing a shadowed row after the sstables lost it, so it is dropped before the read.
            await manager.api.keyspace_compaction(p.server.ip_addr, p.ks, p.cf)
            await manager.api.drop_sstable_caches(p.server.ip_addr)
            fragments = await p.cql.run_async(f'SELECT pk, mutation_fragment_kind FROM MUTATION_FRAGMENTS({p.ks}.{p.cf})')
            rows = {f.pk for f in fragments if f.mutation_fragment_kind == 'clustering row'}
            tombstoned = {f.pk for f in fragments if f.mutation_fragment_kind == 'partition start'}
            return True if not rows & set(gone) and tombstoned >= set(gone) else None
        await wait_for(compacted, deadline(), label='the deleted rows compacted away with their tombstones kept')
        await segments_shipped_after_the_writes(p, before_writes)
        await p.backup()
        x = simple_rows(keys_on_every_shard(1, 3000), now_us(), 'X')
        await p.write(x)
        await p.covered()

        _, target = await p.replay_done(x[-1].ts)
        seen = await pks_of(p.cql, p.ks, target)
        assert not seen & set(gone), f'deleted rows came back: {sorted(seen & set(gone))}'
        assert set(kept) <= seen, 'the never-deleted rows must be in the target'
        await assert_exact(p.cql, p.ks, target, deletes + inserts + x, x[-1].ts)
