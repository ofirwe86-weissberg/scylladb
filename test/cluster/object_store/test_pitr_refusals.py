#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""PITR replay that must refuse, and never give `done`: T past coverage, damaged history, a TRUNCATE, a schema change and a
backup whose upload didn't finish. Each test ends with its positive control, which flips the outcome to `done` in the same
run, so that the refusal is shown to come from the condition and not from a broken setup. T4-T8 of docs-work/test-plan-slim.md
(issue #22)."""

import asyncio
import logging
import re
import time

import pytest

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    assert_exact, backup_prefix, deadline, delete_object, gated_store, get_object, HISTORY_DAMAGED, is_shipping_upload, iso,
    keys_on_every_shard, keys_on_shard, latest_restorable, needs_phase, now_us, objects_of, old_ts, PAST_COVERAGE_WORDS, pitr_node, pks_of, put_object,
    read_exact, Reason, report_stalls, SEAL_SECONDS, SHIPPING_PREFIX, simple_rows, STEP_US, target_statement, UNCOMPRESSED_SCHEMA, W,
    write_several_sstables
)
from test.cluster.object_store.test_backup import do_backup, take_snapshot_on_one_server
from test.pylib.rest_client import HTTPError
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

PAYLOAD = 'p' * (1024 * 1024)   # so that the segment holding the history is the largest object shipped after the backup


def assert_states_coverage(outcome, t):
    latest = latest_restorable(outcome, t)
    assert latest < t, f'the latest restorable time must be below T: {outcome}'
    for word in PAST_COVERAGE_WORDS:
        assert word in outcome.message.lower(), f'the refusal must state the {word}: {outcome}'
    return latest


@pytest.mark.skip_bug(link="https://github.com/ofirwe86-weissberg/scylladb/issues/112",
                      reason="the S3 metrics are registered twice with two endpoints on one node (#112)")
async def test_t_past_coverage_is_refused_while_writes_wait_to_ship(manager: ScyllaClusterManager, gated_store, s3_storage):
    """T4 (a): REF-5, P68. Shipping goes through the gate and the backup around it. The gate holds every upload, below
    pitr_unshipped_limit_in_mb, so writes are acknowledged and not shipped. Control: let the uploads go, and the same target
    gives `done` with the writes."""
    pks = keys_on_every_shard(1, 100)
    async with pitr_node(manager, s3_storage, ship_to=gated_store) as p:
        await p.backup()
        await p.covered()
        gated_store.hold_when(is_shipping_upload)
        t = now_us()
        writes = simple_rows(pks, t)
        await p.write(writes)

        outcome, target = await p.refused(t, Reason.T_PAST_COVERAGE)
        assert_states_coverage(outcome, t)

        gated_store.release()
        await p.covered(t + 1)
        await p.replay_done(t, target)
        await assert_exact(p.cql, p.ks, target, writes, t)


@needs_phase('ep.3', '15-bc')
async def test_t_past_coverage_is_refused_with_a_shard_idle(manager: ScyllaClusterManager, s3_storage):
    """T4 (b): REF-5, limit 26, LVL-1, P68 (PM-64). After the backup only shard 0 is written, for 5 seal intervals. The refusal
    names shard 1 and its covered-through time. Control: one write on shard 1, and the same target gives `done`."""
    on_0 = keys_on_shard(0, 5, 100)
    async with pitr_node(manager, s3_storage) as p:
        await p.backup()
        await p.covered()
        writes = []
        end = time.time() + 5 * SEAL_SECONDS
        while time.time() < end:
            w = W(now_us(), f"INSERT INTO {{t}} (pk, ck, v) VALUES ({on_0[len(writes) % len(on_0)]}, {len(writes)}, 'x') USING TIMESTAMP {{ts}}")
            await p.write([w])
            writes.append(w)
            await asyncio.sleep(0.1)    # paces the writer: this is what's tested, not a wait for something to happen
        t = writes[-1].ts

        outcome, target = await p.refused(t, Reason.T_PAST_COVERAGE)
        assert_states_coverage(outcome, t)
        assert re.search(r'\bshard\W{0,3}1\b', outcome.message, re.I), f'the refusal must name the idle shard 1: {outcome}'

        await p.write(simple_rows(keys_on_shard(1, 1, 100), now_us()))
        await p.covered(t + 1, pulse=False)
        await p.replay_done(t, target)
        await assert_exact(p.cql, p.ks, target, writes, t)


DAMAGES = {
    'removed': None,
    'cut_short': lambda data: data[:len(data) // 2],
    'one_byte_changed': lambda data: data[:len(data) // 2] + bytes([data[len(data) // 2] ^ 0xff]) + data[len(data) // 2 + 1:],
}


@needs_phase('ep.3')
@pytest.mark.parametrize('damage', DAMAGES)
async def test_damaged_history_is_refused(manager: ScyllaClusterManager, s3_storage, damage):
    """T5: REF-4. The largest object shipped after the backup is a segment of the history: ledger entries are at most 4096
    bytes. A cut segment would read as a normal end, so the writes after the cut would go missing. Control: put the bytes back,
    and a new target gives `done`, exact."""
    pks = keys_on_every_shard(1, 100)
    async with pitr_node(manager, s3_storage) as p:
        await p.backup()
        writes = [W(now_us() + i, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 0, '{PAYLOAD}') USING TIMESTAMP {{ts}}") for i, pk in enumerate(pks)]
        await p.write(writes)
        t = max(w.ts for w in writes)
        await p.covered()

        shipped = {k: o for k, o in (await objects_of(s3_storage, SHIPPING_PREFIX)).items() if k not in p.backup_keys}
        key = min(shipped, key=lambda k: (-shipped[k].size, shipped[k].last_modified))
        assert shipped[key].size > 4096, f'the largest object shipped after the backup must be a segment: {key}, {shipped[key].size} bytes'
        original = await get_object(s3_storage, key)
        logger.info(f'Damage ({damage}) to {key}, {len(original)} bytes')
        if DAMAGES[damage] is None:
            await delete_object(s3_storage, key)
        else:
            await put_object(s3_storage, key, DAMAGES[damage](original))

        outcome, _ = await p.refused(t, HISTORY_DAMAGED, loaded_ok=True)
        # The key may be given whole or under the shipping prefix: either holds this part.
        assert key[len(SHIPPING_PREFIX) + 1:] in outcome.message, f'the refusal must name the damaged object {key}: {outcome}'

        await put_object(s3_storage, key, original)
        _, target = await p.replay_done(t)
        await assert_exact(p.cql, p.ks, target, writes, t)


async def restarted_chain_restores_without_truncated_rows(p, truncated, kept=()):
    """The control of T6. P74: after a TRUNCATE, PITR needs a chain that started after it, which takes a restart, and a backup
    on that chain. Then replay gives `done`, without the truncated rows."""
    await p.restart()
    await p.backup()
    later = simple_rows(keys_on_every_shard(1, 300), now_us(), 'later')
    await p.write(later)
    await p.covered()
    _, target = await p.replay_done(later[-1].ts)
    seen = await pks_of(p.cql, p.ks, target)
    assert not seen & set(truncated), f'truncated rows came back: {seen & set(truncated)}'
    await assert_exact(p.cql, p.ks, target, [*kept, *later], later[-1].ts)


@pytest.mark.skip_bug(link="https://github.com/ofirwe86-weissberg/scylladb/issues/15",
                      reason="L7: won't be implemented this release; implementable when replay reads the source's truncation record")
async def test_truncate_after_the_backup_is_refused(manager: ScyllaClusterManager, s3_storage):
    """T6 (a): REF-10, limit 21, RST-4, P55/P74. Rows R written after the backup, then a TRUNCATE. T between R and the
    TRUNCATE is refused: replaying the history would bring R back. auto_snapshot is off, so nothing else keeps R."""
    pks = keys_on_every_shard(1, 100)
    async with pitr_node(manager, s3_storage, auto_snapshot=False) as p:
        await p.write(simple_rows(keys_on_every_shard(1, 500), old_ts(), 'seed'))
        await p.backup()
        r_ts = now_us()
        await p.write(simple_rows(pks, r_ts, 'R'))
        await p.covered()
        await p.cql.run_async(f'TRUNCATE TABLE {p.ks}.t')
        await p.covered()

        await p.refused(r_ts + STEP_US, Reason.TRUNCATED)
        await restarted_chain_restores_without_truncated_rows(p, pks)


@needs_phase('ep.3', '15-bc')
async def test_truncate_before_the_backup_is_refused(manager: ScyllaClusterManager, s3_storage):
    """T6 (b): REF-10, RST-4, P55/P74. With shipping on: rows R, a TRUNCATE, the backup, then rows S. P74 refuses every T once
    the source was truncated after its chain started. A commitlog-wide bound below the cleared writes would bring R back."""
    r_pks, s_pks = keys_on_every_shard(1, 100), keys_on_every_shard(1, 200)
    async with pitr_node(manager, s3_storage, auto_snapshot=False) as p:
        await p.write(simple_rows(r_pks, old_ts(), 'R'))
        await p.cql.run_async(f'TRUNCATE TABLE {p.ks}.t')
        await p.backup()
        s = simple_rows(s_pks, now_us(), 'S')
        await p.write(s)
        await p.covered()

        await p.refused(s[-1].ts, Reason.TRUNCATED)
        await restarted_chain_restores_without_truncated_rows(p, r_pks, kept=s)


@needs_phase('ep.3', '15-bc', 'st.1')
async def test_schema_change_since_the_backup_is_refused(manager: ScyllaClusterManager, s3_storage):
    """T7: REF-9, limit 22, PM-60. Values in column w, the backup, w dropped, more writes. Any T after the backup is refused, or
    the dropped values would come back in the copy. Control: T at the snapshot time gives `done`, exact."""
    pks = keys_on_every_shard(1, 100)
    async with pitr_node(manager, s3_storage) as p:
        seed = [W(old_ts(), f"INSERT INTO {{t}} (pk, ck, v, w) VALUES ({pk}, 0, 'seed', {pk}) USING TIMESTAMP {{ts}}") for pk in pks]
        await p.write(seed)
        await p.backup()
        await p.cql.run_async(f'ALTER TABLE {p.ks}.t DROP w')
        later = simple_rows(keys_on_every_shard(1, 200), now_us(), 'later')
        await p.write(later)
        await p.covered()

        await p.refused(later[-1].ts, Reason.SCHEMA_CHANGED)

        snapshot_t = await p.snapshot_time()
        _, target = await p.replay_done(snapshot_t)
        await assert_exact(p.cql, p.ks, target, seed, snapshot_t)


@needs_phase('ep.3', '12-2.2')
async def test_backup_with_unfinished_upload_is_refused(manager: ScyllaClusterManager, gated_store, s3_storage):
    """T8: REF-7 (don't-cut list). The backup goes through the gate, cut after about half the snapshot's bytes, and shipping
    goes around it. A partial backup loads fewer sstables. Control: back up the same snapshot again, uncut, and a new target
    gives `done`, exact."""
    async with pitr_node(manager, s3_storage, schema=UNCOMPRESSED_SCHEMA, also=(gated_store,)) as p:
        await write_several_sstables(manager, p.server, p.cql, p.ks)
        size = await manager.api.get_table_disk_space_used(p.server.ip_addr, p.ks, 't')
        p.snap, _ = await take_snapshot_on_one_server(p.ks, p.server, manager, logger)
        p.prefix = backup_prefix(p.ks, 't', p.snap)
        gated_store.fail_after_bytes(size // 2)
        tid = await manager.api.backup(p.server.ip_addr, p.ks, 't', p.snap, gated_store.address, gated_store.bucket_name, p.prefix)

        async def went_down():
            return True if gated_store.down else None
        await wait_for(went_down, deadline())
        try:
            await manager.api.abort_task(p.server.ip_addr, tid)
        except HTTPError as e:
            logger.info(f'The task had already ended: {e}')
        assert (await manager.api.wait_task(p.server.ip_addr, tid))['state'] == 'failed'
        uploaded = sum(o.size for o in (await objects_of(s3_storage, f'{p.prefix}/')).values())
        assert 0 < uploaded < size, f'the upload must be part-way: {uploaded} of {size} bytes'
        gated_store.recover()
        t = now_us()
        await p.covered()

        # The cut upload may lack schema.cql, so this target is made from the source's own schema, the same at this point.
        target = 'cut_target'
        source = (await p.cql.run_async(f'DESCRIBE TABLE {p.ks}.t'))[0].create_statement
        await p.cql.run_async(target_statement(source, p.ks, target))
        await p.refused(t, Reason.BACKUP_UNFINISHED, target=target)

        await do_backup(p.server, p.snap, p.prefix, p.ks, 't', s3_storage, manager, logger)
        _, target = await p.replay_done(t)
        assert await read_exact(p.cql, p.ks, target) == await read_exact(p.cql, p.ks, 't')


@needs_phase('ep.3')
async def test_a_replay_into_a_done_target_is_refused_naming_the_earlier_run(manager: ScyllaClusterManager, s3_storage):
    """R16-P2: REF-13. A second replay into a target that is already `done` is refused, the message names the earlier run's backup
    and T, and the target keeps its rows. Control: the first call was `done` and the target is not empty."""
    pks = keys_on_every_shard(2)
    async with pitr_node(manager, s3_storage) as p:
        await p.write(simple_rows(pks, old_ts(), 'seed'))
        await p.backup()
        t = now_us()
        await p.covered(t + 1_000_000)

        _, target = await p.replay_done(t)
        rows = await read_exact(p.cql, p.ks, target)
        assert rows, 'the first replay must have loaded the backup'
        t2 = t + 1000
        second, _ = await p.replay(t2, target)
        assert second.state == 'failed' and second.reason == Reason.TARGET_ALREADY_REPLAYED, f'a done target must be refused: {second}'
        assert p.snap in second.message, f'the refusal must name the earlier run\'s backup {p.snap}: {second}'
        assert iso(t) in second.message, f'the refusal must name the earlier run\'s T {iso(t)}: {second}'
        assert iso(t2) not in second.message, f'the refusal must not echo this request\'s T {iso(t2)}: {second}'
        assert await read_exact(p.cql, p.ks, target) == rows, 'the refused replay changed the target'
