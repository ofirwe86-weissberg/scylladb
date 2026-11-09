#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""PITR across a kill of the node: no `done` misses an acknowledged write, a `done` stays done, and an unfinished replay shows
`incomplete`. T9 and T10 of the QA test plan (issue #22)."""

import asyncio
import contextlib
import logging

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    assert_acked_writes_present, assert_exact, deadline, FAILED, FAR_FUTURE_US, first_history_read_after_load, gated_store,
    latest_restorable, needs_phase, now_us, old_ts, pitr_node, read_all, read_exact, Reason, report_stalls, RESULT_LOGGED_WORDS,
    objects_of, SEAL_SECONDS, SEGMENT_KEY_RE, SHIPPING_PREFIX, simple_rows, TABLE_SCHEMA, Writer
)
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

SEEDED = range(4)
# What the README measured for the time to upload a sealed segment (U, at most 1.4 s there), with room for a slow host.
U_ALLOWANCE_US = 3_000_000


@needs_phase('ep.3')
async def test_kill_never_gives_done_missing_acked_writes(manager: ScyllaClusterManager, s3_storage):
    """T9: A writer on both shards records each acknowledgement, and the node is killed under it. The latest
    restorable time L comes from a refusal. Replay to L has every acknowledged write at or below L, and L + 1 is refused."""
    async with pitr_node(manager, s3_storage) as p:
        await p.write(simple_rows(SEEDED, old_ts()))
        await p.backup()
        writer = Writer(p.cql, p.ks, 't', first_pk=1000).start()
        await writer.wait_acked(50)
        before_kill = await p.covered()
        await writer.wait_acked(len(writer.acked) + 50)

        async def segments():
            return {k for k in await objects_of(s3_storage, SHIPPING_PREFIX) if SEGMENT_KEY_RE.search(k)}
        segments_before = await segments()
        last_ack = max(ack for _, ack in writer.acked.values())
        await p.restart(kill=True)
        writer.stopping = True
        await asyncio.wait_for(writer.task, 120)

        far = now_us() + FAR_FUTURE_US
        outcome, _ = await p.refused(far, Reason.T_PAST_COVERAGE)
        latest = latest_restorable(outcome, far)
        assert latest >= before_kill, f'coverage went back across the kill: {latest} < {before_kill}'

        _, target = await p.replay_done(latest)
        required = await assert_acked_writes_present(p.cql, p.ks, target, [writer], latest, SEEDED)
        assert required, 'no acknowledged write was covered, so this proves nothing'
        bound = (SEAL_SECONDS + 1) * 1_000_000 + U_ALLOWANCE_US
        # The loss is measured from the last write acknowledged before the kill. Whether the restarted node ships the killed node's
        # leftover segments, which would hide a loss, is logged: the chains and the segments that appeared across the restart.
        segments_after = await segments()
        logger.info(f'R27-S1: loss {(last_ack - latest) / 1e6:.2f} s (bound {bound / 1e6:.1f} s); latest - before_kill = {(latest - before_kill) / 1e6:.2f} s; '
                    f'chains before {sorted({SEGMENT_KEY_RE.search(k).group(2) for k in segments_before})}, '
                    f'after {sorted({SEGMENT_KEY_RE.search(k).group(2) for k in segments_after})}; '
                    f'{len(segments_after - segments_before)} segments appeared after the restart, {len(segments_before - segments_after)} vanished')
        assert last_ack - latest <= bound, f'the loss at the kill is {(last_ack - latest) / 1e6:.1f} s, over S + U + 1 s = {bound / 1e6:.1f} s'
        await p.refused(latest + 1, Reason.T_PAST_COVERAGE)


async def replay_setup(p):
    """A backup, writes after it, and table `other`, which a replay must leave alone. Returns T and the writes."""
    await p.cql.run_async(f'CREATE TABLE {p.ks}.other {TABLE_SCHEMA}')
    await p.write(simple_rows(range(10), old_ts(), 'other'), table='other')
    await p.write(simple_rows(SEEDED, old_ts()))
    await p.backup()
    writes = simple_rows(range(100, 110), now_us())
    await p.write(writes)
    await p.covered()
    return writes[-1].ts, writes


@needs_phase('ep.3', 'st.1')
async def test_unfinished_replay_shows_incomplete_across_a_kill(manager: ScyllaClusterManager, gated_store):
    """T10 (a): The replay is held after it read the backup and before it read the history, and the
    node is killed there. Status lists the target `incomplete` before and after the restart, and other tables read the same."""
    async with pitr_node(manager, gated_store) as p:
        t, _ = await replay_setup(p)
        target = await p.new_target()
        gated_store.hold_when(first_history_read_after_load())
        replay = asyncio.create_task(p.replay(t, target))

        async def held():
            return True if gated_store.holding.is_set() or replay.done() else None
        await wait_for(held, deadline())
        assert gated_store.holding.is_set(), f'the replay ended before the history was read: {replay.result()}'
        await p.assert_incomplete(target, required=True)
        before = {cf: await read_all(p.cql, p.ks, cf) for cf in ('t', 'other')}

        replay.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await replay
        await p.restart(kill=True, while_down=gated_store.recover)
        await p.assert_incomplete(target, required=True)
        for cf, rows in before.items():
            assert await read_all(p.cql, p.ks, cf) == rows, f'{cf} changed across the kill of a replay'


@needs_phase('ep.3', 'st.1')
async def test_done_is_durable_across_a_kill(manager: ScyllaClusterManager, s3_storage):
    """T10 (b): The node is killed right after `done`. The target reads as
    before, isn't `incomplete`, and a second replay on it is refused, saying where the result was logged."""
    async with pitr_node(manager, s3_storage) as p:
        t, writes = await replay_setup(p)
        _, target = await p.replay_done(t)
        await assert_exact(p.cql, p.ks, target, writes, t)
        rows = await read_exact(p.cql, p.ks, target)

        await p.restart(kill=True)
        assert await read_exact(p.cql, p.ks, target) == rows, f'{target} changed across a kill after done'
        status = await p.status()
        assert status is not None and (p.ks, target) not in status.incomplete, f'a done target must not be incomplete: {status}'

        outcome, _ = await p.replay(t, target)
        assert outcome.state == FAILED and outcome.reason == Reason.TARGET_ALREADY_REPLAYED, f'a done target must be refused: {outcome}'
        for word in RESULT_LOGGED_WORDS:
            assert word in outcome.message.lower(), f'the refusal must say where the result was logged: {outcome}'
        assert await read_exact(p.cql, p.ks, target) == rows
