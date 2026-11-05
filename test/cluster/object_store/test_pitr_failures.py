#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""PITR failures and restarts: a brief store failure, a long outage, a stop of the node, and a crash around a restore."""

import asyncio
import logging

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import pytest

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    assert_acked_writes_present, assert_exact, assert_status_stays, backup_prefix, CAUSE_OUTAGE, CAUSE_RESTART, CFG_UNSHIPPED_LIMIT_MB,
    change_statements, CLOCK_MARGIN_US, deadline, DONE, gated_store, get_result, get_status, INCOMPLETE, INCOMPLETE_ACTION_WORDS, iso,
    iso_times, needs_phase, NOT_PROTECTED, now_us, objects_of, old_ts, PAST_COVERAGE_WORDS, PROTECTED, protected_table, read_all, Reason,
    report_stalls, Result, run_status_commands, seed_statements, simple_rows, start_restore, STEP_US, STEPS, table_entry, TABLE_SCHEMA, table_status,
    UNCOMPRESSED_SCHEMA, wait_protected, write_several_sstables, Writer
)
from test.cluster.object_store.pitr_util import restart as restart_node
from test.cluster.object_store.test_backup import take_snapshot_on_one_server
from test.pylib.rest_client import HTTPError
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name, wait_for

logger = logging.getLogger(__name__)

OUTAGE_LIMIT_MB = 1


async def wait_store_unreachable(manager, server):
    async def unreachable():
        status = await get_status(manager, server)
        return status if not status.store_reachable else None
    return await wait_for(unreachable, deadline())


# A brief store failure, below the outage limit

@asynccontextmanager
async def brief_failure(manager, gate):
    """A protected table with a covered write, and the store cut and seen to be cut. The test decides what recovers it."""
    async with protected_table(manager, gate, seed=[]) as p:
        before = simple_rows(range(4), p.backup.start_us + STEP_US, 'before')
        await p.write(before)
        await p.covered()
        gate.fail()
        await wait_store_unreachable(manager, p.server)
        yield p, before


@needs_phase('1.3')
async def test_writes_succeed_during_a_brief_store_failure(manager: ScyllaClusterManager, gated_store):
    """T04, OUT-1."""
    async with brief_failure(manager, gated_store) as (p, _):
        await p.write(simple_rows([10], now_us()))
        assert 10 in await _keys(p)


async def _keys(p):
    return {r[0] for r in await p.cql.run_async(f'SELECT pk FROM {p.ks}.{p.cf}')}


@needs_phase('1.3')
async def test_status_shows_the_store_error_during_a_brief_failure(manager: ScyllaClusterManager, gated_store):
    """T04, OUT-1."""
    async with brief_failure(manager, gated_store) as (p, _):
        status = await get_status(manager, p.server)
        assert not status.store_reachable and status.store_error, f'expected unreachable with an error: {status}'


@needs_phase('1.3')
async def test_lag_rises_during_a_brief_failure(manager: ScyllaClusterManager, gated_store):
    """T04, OUT-1."""
    async with brief_failure(manager, gated_store) as (p, _):
        first = (await p.status()).lag_seconds

        async def rose():
            return True if (await p.status()).lag_seconds > first else None
        await wait_for(rose, deadline())


@needs_phase('1.3')
async def test_brief_failure_doesnt_break_protection(manager: ScyllaClusterManager, gated_store):
    """T04, OUT-1. Every poll is kept, so that a break that comes and goes is seen. wait_for() would swallow an assertion."""
    async with brief_failure(manager, gated_store) as (p, _):
        seen = []

        async def lag_rose_and_recovered():
            entry = await p.status()
            seen.append(entry)
            return True if entry.lag_seconds is not None and entry.lag_seconds > seen[0].lag_seconds else None
        await wait_for(lag_rose_and_recovered, deadline())
        gated_store.recover()
        target = now_us() + CLOCK_MARGIN_US

        async def caught_up_or_broke():
            entry = await p.status()
            seen.append(entry)
            return True if entry.state != PROTECTED or entry.break_cause is not None or entry.latest_us >= target else None
        await wait_for(caught_up_or_broke, deadline())
        broken = [e for e in seen if e.state != PROTECTED or e.break_cause is not None]
        assert not broken, f'expected no break in {len(seen)} polls, saw: {broken}'


@needs_phase('1.3')
async def test_lag_falls_after_a_brief_failure(manager: ScyllaClusterManager, gated_store):
    """T04, OUT-1."""
    async with brief_failure(manager, gated_store) as (p, _):
        first = (await p.status()).lag_seconds

        async def rose():
            lag = (await p.status()).lag_seconds
            return lag if lag > first else None
        peak = await wait_for(rose, deadline())
        gated_store.recover()

        async def fell():
            return True if (await p.status()).lag_seconds < peak else None
        await wait_for(fell, deadline())


@needs_phase('1.3')
@pytest.mark.parametrize('offset', [0, -1], ids=['at_the_write', 'just_below_the_write'])
async def test_t_inside_a_brief_failure_is_exact(manager: ScyllaClusterManager, gated_store, offset):
    """T04, OUT-1."""
    async with brief_failure(manager, gated_store) as (p, before):
        ts = now_us()
        during = simple_rows(range(10, 14), ts, 'during')
        await p.write(during)
        gated_store.recover()
        await p.covered()
        await p.assert_restored_exact(before + during, ts + offset)


# A long outage, past the limit

@dataclass
class Outage:
    p: Any
    covered_writes: list
    covered: int
    failed_at: int
    writers: list
    status: Any
    entry: Any


@asynccontextmanager
async def long_outage(manager, gate):
    """A protected table, the store cut, and writers running until status shows the break. The store is still down on entry."""
    async with protected_table(manager, gate, seed=[], **{CFG_UNSHIPPED_LIMIT_MB: OUTAGE_LIMIT_MB}) as p:
        covered_writes = simple_rows(range(4), p.backup.start_us + STEP_US, 'covered')
        await p.write(covered_writes)
        covered = await p.covered()
        failed_at = now_us()
        gate.fail()
        writers = [Writer(p.cql, p.ks, p.cf, min_ts=covered + 1, first_pk=1000 + 100000 * i, payload='x' * 1024).start() for i in range(3)]

        async def broke_or_a_write_failed():
            status = await get_status(manager, p.server)
            entry = table_entry(status, p.ks, p.cf)
            ended = entry.break_cause is not None or entry.state != PROTECTED or any(w.failure for w in writers)
            return (status, entry) if ended else None
        try:
            status, entry = await wait_for(broke_or_a_write_failed, deadline())
        finally:
            for w in writers:
                await w.stop()
        failures = [w.failure for w in writers if w.failure is not None]
        assert not failures, f'expected every write to succeed during the outage, saw: {failures}'
        assert entry.break_cause == CAUSE_OUTAGE, f'expected a break with cause {CAUSE_OUTAGE}: {entry}'
        yield Outage(p, covered_writes, covered, failed_at, writers, status, entry)


async def recovered(manager, gate, o):
    """Brings the store back, and returns the table's status once the node has seen it."""
    gate.recover()

    async def back():
        return True if (await get_status(manager, o.p.server)).store_reachable else None
    await wait_for(back, deadline())
    return await o.p.status()


@needs_phase('1.3')
async def test_writes_succeed_past_the_outage_limit(manager: ScyllaClusterManager, gated_store):
    """T05, OUT-3."""
    async with long_outage(manager, gated_store) as o:
        failures = [w.failure for w in o.writers if w.failure is not None]
        assert not failures, f'expected every write to succeed, saw: {failures}'
        assert all(w.acked for w in o.writers), 'expected writes acknowledged during the outage'


@needs_phase('1.3')
async def test_status_answers_during_an_outage_and_says_the_store_is_down(manager: ScyllaClusterManager, gated_store):
    """T05, OUT-3, STA-1."""
    async with long_outage(manager, gated_store) as o:
        assert not o.status.store_reachable and o.status.store_error, f'expected unreachable with an error: {o.status}'


@needs_phase('1.3')
async def test_outage_shows_a_break_with_its_cause_and_time(manager: ScyllaClusterManager, gated_store):
    """T05, OUT-3."""
    async with long_outage(manager, gated_store) as o:
        assert (o.entry.state, o.entry.reason) == (NOT_PROTECTED, Reason.BROKEN), o.entry
        assert o.failed_at - CLOCK_MARGIN_US <= o.entry.break_time_us <= now_us() + CLOCK_MARGIN_US, \
            f'expected the time of the cut, {iso(o.failed_at)}: {o.entry}'


@needs_phase('1.3')
async def test_break_from_an_outage_lasts_after_the_store_returns(manager: ScyllaClusterManager, gated_store):
    """T05, OUT-3."""
    async with long_outage(manager, gated_store) as o:
        await recovered(manager, gated_store, o)
        await assert_status_stays(manager, o.p.server, o.p.ks, o.p.cf, lambda e: e.state == NOT_PROTECTED, 'a break to last until a new backup')


@needs_phase('1.3')
async def test_coverage_doesnt_advance_during_an_outage(manager: ScyllaClusterManager, gated_store):
    """T05, OUT-3. Nothing acknowledged after the cut can be in the store."""
    async with long_outage(manager, gated_store) as o:
        entry = await recovered(manager, gated_store, o)
        assert o.covered <= entry.latest_us <= o.failed_at + CLOCK_MARGIN_US, \
            f'expected coverage from {iso(o.covered)} to the cut at {iso(o.failed_at)}: {entry}'


@needs_phase('1.3')
async def test_t_from_before_an_outage_is_exact(manager: ScyllaClusterManager, gated_store):
    """T05, OUT-3."""
    async with long_outage(manager, gated_store) as o:
        await recovered(manager, gated_store, o)
        await o.p.assert_restored_exact(o.covered_writes, o.covered)


@needs_phase('1.3')
async def test_t_at_coverage_after_an_outage_misses_no_acknowledged_write(manager: ScyllaClusterManager, gated_store):
    """T05, OUT-3."""
    async with long_outage(manager, gated_store) as o:
        entry = await recovered(manager, gated_store, o)
        _, new = await o.p.restore(entry.latest_us)
        await assert_acked_writes_present(o.p.cql, o.p.ks, new, o.writers, entry.latest_us, range(4))


@needs_phase('1.3')
async def test_t_1us_past_coverage_after_an_outage_is_refused(manager: ScyllaClusterManager, gated_store):
    """T05, OUT-3, REF-5."""
    async with long_outage(manager, gated_store) as o:
        entry = await recovered(manager, gated_store, o)
        result = await o.p.refused(entry.latest_us + 1, Reason.T_PAST_COVERAGE)
        assert entry.latest_us in iso_times(result.message), f'expected the latest restorable time in: {result.message}'


@needs_phase('1.3')
async def test_status_remedy_restores_protection_after_an_outage(manager: ScyllaClusterManager, gated_store):
    """T05, OUT-3."""
    async with long_outage(manager, gated_store) as o:
        entry = await recovered(manager, gated_store, o)
        if entry.restart_required:
            await o.p.restart()
        await run_status_commands(manager, o.p.server, entry.commands)
        await wait_protected(manager, o.p.server, o.p.ks, o.p.cf)


# A stop of the node, clean or not

STOPS = pytest.mark.parametrize('kill', [False, True], ids=['clean_restart', 'kill'])


@dataclass
class Stopped:
    p: Any
    writes: list
    known: int
    writer: Any
    stopped_at: int
    entry: Any


@asynccontextmanager
async def stopped_during_writes(manager, storage, kill):
    """A protected table, stopped and started with a writer running. Every in-flight write has a timestamp above `known`, so T=known can't be ambiguous.
    The writer ran until coverage passed some of its writes, so that some were acknowledged before coverage."""
    async with protected_table(manager, storage, seed=[]) as p:
        writes = simple_rows(range(4), p.backup.start_us + STEP_US, 'covered')
        await p.write(writes)
        known = await p.covered()
        writer = Writer(p.cql, p.ks, p.cf, min_ts=known + 1).start()
        await writer.wait_acked(20)
        await p.covered(max(ack for _, ack in writer.acked.values()) + CLOCK_MARGIN_US)
        await writer.wait_acked(len(writer.acked) + 20)
        stopped_at = now_us()
        await p.restart(kill)
        writer.stopping = True
        await writer.task
        yield Stopped(p, writes, known, writer, stopped_at, await p.status())


@needs_phase('1.3')
@STOPS
async def test_stop_shows_a_break_with_its_cause_and_time(manager: ScyllaClusterManager, s3_storage, kill):
    """T06, SHP-7."""
    async with stopped_during_writes(manager, s3_storage, kill) as s:
        assert (s.entry.state, s.entry.reason, s.entry.break_cause) == (NOT_PROTECTED, Reason.BROKEN, CAUSE_RESTART), s.entry
        assert s.stopped_at - CLOCK_MARGIN_US <= s.entry.break_time_us <= now_us() + CLOCK_MARGIN_US, f'expected the time of the stop: {s.entry}'


@needs_phase('1.3')
@STOPS
async def test_coverage_doesnt_go_back_after_a_stop(manager: ScyllaClusterManager, s3_storage, kill):
    """T06, SHP-7."""
    async with stopped_during_writes(manager, s3_storage, kill) as s:
        assert s.entry.latest_us >= s.known, f'expected coverage at or past {iso(s.known)}: {s.entry}'


@needs_phase('1.3')
@STOPS
async def test_t_from_before_a_stop_is_exact(manager: ScyllaClusterManager, s3_storage, kill):
    """T06, SHP-7."""
    async with stopped_during_writes(manager, s3_storage, kill) as s:
        await s.p.assert_restored_exact(s.writes, s.known)


@needs_phase('1.3')
@STOPS
async def test_t_at_coverage_after_a_stop_misses_no_acknowledged_write(manager: ScyllaClusterManager, s3_storage, kill):
    """T06, SHP-7. Writes acknowledged within 1 second of coverage aren't required, because of the clock margin: a developer test covers those."""
    async with stopped_during_writes(manager, s3_storage, kill) as s:
        _, new = await s.p.restore(s.entry.latest_us)
        required = await assert_acked_writes_present(s.p.cql, s.p.ks, new, [s.writer], s.entry.latest_us, range(4))
        assert required, 'expected some writes acknowledged before coverage, or this test proves nothing'


@needs_phase('1.3')
@STOPS
async def test_t_1us_past_coverage_after_a_stop_is_refused(manager: ScyllaClusterManager, s3_storage, kill):
    """T06, REF-5."""
    async with stopped_during_writes(manager, s3_storage, kill) as s:
        result = await s.p.refused(s.entry.latest_us + 1, Reason.T_PAST_COVERAGE)
        assert s.entry.latest_us in iso_times(result.message), f'expected the latest restorable time in: {result.message}'
        missing = [word for word in PAST_COVERAGE_WORDS if word not in result.message.lower()]
        assert not missing, f'expected {missing} in: {result.message}'


@needs_phase('1.3')
@STOPS
async def test_write_after_a_stop_is_refused_until_a_new_backup(manager: ScyllaClusterManager, s3_storage, kill):
    """T06, SHP-7, REF-5."""
    async with stopped_during_writes(manager, s3_storage, kill) as s:
        after = simple_rows([5000], s.entry.latest_us + 1000 * STEP_US, 'after')
        await s.p.write(after)
        await s.p.refused(after[0].ts, Reason.T_PAST_COVERAGE)


@needs_phase('1.3')
@STOPS
async def test_status_commands_recover_after_a_stop(manager: ScyllaClusterManager, s3_storage, kill):
    """T06, SHP-7."""
    async with stopped_during_writes(manager, s3_storage, kill) as s:
        await s.p.write(simple_rows([5000], s.entry.latest_us + 1000 * STEP_US, 'after'))
        await run_status_commands(manager, s.p.server, s.entry.commands)
        newer = await wait_protected(manager, s.p.server, s.p.ks, s.p.cf)
        assert newer.name != s.p.backup.name, f'expected a new backup: {newer}'
        _, new = await s.p.restore(await s.p.covered(), backup=newer.name)
        seen = {r[0] for r in await s.p.cql.run_async(f'SELECT pk FROM {s.p.ks}.{new}')}
        assert 5000 in seen, f'expected the write from after the stop, saw {sorted(seen)[:10]}'


# A crash around a restore

@needs_phase('1.4')
async def test_finished_restore_keeps_its_data_after_a_kill(manager: ScyllaClusterManager, s3_storage):
    """T16, RST-12."""
    pks = list(range(8))
    seed = seed_statements(pks, old_ts())
    async with protected_table(manager, s3_storage, seed=seed) as p:
        changes, ts = change_statements(pks, p.backup.start_us)
        await p.write(changes)
        await p.covered()
        _, new = await p.restore(ts['static'])
        before = await read_all(p.cql, p.ks, new)
        await p.restart(kill=True)
        after = await read_all(p.cql, p.ks, new)
        assert after == before, f'expected the same rows. Missing: {sorted(set(before) - set(after))}. Extra: {sorted(set(after) - set(before))}'


@needs_phase('1.4')
async def test_finished_restore_keeps_its_result_after_a_kill(manager: ScyllaClusterManager, s3_storage):
    """T16, RST-7, RST-12."""
    async with protected_table(manager, s3_storage) as p:
        result, new = await p.restore(await p.covered())
        await p.restart(kill=True)
        seen = (await get_result(manager, p.server, p.ks, new)).raw
        assert seen == result.raw, f'expected the same result. Before: {result.raw}. After: {seen}'


@needs_phase('1.4')
async def test_finished_restore_isnt_incomplete_after_a_kill(manager: ScyllaClusterManager, s3_storage):
    """T16, RST-12."""
    async with protected_table(manager, s3_storage) as p:
        _, new = await p.restore(await p.covered())
        await p.restart(kill=True)
        entry = await table_status(manager, p.server, p.ks, new)
        assert entry.state != INCOMPLETE, entry


@needs_phase('1.3')
async def test_kill_during_a_restore_leaves_done_or_incomplete(manager: ScyllaClusterManager, gated_store):
    """T17, RST-12, REF-13, STA-1. Outcome only, with no moment to hit: the store is held, so the restore can't finish. The node is killed only once status
    shows the restore, so this can't pass without one having begun. Which step it reached is for the developer's tests. Either way, other tables are
    unchanged, and the name can't be reused."""
    async with protected_table(manager, gated_store) as p:
        await p.cql.run_async(f'CREATE TABLE {p.ks}.other {TABLE_SCHEMA}')
        changes = simple_rows(range(10, 14), p.backup.start_us + STEP_US)
        await p.write(changes)
        t = await p.covered()
        before = {cf: await read_all(p.cql, p.ks, cf) for cf in (p.cf, 'other')}
        new = unique_name('restored_')
        gated_store.hold()
        # The request may itself wait for the store, so it isn't awaited before looking at status.
        starting = asyncio.create_task(start_restore(manager, p.server, p.ks, p.cf, p.backup.name, t, new))

        async def restore_shows_or_was_refused():
            if starting.done():
                started = starting.result()
                assert not isinstance(started, Result), f'expected a task, saw a refusal: {started}'
            found = [e for e in (await get_status(manager, p.server)).tables if (e.keyspace, e.table) == (p.ks, new)]
            return True if found else None
        await wait_for(restore_shows_or_was_refused, deadline())
        p.cql = await restart_node(manager, p.server, kill=True, while_down=gated_store.recover)
        starting.cancel()

        entry = await table_status(manager, p.server, p.ks, new)
        if entry.state == INCOMPLETE:
            assert entry.step in STEPS, f'expected one of {STEPS}: {entry}'
            missing = [word for word in INCOMPLETE_ACTION_WORDS if word not in entry.text.lower()]
            assert not missing, f'expected {missing} in: {entry.text}'
        else:
            assert (await get_result(manager, p.server, p.ks, new)).state == DONE, f'expected done or incomplete: {entry}'
            await assert_exact(p.cql, p.ks, new, p.seed + changes, t)
        for cf, rows in before.items():
            seen = await read_all(p.cql, p.ks, cf)
            assert seen == rows, f'expected {cf} the same. Missing: {sorted(set(rows) - set(seen))[:5]}. Extra: {sorted(set(seen) - set(rows))[:5]}'
        await p.refused(t, Reason.NAME_TAKEN, new_table=new)


@needs_phase('1.3')
async def test_cut_upload_is_either_usable_or_refused_and_unlisted(manager: ScyllaClusterManager, gated_store):
    """T23, REF-7. Outcome only, with no moment to hit: the store is cut after half the table's bytes have gone towards it. The backup sends far more than
    shipping does on an idle node, so most of them are its own, and a failed upload is required to be part-way. Exact cut points are for the
    developer's tests."""
    async with protected_table(manager, gated_store, seed=[], schema=UNCOMPRESSED_SCHEMA) as p:
        await write_several_sstables(manager, p.server, p.cql, p.ks)
        cut, _ = await take_snapshot_on_one_server(p.ks, p.server, manager, logger)
        size = await manager.api.get_table_disk_space_used(p.server.ip_addr, p.ks, p.cf)
        gated_store.fail_after_bytes(size // 2)
        prefix = backup_prefix(p.ks, p.cf, cut)
        tid = await manager.api.backup(p.server.ip_addr, p.ks, p.cf, cut, gated_store.address, gated_store.bucket_name, prefix)

        async def cut_or_finished():
            if gated_store.down:
                return True
            state = (await manager.api.get_task_status(p.server.ip_addr, tid))['state']
            return True if state in ('done', 'failed') else None
        await wait_for(cut_or_finished, deadline())
        if gated_store.down:
            try:
                await manager.api.abort_task(p.server.ip_addr, tid)
            except HTTPError as e:
                logger.info(f'The task had already ended: {e}')
        state = (await manager.api.wait_task(p.server.ip_addr, tid))['state']
        gated_store.recover()

        if state == 'done':
            await wait_protected(manager, p.server, p.ks, p.cf, cut)
            await p.restore(await p.covered(), backup=cut)
        else:
            uploaded = sum(o.size for o in (await objects_of(gated_store, f'{prefix}/')).values())
            assert 0 < uploaded < size, f'expected the backup itself cut part-way, or this is the never-uploaded case again: {uploaded} of {size} bytes'
            result = await p.refused(await p.covered(), Reason.BACKUP_UNFINISHED, backup=cut)
            assert p.backup.name in result.message, f'expected the usable backups listed in: {result.message}'
            await assert_status_stays(manager, p.server, p.ks, p.cf, lambda e: [b.name for b in e.backups] == [p.backup.name], f'only {p.backup.name} listed')
