#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""PITR backup and status: what makes a table protected, and what the status report says."""

import asyncio
import logging
import uuid

import pytest

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    apply, assert_refused, assert_stays_unprotected, backup_prefix, CLOCK_MARGIN_US, deadline, get_status, needs_phase, node_config, NONDURABLE_KS,
    NOT_PROTECTED, now_us, old_ts, pks_of, PROTECTED, protected_table, read_all, Reason, report_stalls, restore_ok, run_nodetool, run_status_commands,
    simple_rows, STATUS_LIMIT_WORDS, table_entry, TABLE_SCHEMA, table_status, TABLETS_KS, VNODES_KS, wait_covered, wait_protected
)
from test.cluster.object_store.test_backup import do_backup, take_snapshot_on_one_server
from test.cluster.util import new_test_keyspace
from test.pylib.s3mock_server import create_bucket
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name, wait_for

logger = logging.getLogger(__name__)


async def unbacked_table(manager, storage):
    """A node with shipping on, and table t in ks, seeded, with no backup. For use inside new_test_keyspace."""
    return await manager.server_add(config=node_config(storage))


async def create_seeded(manager, ks):
    cql = manager.get_cql()
    await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
    await apply(cql, ks, 't', simple_rows(range(4), old_ts()))


@needs_phase('1.2')
async def test_table_with_no_backup_is_not_protected(manager: ScyllaClusterManager, s3_storage):
    """T07, STA-1."""
    server = await unbacked_table(manager, s3_storage)
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await create_seeded(manager, ks)
        entry = await table_status(manager, server, ks, 't')
        assert (entry.state, entry.reason) == (NOT_PROTECTED, Reason.NO_BACKUP), entry
        assert entry.commands, f'expected the exact backup commands: {entry}'
        assert not entry.backups, f'expected no usable backups: {entry}'


@needs_phase('1.2')
async def test_status_states_its_limits(manager: ScyllaClusterManager, s3_storage):
    """T07, STA-1. Limits 1 and 16."""
    server = await unbacked_table(manager, s3_storage)
    limits = (await get_status(manager, server)).limits_text.lower()
    missing = [word for word in STATUS_LIMIT_WORDS if word not in limits]
    assert not missing, f'expected {missing} in the limits, saw: {limits}'


@needs_phase('1.2')
async def test_status_says_the_store_is_reachable(manager: ScyllaClusterManager, s3_storage):
    """T07, STA-1."""
    server = await unbacked_table(manager, s3_storage)
    status = await get_status(manager, server)
    assert status.store_reachable and not status.store_error, status


@needs_phase('1.2')
async def test_status_commands_make_a_table_protected(manager: ScyllaClusterManager, s3_storage):
    """T07, SHP-5. Real nodetool, with the commands exactly as status gives them, is the only step."""
    server = await unbacked_table(manager, s3_storage)
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await create_seeded(manager, ks)
        entry = await table_status(manager, server, ks, 't')
        await run_status_commands(manager, server, entry.commands)
        await wait_protected(manager, server, ks, 't')


@needs_phase('1.2')
async def test_protected_table_shows_its_backup_and_lag(manager: ScyllaClusterManager, s3_storage):
    """T07, T08, STA-1."""
    async with protected_table(manager, s3_storage) as p:
        latest = await p.covered()
        entry = await p.status()
        assert entry.reason is None, entry
        assert [b.name for b in entry.backups] == [p.backup.name], entry
        assert entry.latest_us >= latest >= p.backup.start_us, entry
        assert entry.lag_seconds is not None and entry.lag_seconds >= 0, entry


@needs_phase('1.3')
async def test_restore_from_status_commands_matches_the_source(manager: ScyllaClusterManager, s3_storage):
    """T07, SHP-5."""
    server = await unbacked_table(manager, s3_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await create_seeded(manager, ks)
        await run_status_commands(manager, server, (await table_status(manager, server, ks, 't')).commands)
        backup = await wait_protected(manager, server, ks, 't')
        latest = await wait_covered(manager, server, ks, 't')
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, latest, new)
        assert await read_all(cql, ks, new) == await read_all(cql, ks, 't')


@needs_phase('1.2')
async def test_status_gives_one_entry_per_table(manager: ScyllaClusterManager, s3_storage):
    """T08, STA-1. Two tables, one protected: table_entry() fails unless each has exactly one answer."""
    async with protected_table(manager, s3_storage) as p:
        await p.cql.run_async(f'CREATE TABLE {p.ks}.other {TABLE_SCHEMA}')
        status = await get_status(manager, p.server)
        assert table_entry(status, p.ks, 't').state == PROTECTED
        assert table_entry(status, p.ks, 'other').state == NOT_PROTECTED


@needs_phase('1.2')
async def test_status_gives_tablets_as_the_reason_for_a_tablet_table(manager: ScyllaClusterManager, s3_storage):
    """T08, STA-1."""
    server = await unbacked_table(manager, s3_storage)
    async with new_test_keyspace(manager, TABLETS_KS) as ks:
        await manager.get_cql().run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        entry = await table_status(manager, server, ks, 't')
        assert (entry.state, entry.reason) == (NOT_PROTECTED, Reason.TABLETS), entry


@needs_phase('1.2')
async def test_status_gives_the_view_as_the_reason_for_a_table_with_a_view(manager: ScyllaClusterManager, s3_storage):
    """T08, STA-1."""
    server = await unbacked_table(manager, s3_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))')
        await cql.run_async(f'CREATE MATERIALIZED VIEW {ks}.v AS SELECT * FROM {ks}.t WHERE pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (ck, pk)')
        entry = await table_status(manager, server, ks, 't')
        assert (entry.state, entry.reason) == (NOT_PROTECTED, Reason.VIEW_BASE), entry


@needs_phase('1.2')
async def test_status_gives_durable_writes_as_the_reason(manager: ScyllaClusterManager, s3_storage):
    """T08, STA-1."""
    server = await unbacked_table(manager, s3_storage)
    async with new_test_keyspace(manager, NONDURABLE_KS) as ks:
        await manager.get_cql().run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        entry = await table_status(manager, server, ks, 't')
        assert (entry.state, entry.reason) == (NOT_PROTECTED, Reason.DURABLE), entry


@needs_phase('1.3')
async def test_restored_table_is_not_protected(manager: ScyllaClusterManager, s3_storage):
    """T08, STA-1."""
    async with protected_table(manager, s3_storage) as p:
        _, new = await p.restore(await p.covered())
        entry = await table_status(manager, p.server, p.ks, new)
        assert (entry.state, entry.reason) == (NOT_PROTECTED, Reason.RESTORED_TABLE), entry


@needs_phase('1.3')
async def test_restored_table_is_protected_after_its_own_backup(manager: ScyllaClusterManager, s3_storage):
    """T08, STA-1."""
    async with protected_table(manager, s3_storage) as p:
        _, new = await p.restore(await p.covered())
        entry = await table_status(manager, p.server, p.ks, new)
        await run_status_commands(manager, p.server, entry.commands)
        await wait_protected(manager, p.server, p.ks, new)


async def backup_by_another_route(manager, server, storage, teardowns, ks, route) -> str:
    """Takes a backup that shouldn't protect the table. Returns its name."""
    if route == 'skip_flush':
        snap = unique_name('skipflush_')
        await run_nodetool(manager, server, ['nodetool', 'snapshot', '--skip-flush', '--tag', snap, ks])
    else:
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
    bucket = storage.bucket_name
    prefix = backup_prefix(ks, 't', snap)
    if route == 'other_prefix':
        prefix = f'elsewhere/{snap}'
    elif route == 'other_bucket':
        bucket = f'pitr-other-{uuid.uuid4().hex[:12]}'
        await asyncio.to_thread(create_bucket, storage.get_resource(), bucket)

        def destroy_bucket():
            other = storage.get_resource().Bucket(bucket)
            other.objects.all().delete()
            other.delete()
        teardowns.append(destroy_bucket)
    tid = await manager.api.backup(server.ip_addr, ks, 't', snap, storage.address, bucket, prefix)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status['state'] == 'done', f'expected the backup itself to work: {status}'
    return snap


ROUTES = ['other_bucket', 'other_prefix', 'skip_flush']


@needs_phase('1.2')
@pytest.mark.parametrize('route', ROUTES)
async def test_backup_by_another_route_leaves_table_not_protected(manager: ScyllaClusterManager, s3_storage, scylla_cluster_teardowns, route):
    """T30, SHP-5."""
    server = await unbacked_table(manager, s3_storage)
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await create_seeded(manager, ks)
        await backup_by_another_route(manager, server, s3_storage, scylla_cluster_teardowns, ks, route)
        entry = await assert_stays_unprotected(manager, server, ks, 't')
        assert entry.reason == Reason.NO_BACKUP and entry.commands, entry


@needs_phase('1.3')
@pytest.mark.parametrize('route', ROUTES)
async def test_restore_refuses_a_backup_by_another_route(manager: ScyllaClusterManager, s3_storage, scylla_cluster_teardowns, route):
    """T30, SHP-5."""
    server = await unbacked_table(manager, s3_storage)
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await create_seeded(manager, ks)
        snap = await backup_by_another_route(manager, server, s3_storage, scylla_cluster_teardowns, ks, route)
        await assert_refused(manager, server, manager.get_cql(), ks, 't', snap, now_us() - CLOCK_MARGIN_US, Reason.BACKUP_NOT_USABLE)


@needs_phase('1.2')
@pytest.mark.parametrize('route', ROUTES)
async def test_status_commands_protect_after_another_route(manager: ScyllaClusterManager, s3_storage, scylla_cluster_teardowns, route):
    """T30, SHP-5. The control: nothing but the route stopped the table being protected."""
    server = await unbacked_table(manager, s3_storage)
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await create_seeded(manager, ks)
        await backup_by_another_route(manager, server, s3_storage, scylla_cluster_teardowns, ks, route)
        await run_status_commands(manager, server, (await table_status(manager, server, ks, 't')).commands)
        await wait_protected(manager, server, ks, 't')


@needs_phase('1.2')
async def test_interval_starts_at_the_snapshot_time(manager: ScyllaClusterManager, s3_storage):
    """T32, REF-6."""
    server = await unbacked_table(manager, s3_storage)
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await create_seeded(manager, ks)
        before = now_us()
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        after = now_us()
        await write_until_past(manager.get_cql(), ks, after + 2 * CLOCK_MARGIN_US)
        await do_backup(server, snap, backup_prefix(ks, 't', snap), ks, 't', s3_storage, manager, logger)
        backup = await wait_protected(manager, server, ks, 't', snap)
        assert before - CLOCK_MARGIN_US <= backup.start_us <= after + CLOCK_MARGIN_US, f'expected the snapshot time, {before} to {after}, saw {backup}'


async def write_until_past(cql, ks, ts):
    """A write above the snapshot, then a wait until its timestamp has passed, so that it's before the upload starts."""
    await apply(cql, ks, 't', simple_rows([10], ts))

    async def time_passed():
        return True if now_us() > ts else None
    await wait_for(time_passed, deadline())


@needs_phase('1.3')
@pytest.mark.parametrize('side', ['at_the_write', 'just_below_the_write', 'at_the_interval_start'])
async def test_t_between_snapshot_and_upload_is_exact(manager: ScyllaClusterManager, s3_storage, side):
    """T32, REF-6, RST-2. The write is after the snapshot and before the upload starts, so T there can only work if the interval starts at the snapshot."""
    server = await unbacked_table(manager, s3_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await create_seeded(manager, ks)
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        ts = now_us() + 2 * CLOCK_MARGIN_US
        await write_until_past(cql, ks, ts)
        await do_backup(server, snap, backup_prefix(ks, 't', snap), ks, 't', s3_storage, manager, logger)
        backup = await wait_protected(manager, server, ks, 't', snap)
        await wait_covered(manager, server, ks, 't')

        new = unique_name('restored_')
        t = {'at_the_write': ts, 'just_below_the_write': ts - 1, 'at_the_interval_start': backup.start_us}[side]
        await restore_ok(manager, server, ks, 't', snap, t, new)
        expected = {0, 1, 2, 3, 10} if side == 'at_the_write' else {0, 1, 2, 3}
        seen = await pks_of(cql, ks, new)
        assert seen == expected, f'expected keys {expected} at {t}, saw {seen}'
