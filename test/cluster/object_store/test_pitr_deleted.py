#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""PITR and deleted data: a deleted row stays deleted, and an accidental TRUNCATE can be undone."""

import asyncio
import logging

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    apply, DONE, get_result, needs_phase, node_config, now_us, old_ts, pks_of, protect_table, protected_table, PURGEABLE_OPTIONS, read_all,
    report_stalls, restore_ok, Result, simple_rows, start_restore, STEP_US, TABLE_SCHEMA, VNODES_KS, wait_covered, write_and_purge
)
from test.cluster.util import new_test_keyspace
from test.pylib.rest_client import HTTPError
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name

logger = logging.getLogger(__name__)

PURGEABLE_SCHEMA = f'{TABLE_SCHEMA} {PURGEABLE_OPTIONS}'


@needs_phase('1.3')
async def test_row_purged_before_the_backup_stays_deleted(manager: ScyllaClusterManager, s3_storage):
    """T13, RST-3. Only the shipped log still knows about row 1. write_and_purge() checks the purge happened, or this case proves nothing."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.purged {PURGEABLE_SCHEMA}')
        await write_and_purge(manager, server, cql, ks)
        backup = await protect_table(manager, server, s3_storage, ks, 'purged')
        latest = await wait_covered(manager, server, ks, 'purged')
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 'purged', backup.name, latest, new)
        seen = await pks_of(cql, ks, new)
        assert seen == {2}, f'expected row 2 only, because row 1 was purged. Saw {seen}'


@needs_phase('1.3')
async def test_row_deleted_after_the_backup_with_no_grace_stays_deleted(manager: ScyllaClusterManager, s3_storage):
    """T13, RST-3. The row is in the backup, and a restore mustn't purge its tombstone before loading it."""
    async with protected_table(manager, s3_storage, schema=PURGEABLE_SCHEMA) as p:
        ts = p.backup.start_us + STEP_US
        await p.cql.run_async(f'DELETE FROM {p.ks}.t USING TIMESTAMP {ts} WHERE pk = 0')
        await p.covered()
        _, new = await p.restore(ts)
        seen = await pks_of(p.cql, p.ks, new)
        assert seen == {1, 2, 3}, f'expected row 0 gone, saw {seen}'


@needs_phase('1.3')
async def test_row_deleted_above_t_is_present(manager: ScyllaClusterManager, s3_storage):
    """T13, RST-3."""
    async with protected_table(manager, s3_storage, schema=PURGEABLE_SCHEMA) as p:
        ts = p.backup.start_us + STEP_US
        await p.cql.run_async(f'DELETE FROM {p.ks}.t USING TIMESTAMP {ts} WHERE pk = 0')
        await p.covered()
        _, new = await p.restore(ts - 1)
        seen = await pks_of(p.cql, p.ks, new)
        assert seen == {0, 1, 2, 3}, f'expected row 0 still there, saw {seen}'


@needs_phase('1.3')
async def test_rows_truncated_before_the_backup_stay_gone(manager: ScyllaClusterManager, s3_storage):
    """T13, RST-3."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows([1, 2], old_ts()))
        await cql.run_async(f'TRUNCATE TABLE {ks}.t')
        await apply(cql, ks, 't', simple_rows([3], old_ts()))
        backup = await protect_table(manager, server, s3_storage, ks, 't')
        latest = await wait_covered(manager, server, ks, 't')
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, latest, new)
        seen = await pks_of(cql, ks, new)
        assert seen == {3}, f'expected only the row written after the truncate, saw {seen}'


@needs_phase('1.3')
async def test_compaction_during_a_restore_keeps_a_row_deleted(manager: ScyllaClusterManager, s3_storage):
    """T13, RST-3. **Proposed to move to developer tests:** this passes if every compaction misses the window, and nothing outside can hold the
    restore there."""
    async with protected_table(manager, s3_storage, schema=PURGEABLE_SCHEMA) as p:
        ts = p.backup.start_us + STEP_US
        await p.cql.run_async(f'DELETE FROM {p.ks}.t USING TIMESTAMP {ts} WHERE pk = 0')
        await p.covered()
        new = unique_name('restored_')
        started = await start_restore(manager, p.server, p.ks, p.cf, p.backup.name, ts, new)
        assert not isinstance(started, Result), f'expected a task, saw a refusal: {started}'
        waiting = asyncio.create_task(manager.api.wait_task(p.server.ip_addr, started))
        attempts = 0
        while not waiting.done():
            attempts += 1
            try:
                await manager.api.keyspace_compaction(p.server.ip_addr, p.ks, new)
            except HTTPError as e:      # the table may not exist yet
                logger.info(f'Compaction attempt {attempts}: {e}')
        await waiting
        logger.info(f'Attempted compaction {attempts} times during the restore')
        assert (await get_result(manager, p.server, p.ks, new)).state == DONE
        seen = await pks_of(p.cql, p.ks, new)
        assert seen == {1, 2, 3}, f'expected row 0 gone, saw {seen}'


async def truncate_after_t(p) -> tuple[int, list]:
    """Writes, and a TRUNCATE after them. Returns T, from before the TRUNCATE, and the writes."""
    writes = p.seed + simple_rows([10], p.backup.start_us + STEP_US)
    await p.write(writes[len(p.seed):])
    t = await p.covered()
    await p.cql.run_async(f'TRUNCATE TABLE {p.ks}.t')
    await p.covered()
    return t, writes


@needs_phase('1.3')
async def test_restore_to_before_a_truncate_brings_the_data_back(manager: ScyllaClusterManager, s3_storage):
    """T14, RST-4, REF-10. No restart, because one could hide a filter on truncated data."""
    async with protected_table(manager, s3_storage) as p:
        t, writes = await truncate_after_t(p)
        rows = await p.assert_restored_exact(writes, t)
        assert rows, 'expected the truncated data to be back'


@needs_phase('1.3')
async def test_source_stays_empty_after_a_restore_from_before_its_truncate(manager: ScyllaClusterManager, s3_storage):
    """T14, RST-4."""
    async with protected_table(manager, s3_storage) as p:
        t, _ = await truncate_after_t(p)
        await p.restore(t)
        seen = await read_all(p.cql, p.ks, p.cf)
        assert seen == [], f'expected the source to stay empty, saw {seen}'


@needs_phase('1.3')
async def test_source_stays_in_use_after_a_restore_from_before_its_truncate(manager: ScyllaClusterManager, s3_storage):
    """T14, RST-4."""
    async with protected_table(manager, s3_storage) as p:
        t, _ = await truncate_after_t(p)
        await p.restore(t)
        await p.write(simple_rows([20], now_us()))
        seen = await pks_of(p.cql, p.ks, p.cf)
        assert seen == {20}, f'expected the new write in the source, saw {seen}'
