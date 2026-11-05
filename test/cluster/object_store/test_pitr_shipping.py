#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""PITR shipping: when it's on, what it stores, and that coverage keeps moving."""

import logging
import time

import pytest

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    ABSENCE_WINDOW_SECONDS, apply, assert_nothing_appears, CFG_SEAL_SECONDS, get_status, needs_phase, node_config, now_us, objects_of, old_ts, pks_of,
    protect_table, protected_table, report_stalls, restart, restore_ok, SEAL_SECONDS, SHARDS, shipped_bytes, shipping_options, SHIPPING_PREFIX,
    simple_rows, STEP_US, TABLE_SCHEMA, VNODES_KS, W, wait_covered
)
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name, wait_for

logger = logging.getLogger(__name__)

IDLE_WINDOW_US = 10_000_000


async def off_node(manager, storage):
    return await manager.server_add(config=node_config(storage, shipping=False, **{CFG_SEAL_SECONDS: SEAL_SECONDS}))


@needs_phase('1.1')
async def test_status_says_shipping_is_off_by_default(manager: ScyllaClusterManager, s3_storage):
    """T01, SHP-1."""
    server = await off_node(manager, s3_storage)
    status = await get_status(manager, server)
    assert not status.shipping_on, f'expected shipping off, status says on: {status}'


@needs_phase('1.1')
async def test_nothing_is_shipped_when_off(manager: ScyllaClusterManager, s3_storage):
    """T01, SHP-1. Means something only with test_config_and_restart_turn_shipping_on, which shows the same window catches shipping."""
    server = await off_node(manager, s3_storage)
    cql = manager.get_cql()
    assert not (await get_status(manager, server)).shipping_on
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows(range(4), old_ts()))
        await manager.api.flush_keyspace(server.ip_addr, ks)
        await assert_nothing_appears(s3_storage)


@needs_phase('1.1')
async def test_config_and_restart_turn_shipping_on(manager: ScyllaClusterManager, s3_storage):
    """T01, SHP-1. Objects appear within the window that test_nothing_is_shipped_when_off watches."""
    server = await off_node(manager, s3_storage)
    await manager.server_update_config(server.server_id, config_options=shipping_options(s3_storage))
    cql = await restart(manager, server)
    assert (await get_status(manager, server)).shipping_on, 'expected shipping on after config and restart'
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows(range(4), old_ts()))

        async def something_shipped():
            return True if await objects_of(s3_storage, SHIPPING_PREFIX) else None
        await wait_for(something_shipped, time.time() + ABSENCE_WINDOW_SECONDS)


@needs_phase('1.3')
@pytest.mark.parametrize('created', ['before_shipping', 'after_shipping'])
async def test_every_table_is_shipped_once_on(manager: ScyllaClusterManager, s3_storage, created):
    """T01, SHP-1. A table from either side of turning shipping on gets its later writes restored."""
    server = await off_node(manager, s3_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        if created == 'before_shipping':
            await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await manager.server_update_config(server.server_id, config_options=shipping_options(s3_storage))
        cql = await restart(manager, server)
        if created == 'after_shipping':
            await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        backup = await protect_table(manager, server, s3_storage, ks, 't')
        await apply(cql, ks, 't', simple_rows([10], backup.start_us + STEP_US))
        latest = await wait_covered(manager, server, ks, 't')
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, latest, new)
        assert await pks_of(cql, ks, new) == {10}


@needs_phase('1.1')
async def test_status_says_why_shipping_is_off_on_two_nodes(manager: ScyllaClusterManager, s3_storage):
    """T02, SHP-1."""
    servers = await manager.servers_add(2, config=node_config(s3_storage))
    for server in servers:
        status = await get_status(manager, server)
        assert not status.shipping_on, f'expected shipping off on two nodes: {status}'
        assert 'node' in status.off_reason.lower(), f'expected a reason naming the nodes, saw: {status.off_reason!r}'


@needs_phase('1.1')
async def test_nothing_is_shipped_on_two_nodes(manager: ScyllaClusterManager, s3_storage):
    """T02, SHP-1. Means something only with test_config_and_restart_turn_shipping_on, which uses the same configuration."""
    servers = await manager.servers_add(2, config=node_config(s3_storage))
    cql = manager.get_cql()
    assert not (await get_status(manager, servers[0])).shipping_on
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows(range(4), now_us()))
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)
        await assert_nothing_appears(s3_storage)


def one_kb_write(ts):
    return [W(ts, "INSERT INTO {t} (pk, ck, v) VALUES (1, 0, '" + 'x' * 1024 + "') USING TIMESTAMP {ts}")]


@needs_phase('1.3')
async def test_one_small_write_costs_under_1_mib(manager: ScyllaClusterManager, s3_storage):
    """T03, SHP-6. At a 1 second seal interval, not the default, to stay fast. Reserved space would show just the same."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        await p.covered()
        before = await shipped_bytes(s3_storage)
        await p.write(one_kb_write(p.backup.start_us + STEP_US))
        await p.covered()
        grew = await shipped_bytes(s3_storage) - before
        assert 0 < grew < 1024 * 1024, f'expected one 1 KB write to cost between 0 and 1 MiB, it cost {grew} bytes'


@needs_phase('1.3')
async def test_restore_after_one_small_write_has_only_that_write(manager: ScyllaClusterManager, s3_storage):
    """T03, SHP-6."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        statements = one_kb_write(p.backup.start_us + STEP_US)
        await p.write(statements)
        rows = await p.assert_restored_exact(statements, await p.covered())
        assert len(rows) == 1, f'expected one row, saw {rows}'


@needs_phase('1.2')
async def test_coverage_passes_a_write_with_the_other_shard_idle(manager: ScyllaClusterManager, s3_storage):
    """T09, LVL-1. One key is on one shard of two. The time taken is logged, not gated: the S + U bound is a developer test (req:82)."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        ts = max(now_us(), p.backup.start_us + STEP_US)
        await p.write(simple_rows([1], ts))
        await p.covered(ts)
        logger.info(f'One write covered after {(now_us() - ts) / 1e6} seconds')


@needs_phase('1.3')
async def test_restore_at_a_writes_timestamp_includes_it(manager: ScyllaClusterManager, s3_storage):
    """T09, LVL-1."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        ts = max(now_us(), p.backup.start_us + STEP_US)
        await p.write(simple_rows([1], ts))
        await p.covered()
        _, new = await p.restore(ts)
        assert await pks_of(p.cql, p.ks, new) == {1}


@needs_phase('1.2')
async def test_coverage_advances_with_no_writes(manager: ScyllaClusterManager, s3_storage):
    """T09, LVL-1."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        latest = await p.covered()
        await p.covered(latest + IDLE_WINDOW_US)


@needs_phase('1.2')
async def test_idle_node_grows_under_4_kib_per_shard_per_seal_interval(manager: ScyllaClusterManager, s3_storage):
    """T09, LVL-1."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        latest = await p.covered()
        started_at = time.time()
        before = await shipped_bytes(s3_storage)
        await p.covered(latest + IDLE_WINDOW_US)
        grew = await shipped_bytes(s3_storage) - before
        intervals = (time.time() - started_at) / SEAL_SECONDS
        assert grew < 4096 * SHARDS * intervals, f'expected under {4096 * SHARDS * intervals} bytes in {intervals} seal intervals, saw {grew}'


@needs_phase('1.2')
async def test_idle_node_writes_no_zero_byte_objects(manager: ScyllaClusterManager, s3_storage):
    """T09, LVL-1."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        latest = await p.covered()
        await p.covered(latest + IDLE_WINDOW_US)
        objects = await objects_of(s3_storage, SHIPPING_PREFIX)
        assert objects, 'expected some objects to look at'
        empty = [k for k, o in objects.items() if o.size == 0]
        assert not empty, f'expected no zero-byte objects, saw {empty}'
