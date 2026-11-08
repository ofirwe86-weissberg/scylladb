#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""What the node ships to the bucket, and when: nothing while shipping is off, and the seal interval's range, default and
timer (issue #22)."""

import asyncio
import logging
import time

import pytest

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    keys_on_every_shard, needs_phase, now_us, objects_of, pitr_node, report_stalls, segment_keys, SEGMENT_KEY_RE, SHARDS, SHIPPING_PREFIX, W,
    wait_log
)
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

SMALL_SEGMENTS = {'commitlog_segment_size_in_mb': 1}
# 24 writes of 128 KiB on each shard fill three 1 MiB segments there, so every shard closes segments.
WRITES_PER_SHARD = 24
VALUE_BYTES = 128 * 1024
SHIP_WINDOW_SECONDS = 10
OFF_LINE = r'pitr - Point-in-time recovery shipping is off: '


async def fill_segments(manager, p):
    """Writes enough on every shard to close several segments of 1 MiB, then flushes."""
    base = now_us()
    pks = keys_on_every_shard(WRITES_PER_SHARD)
    await p.write([W(base + i, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 0, ?) USING TIMESTAMP {{ts}}", ('x' * VALUE_BYTES,))
                   for i, pk in enumerate(pks)])
    await manager.api.keyspace_flush(p.server.ip_addr, p.ks, p.cf)


async def shipped_by(manager, p, shard):
    """The segments this node's shard has shipped, which tells it from another node ship to the same bucket."""
    return await segment_keys(p.storage, await manager.get_host_id(p.server.server_id), shard)


async def objects_shipped_by(manager, p):
    """Everything under the node's own shipping directory, segments and ledger entries alike."""
    return await objects_of(p.storage, f'{SHIPPING_PREFIX}/{await manager.get_host_id(p.server.server_id)}/')


async def wait_shipped_on_every_shard(manager, p, seconds=SHIP_WINDOW_SECONDS, pulse=False):
    """With pulse, one write goes to every shard at each poll: a shard seals a segment on the write that finds it due, not on its own."""
    async def shipped():
        if pulse:
            await p.pulse()
        return True if all([await shipped_by(manager, p, s) for s in range(SHARDS)]) else None
    await wait_for(shipped, time.time() + seconds, label=f'a segment shipped by each shard within {seconds} s')


async def stays_unshipped(manager, p, seconds=SHIP_WINDOW_SECONDS, pulse=False):
    """Nothing is shipped during a fixed window. The product's seal and upload cadence is the only clock there is for an absence,
    so this is the one place a test waits out a window; it fails the moment anything appears."""
    end = time.time() + seconds
    while time.time() < end:
        if pulse:
            await p.pulse()
        assert not await objects_shipped_by(manager, p), 'the node shipped something'
        await asyncio.sleep(0.5)


@needs_phase('ep.3')
async def test_nothing_is_shipped_when_off(manager: ScyllaClusterManager, s3_storage):
    """R01-P2: SHP-1. A node with no shipping endpoint logs that shipping is off and, whatever it writes, ships nothing.
    Control: a node with shipping on, given the same writes, ships a segment from each shard within 10 s."""
    async with pitr_node(manager, s3_storage, **SMALL_SEGMENTS) as on:
        await fill_segments(manager, on)
        await wait_shipped_on_every_shard(manager, on)

    async with pitr_node(manager, s3_storage, shipping=False, **SMALL_SEGMENTS) as off:
        log = await manager.server_open_log(server_id=off.server.server_id)
        await wait_log(log, OFF_LINE + 'pitr_endpoint and pitr_bucket are unset')
        await fill_segments(manager, off)
        assert not await objects_shipped_by(manager, off), 'a node with shipping off shipped'


@needs_phase('ep.3')
@pytest.mark.parametrize('seal', [1, 3600])
async def test_a_seal_interval_inside_the_range_ships(manager: ScyllaClusterManager, s3_storage, seal):
    """R28-P1: LVL-1. A seal interval at either end of 1..3600 s ships: segments closed by rollover reach the bucket from each
    shard within 10 s, even where the timer is an hour (the segments are 1 MiB, so rollover closes them). Control for R28-P2."""
    async with pitr_node(manager, s3_storage, pitr_seal_interval_in_seconds=seal, **SMALL_SEGMENTS) as p:
        await fill_segments(manager, p)
        await wait_shipped_on_every_shard(manager, p)


@needs_phase('ep.3')
@pytest.mark.parametrize('seal', [0, 3601])
async def test_a_seal_interval_outside_the_range_keeps_shipping_off(manager: ScyllaClusterManager, s3_storage, seal):
    """R28-P2: LVL-1. A seal interval outside 1..3600 s is logged as the cause, the node starts with shipping off, and writes
    ship nothing. R28-P1 is the control: the same writes ship with the interval inside the range."""
    async with pitr_node(manager, s3_storage, pitr_seal_interval_in_seconds=seal, **SMALL_SEGMENTS) as p:
        log = await manager.server_open_log(server_id=p.server.server_id)
        await wait_log(log, OFF_LINE + r'pitr_seal_interval_in_seconds must be between 1 and 3600')
        await fill_segments(manager, p)
        await stays_unshipped(manager, p)


@needs_phase('ep.3')
async def test_seal_interval_defaults_to_60(manager: ScyllaClusterManager, s3_storage):
    async with pitr_node(manager, s3_storage, shipping=False) as p:
        rows = await p.cql.run_async("SELECT value FROM system.config WHERE name = 'pitr_seal_interval_in_seconds'")
        assert [r.value for r in rows] == ['60']


def one_kib_per_shard():
    """A small write on each shard: far from filling a segment, so only the seal timer can close one."""
    return [W(now_us() + i, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 0, ?) USING TIMESTAMP {{ts}}", ('x' * 1024,))
            for i, pk in enumerate(keys_on_every_shard(1))]


@needs_phase('ep.3')
async def test_the_timer_seals_a_partly_filled_segment(manager: ScyllaClusterManager, s3_storage):
    """R28-P4: LVL-1. With the default segment size and a 1 s seal interval, one 1 KiB write on each shard, then a small write on
    each shard at every poll (a shard seals when a write finds the segment due), puts a segment in the bucket within 5 s, so
    the timer and not a rollover closed it. The control is the next test."""
    async with pitr_node(manager, s3_storage) as p:
        await p.write(one_kib_per_shard())
        await wait_shipped_on_every_shard(manager, p, 5, pulse=True)


@needs_phase('ep.3')
async def test_a_long_seal_interval_does_not_seal_a_partly_filled_segment(manager: ScyllaClusterManager, s3_storage):
    """Control for R28-P4: with a 3600 s interval, the same writes and pulses ship nothing over 10 s. It is a test of its own
    because a second node in the cluster would switch shipping off (single node only)."""
    async with pitr_node(manager, s3_storage, pitr_seal_interval_in_seconds=3600) as p:
        await p.write(one_kib_per_shard())
        await stays_unshipped(manager, p, pulse=True)
