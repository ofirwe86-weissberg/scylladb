#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""The checks POST /storage_service/pitr/replay_to makes before it loads anything (issue #18, ep.1).

Each test drives the real endpoint and reads the task's state, because the refusal a user sees is
the task's failure text: a check that holds in C++ but never reaches the user lets a wrong restore
go ahead. The load of the backup and the replay of the shipped history are ep.2 and ep.3, so a call
that passes every check still ends in failure here, with "not built yet"."""

import asyncio
import logging

from test.cluster.object_store.test_backup import do_backup, take_snapshot_on_one_server
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

RF_1 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}"
T = '2026-11-04T14:05:00.000000Z'


async def one_server(manager, object_storage):
    cfg = {'object_storage_endpoints': object_storage.create_endpoint_conf(), 'task_ttl_in_seconds': 300}
    cmd = ['--logger-log-level', 'pitr_replay_to=debug:sstables_loader=info:api=info']
    return await manager.server_add(config=cfg, cmdline=cmd)


async def replay(manager, server, object_storage, ks, table, snapshot, prefix, sstables, t=T):
    tid = await manager.api.pitr_replay_to(server.ip_addr, ks, table, object_storage.address,
                                           object_storage.bucket_name, prefix, snapshot, t, sstables)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None, 'the replay task left no status'
    return status


def refusal_of(status):
    """The reason code of a refused replay, which must not look like a crash or like a success."""
    assert status['state'] == 'failed', f'the replay was not refused: {status}'
    return status.get('error', '')


async def test_replay_refuses_a_target_that_does_not_exist(manager: ScyllaClusterManager, object_storage):
    server = await one_server(manager, object_storage)
    async with new_test_keyspace(manager, RF_1) as ks:
        status = await replay(manager, server, object_storage, ks, 'no_such_table', 'snap', 'prefix', [])
        assert 'target_missing' in refusal_of(status), refusal_of(status)


async def test_replay_refuses_a_malformed_t(manager: ScyllaClusterManager, object_storage):
    server = await one_server(manager, object_storage)
    async with new_test_keyspace(manager, RF_1) as ks:
        status = await replay(manager, server, object_storage, ks, 'no_such_table', 'snap', 'prefix', [],
                              t='2026-11-04 14:05:00')
        assert 't_invalid' in refusal_of(status), refusal_of(status)


async def test_replay_refuses_a_target_with_auto_compaction_enabled(manager: ScyllaClusterManager, object_storage):
    server = await one_server(manager, object_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text);")
        status = await replay(manager, server, object_storage, ks, 'target', 'snap', 'prefix', [])
        assert 'target_auto_compaction_enabled' in refusal_of(status), refusal_of(status)


async def test_replay_refuses_a_target_holding_data(manager: ScyllaClusterManager, object_storage):
    server = await one_server(manager, object_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text);")
        await manager.api.disable_autocompaction(server.ip_addr, ks, 'target')
        await cql.run_async(f"INSERT INTO {ks}.target (pk, value) VALUES ('a', 'b');")
        status = await replay(manager, server, object_storage, ks, 'target', 'snap', 'prefix', [])
        assert 'target_not_empty' in refusal_of(status), refusal_of(status)


async def test_replay_refuses_a_backup_taken_without_a_pitr_bound(manager: ScyllaClusterManager, object_storage):
    """A backup taken with shipping off carries no pitr-bound.txt, so where the replay starts is
    unknown: the replay refuses it instead of loading the backup and stopping there."""
    server = await one_server(manager, object_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.source (pk text primary key, value text);")
        await asyncio.gather(*(cql.run_async(f"INSERT INTO {ks}.source (pk, value) VALUES ('{k}', '{v}');")
                               for k, v in [('0', 'zero'), ('1', 'one')]))
        snap_name, toc_names = await take_snapshot_on_one_server(ks, server, manager, logger)
        prefix = f'source/{snap_name}'
        await do_backup(server, snap_name, prefix, ks, 'source', object_storage, manager, logger)

        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text);")
        await manager.api.disable_autocompaction(server.ip_addr, ks, 'target')
        status = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix, toc_names)
        assert 'backup_file_missing' in refusal_of(status), refusal_of(status)
