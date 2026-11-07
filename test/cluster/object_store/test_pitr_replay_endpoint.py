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
import glob
import logging

import pytest

from test.cluster.object_store.test_backup import do_backup, take_snapshot_on_one_server
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Vnodes: a replay restores one table of one node from that node's own commitlog, which tablets do
# not give, and a tablets target is refused (REF-1).
RF_1 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'} AND tablets = {'enabled': false}"
TABLETS_KS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'} AND tablets = {'enabled': true}"
T = '2026-11-04T14:05:00.000000Z'


async def one_server(manager, object_storage, shipping=False):
    """One node, with the object store configured. With shipping on, a backup it takes carries the
    point-in-time bound a replay needs; with it off, the backup has none."""
    cfg = {'object_storage_endpoints': object_storage.create_endpoint_conf(), 'task_ttl_in_seconds': 300}
    if shipping:
        cfg |= {'pitr_endpoint': object_storage.address, 'pitr_bucket': object_storage.bucket_name,
                'pitr_prefix': 'pitr-log', 'pitr_seal_interval_in_seconds': 1}
    cmd = ['--logger-log-level', 'pitr_replay_to=debug:sstables_loader=info:api=info:compaction_manager=debug']
    return await manager.server_add(config=cfg, cmdline=cmd)


async def replay(manager, server, object_storage, ks, table, snapshot, prefix, t=T):
    tid = await manager.api.pitr_replay_to(server.ip_addr, ks, table, object_storage.address,
                                           object_storage.bucket_name, prefix, snapshot, t)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None, 'the replay task left no status'
    return status


async def backup_with_a_pitr_bound(manager, server, object_storage, ks, rows, schema_flag_only=False):
    """A backup of ks.source taken with shipping on, so that it carries a pitr bound, and the
    target created from it: what every replay that gets past the checks needs.

    With schema_flag_only, the target turns compaction off the way the README tells a user to,
    through its own schema, and nodetool is not called at all."""
    cql = manager.get_cql()
    await cql.run_async(f"CREATE TABLE {ks}.source (pk text primary key, value text);")
    await asyncio.gather(*(cql.run_async(f"INSERT INTO {ks}.source (pk, value) VALUES ('{k}', '{v}');")
                           for k, v in rows.items()))
    snap_name, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
    prefix = f'source/{snap_name}'
    await do_backup(server, snap_name, prefix, ks, 'source', object_storage, manager, logger)
    if schema_flag_only:
        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text) "
                            "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};")
    else:
        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text);")
        await manager.api.disable_autocompaction(server.ip_addr, ks, 'target')
    return snap_name, prefix


async def replay_marks(manager, server):
    """The durable marks this node wrote, one file per target it was asked to replay into."""
    workdir = await manager.server_get_workdir(server.server_id)
    return sorted(glob.glob(f'{workdir}/pitr/replay/*.replay'))


def refusal_of(status):
    """The reason code of a refused replay, which must not look like a crash or like a success."""
    assert status['state'] == 'failed', f'the replay was not refused: {status}'
    return status.get('error', '')


async def test_replay_refuses_a_target_that_does_not_exist(manager: ScyllaClusterManager, object_storage):
    server = await one_server(manager, object_storage)
    async with new_test_keyspace(manager, RF_1) as ks:
        status = await replay(manager, server, object_storage, ks, 'no_such_table', 'snap', 'prefix')
        assert 'target_missing' in refusal_of(status), refusal_of(status)


async def test_replay_refuses_a_target_in_a_tablets_keyspace(manager: ScyllaClusterManager, object_storage):
    server = await one_server(manager, object_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, TABLETS_KS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text);")
        status = await replay(manager, server, object_storage, ks, 'target', 'snap', 'prefix')
        assert 'target_uses_tablets' in refusal_of(status), refusal_of(status)


async def test_replay_refuses_a_malformed_t(manager: ScyllaClusterManager, object_storage):
    server = await one_server(manager, object_storage)
    async with new_test_keyspace(manager, RF_1) as ks:
        status = await replay(manager, server, object_storage, ks, 'no_such_table', 'snap', 'prefix',
                              t='2026-11-04 14:05:00')
        assert 't_invalid' in refusal_of(status), refusal_of(status)


async def test_replay_refuses_a_target_with_auto_compaction_enabled(manager: ScyllaClusterManager, object_storage):
    server = await one_server(manager, object_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text);")
        status = await replay(manager, server, object_storage, ks, 'target', 'snap', 'prefix')
        assert 'target_auto_compaction_enabled' in refusal_of(status), refusal_of(status)


async def test_replay_refuses_a_target_holding_data(manager: ScyllaClusterManager, object_storage):
    server = await one_server(manager, object_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text);")
        await manager.api.disable_autocompaction(server.ip_addr, ks, 'target')
        await cql.run_async(f"INSERT INTO {ks}.target (pk, value) VALUES ('a', 'b');")
        status = await replay(manager, server, object_storage, ks, 'target', 'snap', 'prefix')
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
        snap_name, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        prefix = f'source/{snap_name}'
        await do_backup(server, snap_name, prefix, ks, 'source', object_storage, manager, logger)

        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text);")
        await manager.api.disable_autocompaction(server.ip_addr, ks, 'target')
        status = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        assert 'backup_file_missing' in refusal_of(status), refusal_of(status)


async def test_replay_refuses_a_prefix_with_no_backup(manager: ScyllaClusterManager, object_storage):
    """A prefix holding no manifest holds no backup, so there is nothing to name."""
    server = await one_server(manager, object_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text);")
        await manager.api.disable_autocompaction(server.ip_addr, ks, 'target')
        status = await replay(manager, server, object_storage, ks, 'target', 'snap', 'nothing/is/here')
        assert 'backup_file_missing' in refusal_of(status), refusal_of(status)


async def test_replay_refuses_a_damaged_pitr_bound(manager: ScyllaClusterManager, object_storage):
    """The bound says where the backup's sstables end and the replay starts. One that will not
    parse cannot be guessed at, so the replay refuses the backup instead of starting from a bound
    it made up - and it is told apart from a backup that carries no bound at all."""
    if object_storage.type != 'S3':
        # Only the damage is written through boto3, which the GCS front refuses; the check it
        # exercises reads the object the same way whatever wrote it.
        pytest.skip('putting an object back into the bucket needs the S3 front')
    server = await one_server(manager, object_storage, shipping=True)
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        # Damage in the bucket, under the name the backup wrote: present, and unreadable.
        await asyncio.to_thread(object_storage.get_resource().Object(object_storage.bucket_name,
                                                                    f'{prefix}/pitr-bound.txt').put,
                                Body=b'format=pitr-bound\nversion=1\nthis is not a bound file\n')
        status = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        error = refusal_of(status)
        assert 'bound_file_damaged' in error, error
        assert 'backup_file_missing' not in error, error


async def test_replay_refuses_a_target_whose_schema_differs(manager: ScyllaClusterManager, object_storage):
    """The target carries a column the backup does not. The mutations the replay would apply were
    written against the backup's schema, so the two must match in everything but name, id and
    compaction."""
    server = await one_server(manager, object_storage, shipping=True)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        await cql.run_async(f"ALTER TABLE {ks}.target ADD extra int;")
        status = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        assert 'target_schema_differs' in refusal_of(status), refusal_of(status)


async def test_a_replay_that_passes_every_check_marks_the_target(manager: ScyllaClusterManager, object_storage):
    """The success path of ep.1: every check passes, the durable mark is written, and the task ends
    in failure because the load and the replay are ep.2 and ep.3 - a recovery that replayed nothing
    may not read as a finished one. The mark is what makes the target incomplete from then on, so a
    second call on it is refused."""
    server = await one_server(manager, object_storage, shipping=True)
    async with new_test_keyspace(manager, RF_1) as ks:
        assert not await replay_marks(manager, server), 'the node had a replay mark before any replay'
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        status = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        assert 'not built yet' in refusal_of(status), refusal_of(status)

        marks = await replay_marks(manager, server)
        assert len(marks) == 1, marks
        mark = open(marks[0]).read()
        assert 'phase=started' in mark, mark
        assert f'snapshot={snap_name}' in mark, mark

        second = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        assert 'target_already_replayed' in refusal_of(second), refusal_of(second)
