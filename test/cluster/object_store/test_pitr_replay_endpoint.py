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


async def start_replay(manager, server, object_storage, ks, table, snapshot, prefix, t=T):
    return await manager.api.pitr_replay_to(server.ip_addr, ks, table, object_storage.address,
                                            object_storage.bucket_name, prefix, snapshot, t)


async def replay(manager, server, object_storage, ks, table, snapshot, prefix, t=T):
    tid = await start_replay(manager, server, object_storage, ks, table, snapshot, prefix, t)
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


async def test_replay_refuses_a_prefix_holding_another_backup(manager: ScyllaClusterManager, object_storage):
    """One prefix is one backup. A call naming a snapshot the manifest under the prefix does not
    record would restore another table's data under the name the user asked for, so it is refused
    before anything is read from the backup's sstables."""
    server = await one_server(manager, object_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.source (pk text primary key, value text);")
        await cql.run_async(f"INSERT INTO {ks}.source (pk, value) VALUES ('0', 'zero');")
        snap_name, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        prefix = f'source/{snap_name}'
        await do_backup(server, snap_name, prefix, ks, 'source', object_storage, manager, logger)

        await cql.run_async(f"CREATE TABLE {ks}.target (pk text primary key, value text);")
        await manager.api.disable_autocompaction(server.ip_addr, ks, 'target')
        status = await replay(manager, server, object_storage, ks, 'target', 'a-backup-of-something-else', prefix)
        error = refusal_of(status)
        assert 'backup_names_another_table' in error, error
        assert snap_name in error, error


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
async def test_replay_loads_the_backup_into_the_target(manager: ScyllaClusterManager, object_storage):
    """The load is ep.2: the target holds the backup's rows after it, the source is untouched, and
    the run still ends in failure, because the history is not replayed yet (ep.3) and a recovery
    that replayed nothing may not read as a finished one."""
    server = await one_server(manager, object_storage, shipping=True)
    cql = manager.get_cql()
    rows = {'0': 'zero', '1': 'one', '2': 'two'}
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, rows)
        status = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        error = refusal_of(status)
        assert 'not built yet' in error, error
        # The result names the table the backup was taken from, not only the snapshot tag, which
        # can be shared: it is what tells the user which table they just loaded.
        assert f"loaded backup '{snap_name}' of {ks}.source" in error, error

        loaded = {x.pk: x.value for x in await cql.run_async(f"SELECT * FROM {ks}.target;")}
        assert loaded == rows, f'the target does not hold the backup after the load: {loaded}'
        kept = {x.pk: x.value for x in await cql.run_async(f"SELECT * FROM {ks}.source;")}
        assert kept == rows, f'the replay modified the source table: {kept}'


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_replay_fails_if_auto_compaction_is_enabled_while_it_runs(manager: ScyllaClusterManager, object_storage):
    """The hold keeps every compaction of the target off, but it cannot stop auto-compaction being
    turned back on. The run checks what it holds before it ends and fails rather than reporting a
    recovery it cannot vouch for: a compaction between the backup's sstables and the replayed
    history purges a tombstone before the writes it covers."""
    server = await one_server(manager, object_storage, shipping=True)
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        await manager.api.enable_injection(server.ip_addr, 'pitr_replay_loaded', one_shot=True)
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        tid = await start_replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        # Only once the run holds the target and has loaded the backup: enabling it earlier is
        # refused by the check that runs before the hold, which is another test's subject.
        await log.wait_for(f"backup '{snap_name}' loaded", from_mark=mark)
        await manager.api.enable_autocompaction(server.ip_addr, ks, 'target')
        await manager.api.message_injection(server.ip_addr, 'pitr_replay_loaded')
        status = await manager.api.wait_task(server.ip_addr, tid)
        error = refusal_of(status)
        assert 'cannot be trusted' in error, error
        assert 'not built yet' not in error, error

        # A run that failed after the mark was written leaves the target marked, so it reads as
        # incomplete and a second call on it is refused: the user drops it and replays into a new
        # table.
        second = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        assert 'target_already_replayed' in refusal_of(second), refusal_of(second)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_a_hold_that_fails_on_one_shard_leaves_no_hold_behind(manager: ScyllaClusterManager, object_storage):
    """Each reenabler holds the compaction state's gate, so a hold left behind on the shards that
    did take it would hang a later compaction or DROP of the table. A hold that fails anywhere is
    released everywhere before the run gives up."""
    server = await one_server(manager, object_storage, shipping=True)
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        await manager.api.enable_injection(server.ip_addr, 'pitr_hold_compaction_fail', one_shot=True)
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        status = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        assert 'Injected compaction hold failure' in refusal_of(status), refusal_of(status)

        # The shards that did take a hold gave it back, which the compaction manager logs.
        released = await log.grep('Reenabling compaction for', from_mark=mark)
        assert released, 'no shard released its compaction hold after the failure'
        # And nothing holds the table any more, so a major compaction of it runs instead of
        # waiting for a hold that nobody will release.
        await asyncio.wait_for(manager.api.keyspace_compaction(server.ip_addr, ks, 'target'), timeout=120)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_aborting_a_replay_releases_the_hold_and_keeps_the_mark(manager: ScyllaClusterManager, object_storage):
    """A user can abort the task. The hold goes with the run, and the mark stays: the target has
    the backup's sstables in it, so it is incomplete and a second call is refused."""
    server = await one_server(manager, object_storage, shipping=True)
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        await manager.api.enable_injection(server.ip_addr, 'pitr_replay_loaded', one_shot=True)
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        tid = await start_replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        await log.wait_for(f"backup '{snap_name}' loaded", from_mark=mark)
        await manager.api.abort_task(server.ip_addr, tid)
        await manager.api.message_injection(server.ip_addr, 'pitr_replay_loaded')
        status = await manager.api.wait_task(server.ip_addr, tid)
        assert status['state'] in ('failed', 'aborted'), status

        await asyncio.wait_for(manager.api.keyspace_compaction(server.ip_addr, ks, 'target'), timeout=120)
        second = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        assert 'target_already_replayed' in refusal_of(second), refusal_of(second)


async def test_replay_into_a_target_whose_schema_turns_compaction_off(manager: ScyllaClusterManager, object_storage):
    """The way the README tells a user to create the target: compaction off in its own schema, and
    nodetool never called. The run must get past the check it makes at the end as well as the one
    before the load, or every ordinary replay would end in "cannot be trusted"."""
    server = await one_server(manager, object_storage, shipping=True)
    cql = manager.get_cql()
    rows = {'0': 'zero', '1': 'one'}
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, rows,
                                                           schema_flag_only=True)
        status = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        error = refusal_of(status)
        assert 'cannot be trusted' not in error, error
        assert f"loaded backup '{snap_name}' of {ks}.source" in error, error

        loaded = {x.pk: x.value for x in await cql.run_async(f"SELECT * FROM {ks}.target;")}
        assert loaded == rows, f'the target does not hold the backup after the load: {loaded}'
