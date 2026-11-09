#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""POST /storage_service/pitr/replay_to: its checks, and the run they let through (issue #18).

Each test drives the real endpoint and reads the task's state, because the refusal a user sees is
the task's failure text: a check that holds in C++ but never reaches the user lets a wrong restore
go ahead. The run itself is here too, as far as a cluster test can see it: the target marked `done`
only after the whole run, a failure in the middle leaving it `started`, and the compaction hold
lasting until the mark."""

import asyncio
import glob
import logging
import time

import pytest

from test.cluster.object_store.pitr_util import from_iso, iso, now_us, SHARDS, shard_of
from test.cluster.object_store.test_backup import do_backup, take_snapshot_on_one_server
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.skip_types import skip_env
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

# Vnodes: a replay restores one table of one node from that node's own commitlog, which tablets do
# not give, and a tablets target is refused.
RF_1 = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'} AND tablets = {'enabled': false}"
TABLETS_KS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'} AND tablets = {'enabled': true}"
T = '2026-11-04T14:05:00.000000Z'
LOG_LEVELS = 'pitr_replay_to=debug:sstables_loader=info:api=info:compaction_manager=debug'


async def one_server(manager, object_storage, shipping=False, seal_seconds=1, loggers=LOG_LEVELS, cmdline_extra=()):
    """One node, with the object store configured. With shipping on, a backup it takes carries the
    point-in-time bound a replay needs; with it off, the backup has none.

    The seal interval, the log levels and extra command-line options are arguments because the
    measured run (#21) needs the product's own seal interval and the shipper's debug lines, while
    every test here wants a seal every second and nothing else."""
    cfg = {'object_storage_endpoints': object_storage.create_endpoint_conf(), 'task_ttl_in_seconds': 300}
    if shipping:
        cfg |= {'pitr_endpoint': object_storage.address, 'pitr_bucket': object_storage.bucket_name,
                'pitr_prefix': 'pitr-log', 'pitr_seal_interval_in_seconds': seal_seconds}
    cmd = ['--logger-log-level', loggers, *cmdline_extra]
    return await manager.server_add(config=cfg, cmdline=cmd)


async def start_replay(manager, server, object_storage, ks, table, snapshot, prefix, t=T):
    return await manager.api.pitr_replay_to(server.ip_addr, ks, table, object_storage.address,
                                            object_storage.bucket_name, prefix, snapshot, t)


async def replay(manager, server, object_storage, ks, table, snapshot, prefix, t=T):
    tid = await start_replay(manager, server, object_storage, ks, table, snapshot, prefix, t)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None, 'the replay task left no status'
    return status


async def backup_with_a_pitr_bound(manager, server, object_storage, ks, rows, schema_flag_only=False, seed=None):
    """A backup of ks.source taken with shipping on, so that it carries a pitr bound, and the
    target created from it: what every replay that gets past the checks needs.

    With schema_flag_only, the target turns compaction off the way the README tells a user to,
    through its own schema, and nodetool is not called at all. `seed` fills the source instead of
    `rows`, for a backup too large to insert in one gather (#21)."""
    cql = manager.get_cql()
    await cql.run_async(f"CREATE TABLE {ks}.source (pk text primary key, value text);")
    if seed is not None:
        await seed()
    else:
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


def succeeded(status):
    assert status['state'] == 'done', f'the replay did not finish: {status}'


async def marks_of(manager, server):
    """Every mark this node wrote, as text, so a test can say what phase each target is in."""
    return [open(path).read() for path in await replay_marks(manager, server)]


async def one_key_per_shard(cql, table, literals):
    """One of `literals` per shard, the one that lands on it. The keys go into `table` first,
    because a token can only be read off a row that exists, so the table must be one the caller
    does not mind holding them."""
    await asyncio.gather(*(cql.run_async(f"INSERT INTO {table} (pk) VALUES ({k});") for k in literals))
    one_per_shard = {}
    for pk, token in await cql.run_async(f"SELECT pk, token(pk) FROM {table};"):
        one_per_shard.setdefault(shard_of(token), pk)
    assert set(one_per_shard) == set(range(SHARDS)), f'the keys must reach every shard, they reach {sorted(one_per_shard)}'
    return [one_per_shard[shard] for shard in range(SHARDS)]


async def text_keys_one_per_shard(cql, ks):
    """One text key per shard, picked in a probe table of its own. A token depends on the key's
    value alone, so these keys land on the same shards in any table with a text partition key."""
    await cql.run_async(f"CREATE TABLE {ks}.probe (pk text primary key);")
    keys = await one_key_per_shard(cql, f'{ks}.probe', [f"'k{i}'" for i in range(32)])
    logger.info('the per-shard text keys are %s', keys)
    return keys


async def make_pulse(cql, ks):
    """A write on every shard, on demand, in a table of its own.

    Coverage is the lowest of the shards', each shard's moves only when that shard seals a segment
    it has written to, and it starts at 0: one idle shard holds the whole node at 1970 and refuses
    every replay (README limit 10). A table of its own, so the source a replay reads holds exactly the rows the
    test wrote."""
    await cql.run_async(f"CREATE TABLE {ks}.pulse (pk int primary key);")
    keys = sorted(await one_key_per_shard(cql, f'{ks}.pulse', [str(pk) for pk in range(32)]))
    logger.info('the pulse writes %s, one key per shard', keys)

    async def pulse():
        await asyncio.gather(*(cql.run_async(f"INSERT INTO {ks}.pulse (pk) VALUES ({pk});") for pk in keys))
    return pulse


async def coverage_reached(manager, server, object_storage, ks, table, snapshot, prefix):
    """How far the shipped history covers, as the endpoint itself reports it. A T no chain can
    cover is always refused with `t_past_coverage`, and that refusal leaves no mark, so this probe
    can be repeated on the real target."""
    status = await replay(manager, server, object_storage, ks, table, snapshot, prefix, t=iso(now_us() + 3_600_000_000))
    error = refusal_of(status)
    assert 't_past_coverage' in error, error
    return from_iso(error.split('has only reached ')[1].split(',')[0])


async def wait_for_coverage(manager, server, object_storage, ks, table, snapshot, prefix, pulse, us):
    """Waits until the shipped history covers a time, pulsing every shard while it waits."""
    async def covered():
        await pulse()
        reached = await coverage_reached(manager, server, object_storage, ks, table, snapshot, prefix)
        if reached < us:
            logger.info('the history covers %s, waiting for %s', iso(reached), iso(us))
            return None
        return reached
    return await wait_for(covered, time.time() + 120)


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
        skip_env('putting an object back into the bucket needs the S3 front')
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


async def test_a_full_run_marks_the_target_done(manager: ScyllaClusterManager, object_storage):
    """The whole run, to a T above writes made after the backup: the target holds the backup's rows
    and every write below T and none above it, the task succeeds, the target's mark says `done`,
    and the one result line is in the node log, split writes and all. `done` is also final: a second
    call on the target is refused, because replaying into it again would write over a restore the
    user already has.

    The writes below T cover every shard, because each shard replays its own history: a run that
    reads one shard's segments only would otherwise pass on its mark alone."""
    server = await one_server(manager, object_storage, shipping=True)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        assert not await replay_marks(manager, server), 'the node had a replay mark before any replay'
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        pulse = await make_pulse(cql, ks)
        per_shard = await text_keys_one_per_shard(cql, ks)
        await cql.run_async(f"INSERT INTO {ks}.source (pk, value) VALUES ('1', 'after the backup');")
        await asyncio.gather(*(cql.run_async(f"INSERT INTO {ks}.source (pk, value) VALUES ('{k}', 'below T');")
                               for k in per_shard))
        expected = {'0': 'zero', '1': 'after the backup'} | {k: 'below T' for k in per_shard}
        t_us = now_us() + 1_000_000
        t = iso(t_us)
        await wait_for_coverage(manager, server, object_storage, ks, 'target', snap_name, prefix, pulse, t_us)
        # Above T, and shipped: waiting until the history covers it means the run has to drop this
        # write at T rather than simply never seeing it.
        await cql.run_async(f"INSERT INTO {ks}.source (pk, value) VALUES ('above-t', 'above T');")
        await wait_for_coverage(manager, server, object_storage, ks, 'target', snap_name, prefix, pulse, now_us())
        log = await manager.server_open_log(server.server_id)
        log_mark = await log.mark()
        succeeded(await replay(manager, server, object_storage, ks, 'target', snap_name, prefix, t))

        # What the run was for: the rows, not the mark. Exactly the backup plus the writes below T,
        # on every shard, and nothing from above T.
        restored = {x.pk: x.value for x in await cql.run_async(f"SELECT * FROM {ks}.target;")}
        assert restored == expected, f'the target does not hold the backup and the writes below T: {restored}'

        marks = await marks_of(manager, server)
        assert len(marks) == 1, marks
        assert 'phase=done' in marks[0], marks[0]
        assert f'snapshot={snap_name}' in marks[0], marks[0]
        # One line in the node log is where a user reads what the run did, until the status GET of
        # #14. "up to" is the split-write count, which the result must carry even when it is zero:
        # a restore that silently left writes out would otherwise read as a complete one.
        result = await log.grep(f'replay of {ks}.target to t={t} .* is done:', from_mark=log_mark)
        assert result, 'the run wrote no result line to the node log'
        assert 'up to' in result[0][0], result[0][0]

        second = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix, t)
        assert 'target_already_replayed' in refusal_of(second), refusal_of(second)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_a_failure_in_the_middle_of_the_replay_leaves_the_target_started(manager: ScyllaClusterManager, object_storage):
    """A run that breaks after it has applied part of the history must never say `done`: the target
    holds some of the writes up to T and not the rest, and only the `started` mark tells the user
    that what they are looking at is incomplete."""
    server = await one_server(manager, object_storage, shipping=True)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        pulse = await make_pulse(cql, ks)
        await wait_for_coverage(manager, server, object_storage, ks, 'target', snap_name, prefix, pulse, from_iso(T))
        await manager.api.enable_injection(server.ip_addr, 'pitr_replay_segment_replayed', one_shot=True)
        status = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        assert 'Injected replay failure' in refusal_of(status), refusal_of(status)

        marks = await marks_of(manager, server)
        assert len(marks) == 1, marks
        assert 'phase=started' in marks[0], marks[0]
        assert 'phase=done' not in marks[0], marks[0]


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_the_compaction_hold_lasts_until_the_target_is_done(manager: ScyllaClusterManager, object_storage):
    """The hold covers the replay and the flush, not the load alone: a compaction between the
    backup's sstables landing and the history being replayed over them purges a tombstone before
    the writes it covers, and no later check can see that it happened. The run is parked after the
    flush and before the mark, where the hold must still be in force."""
    server = await one_server(manager, object_storage, shipping=True)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        pulse = await make_pulse(cql, ks)
        await wait_for_coverage(manager, server, object_storage, ks, 'target', snap_name, prefix, pulse, from_iso(T))
        await manager.api.enable_injection(server.ip_addr, 'pitr_replay_flushed', one_shot=True)
        log = await manager.server_open_log(server.server_id)
        log_mark = await log.mark()
        tid = await start_replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        await log.wait_for('pitr_replay_flushed: waiting for message', from_mark=log_mark)
        # The compaction manager logs each reenabler it gives back, so the absence of the line is
        # the hold still being in force. Asking for a compaction instead would prove nothing: a
        # major on a held view returns at once without compacting.
        assert not await log.grep('Reenabling compaction for', from_mark=log_mark), 'the hold was released before the target was done'
        # The park sits between the flush and the mark, so `done` written any earlier shows up
        # here: a target marked done with its writes still in a memtable loses them on a crash.
        parked = await marks_of(manager, server)
        assert len(parked) == 1, parked
        assert 'phase=started' in parked[0], parked[0]

        await manager.api.message_injection(server.ip_addr, 'pitr_replay_flushed')
        succeeded(await manager.api.wait_task(server.ip_addr, tid))
        assert await log.grep('Reenabling compaction for', from_mark=log_mark), 'the finished run kept its compaction hold'
        marks = await marks_of(manager, server)
        assert len(marks) == 1, marks
        assert 'phase=done' in marks[0], marks[0]


async def test_replay_loads_the_backup_into_the_target(manager: ScyllaClusterManager, object_storage):
    """The target holds the backup's rows after the run, the source is untouched, and the hold the
    run took is given back. T here is below every write, so the history adds nothing: what the
    target holds is the backup."""
    server = await one_server(manager, object_storage, shipping=True)
    cql = manager.get_cql()
    rows = {'0': 'zero', '1': 'one', '2': 'two'}
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, rows)
        pulse = await make_pulse(cql, ks)
        await wait_for_coverage(manager, server, object_storage, ks, 'target', snap_name, prefix, pulse, from_iso(T))
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        succeeded(await replay(manager, server, object_storage, ks, 'target', snap_name, prefix))
        # The hold goes with the run: the compaction manager logs each reenabler it gives back.
        # Asking for a compaction instead would prove nothing, since a major on a held view
        # returns at once without compacting.
        assert await log.grep('Reenabling compaction for', from_mark=mark), 'the run kept its compaction hold'
        # The result names the table the backup was taken from, not only the snapshot tag, which
        # can be shared: it is what tells the user which table they just restored.
        assert await log.grep(f"restored from backup '{snap_name}' of {ks}.source", from_mark=mark), 'the result does not name the backup'

        loaded = {x.pk: x.value for x in await cql.run_async(f"SELECT * FROM {ks}.target;")}
        assert loaded == rows, f'the target does not hold the backup after the load: {loaded}'
        kept = {x.pk: x.value for x in await cql.run_async(f"SELECT * FROM {ks}.source;")}
        assert kept == rows, f'the replay modified the source table: {kept}'


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize('park', ['pitr_replay_loaded', 'pitr_replay_flushed'])
async def test_replay_fails_if_auto_compaction_is_enabled_while_it_runs(manager: ScyllaClusterManager, object_storage, park):
    """The hold keeps every compaction of the target off, but it cannot stop auto-compaction being
    turned back on. The run checks what it holds after the load and again after the flush, and
    fails rather than reporting a recovery it cannot vouch for: a compaction between the backup's
    sstables and the replayed history purges a tombstone before the writes it covers. Each park
    reaches one of the two checks, and both fail with that code (#111)."""
    server = await one_server(manager, object_storage, shipping=True)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        pulse = await make_pulse(cql, ks)
        await wait_for_coverage(manager, server, object_storage, ks, 'target', snap_name, prefix, pulse, from_iso(T))
        await manager.api.enable_injection(server.ip_addr, park, one_shot=True)
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        tid = await start_replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        # Only once the run holds the target and is parked: enabling it earlier is refused by the
        # check that runs before the hold, which is another test's subject.
        await log.wait_for(f'{park}: waiting for message', from_mark=mark)
        await manager.api.enable_autocompaction(server.ip_addr, ks, 'target')
        await manager.api.message_injection(server.ip_addr, park)
        status = await manager.api.wait_task(server.ip_addr, tid)
        error = refusal_of(status)
        assert 'target_compaction_during_replay' in error, error

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
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        pulse = await make_pulse(cql, ks)
        await wait_for_coverage(manager, server, object_storage, ks, 'target', snap_name, prefix, pulse, from_iso(T))
        await manager.api.enable_injection(server.ip_addr, 'pitr_hold_compaction_fail', one_shot=True)
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        status = await replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        assert 'Injected compaction hold failure' in refusal_of(status), refusal_of(status)

        # The shards that did take a hold gave it back, which the compaction manager logs. A
        # compaction request would prove nothing: a major on a held view returns at once.
        released = await log.grep('Reenabling compaction for', from_mark=mark)
        assert released, 'no shard released its compaction hold after the failure'


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_aborting_a_replay_releases_the_hold_and_keeps_the_mark(manager: ScyllaClusterManager, object_storage):
    """A user can abort the task. The hold goes with the run, and the mark stays: the target has
    the backup's sstables in it, so it is incomplete and a second call is refused."""
    server = await one_server(manager, object_storage, shipping=True)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        snap_name, prefix = await backup_with_a_pitr_bound(manager, server, object_storage, ks, {'0': 'zero'})
        pulse = await make_pulse(cql, ks)
        await wait_for_coverage(manager, server, object_storage, ks, 'target', snap_name, prefix, pulse, from_iso(T))
        await manager.api.enable_injection(server.ip_addr, 'pitr_replay_loaded', one_shot=True)
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        tid = await start_replay(manager, server, object_storage, ks, 'target', snap_name, prefix)
        await log.wait_for(f"backup '{snap_name}' loaded", from_mark=mark)
        await manager.api.abort_task(server.ip_addr, tid)
        await manager.api.message_injection(server.ip_addr, 'pitr_replay_loaded')
        status = await manager.api.wait_task(server.ip_addr, tid)
        assert status['state'] in ('failed', 'aborted'), status

        assert await log.grep('Reenabling compaction for', from_mark=mark), 'the aborted run kept its compaction hold'
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
        pulse = await make_pulse(cql, ks)
        await wait_for_coverage(manager, server, object_storage, ks, 'target', snap_name, prefix, pulse, from_iso(T))
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        succeeded(await replay(manager, server, object_storage, ks, 'target', snap_name, prefix))
        assert await log.grep(f"restored from backup '{snap_name}' of {ks}.source", from_mark=mark), 'the result does not name the backup'

        loaded = {x.pk: x.value for x in await cql.run_async(f"SELECT * FROM {ks}.target;")}
        assert loaded == rows, f'the target does not hold the backup after the load: {loaded}'
