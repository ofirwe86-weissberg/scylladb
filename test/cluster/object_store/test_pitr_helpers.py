#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Checks of the test code in pitr_util.py that don't need PITR, so these run on any build.

Every "exact at T" test rests on the reference table, every store failure on GatedStore, and every wait on helpers
that have to fail when they should. wait_for() swallows exceptions, so a helper that asserts inside it can pass by
accident. Nothing here tests the product, apart from the CQL and backup behaviour the PITR tests rely on.
"""

import asyncio
import logging
import time
import uuid

from types import SimpleNamespace

import boto3
import botocore.config
import botocore.exceptions
import pytest

from test.cluster.object_store import pitr_util as pitr
from test.cluster.object_store.pitr_util import GatedStore, W, gated_store  # noqa: F401 (gated_store is a fixture)
from test.cluster.object_store.test_backup import do_backup, take_snapshot_on_one_server
from test.cluster.util import new_test_keyspace
from test.pylib.rest_client import HTTPError
from test.pylib.s3mock_server import create_bucket
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name, wait_for

logger = logging.getLogger(__name__)


class EchoServer:
    """Stands in for the store: it counts what arrives, and sends it back."""
    def __init__(self):
        self.received = 0
        self.server = None
        self.port = None

    async def _serve(self, reader, writer):
        try:
            while data := await reader.read(65536):
                self.received += len(data)
                writer.write(data)
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()

    async def __aenter__(self):
        self.server = await asyncio.start_server(self._serve, '127.0.0.1', 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *args):
        self.server.close()


def gate_for(echo) -> GatedStore:
    gate = GatedStore(SimpleNamespace(ip='127.0.0.1', port=echo.port, region='us-east-1'))
    gate.start()
    return gate


async def is_cut(reader) -> bool:
    """True if the connection ended. A timeout means traffic could still flow, and fails the caller's check."""
    try:
        return await asyncio.wait_for(reader.read(1), 10) == b''
    except ConnectionError:
        return True
    except asyncio.TimeoutError:
        return False


async def test_gate_cuts_open_and_new_connections_and_recovers():
    async with EchoServer() as echo:
        gate = gate_for(echo)
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        writer.write(b'hello')
        assert await asyncio.wait_for(reader.readexactly(5), 10) == b'hello', 'the gate must carry bytes unchanged'

        gate.fail()
        assert await is_cut(reader), 'fail() must cut an open connection'
        new_reader, new_writer = await asyncio.open_connection('127.0.0.1', gate.port)
        new_writer.write(b'lost')
        assert await is_cut(new_reader), 'a connection made while down must be cut'
        assert echo.received == 5, 'nothing may reach the store while it is down'

        gate.recover()
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        writer.write(b'again')
        assert await asyncio.wait_for(reader.readexactly(5), 10) == b'again', 'recover() must let traffic through'
        writer.close()
        gate.stop()


async def test_gate_cuts_a_connection_opening_across_the_failure(monkeypatch):
    async with EchoServer() as echo:
        gate = gate_for(echo)
        real_open = asyncio.open_connection

        async def fail_while_opening(host, port, **kwargs):
            opened = await real_open(host, port, **kwargs)
            if port == echo.port:
                gate.fail()
                await asyncio.sleep(0.01)   # so that the cut has finished before the connection is done opening
            return opened
        monkeypatch.setattr(asyncio, 'open_connection', fail_while_opening)

        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        writer.write(b'lost')
        assert await is_cut(reader), 'a connection opening across fail() must not survive it'
        assert gate.down
        assert echo.received == 0
        gate.stop()


async def test_gate_throttle_slows_traffic():
    async with EchoServer() as echo:
        gate = gate_for(echo)
        gate.throttle(128 * 1024)
        size = 64 * 1024
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        started = time.time()
        writer.write(b'x' * size)
        await asyncio.wait_for(reader.readexactly(size), 30)
        assert time.time() - started >= size / (128 * 1024), 'throttle() must hold traffic back'
        writer.close()
        gate.stop()


def _resource(storage, endpoint, secret):
    return boto3.resource('s3', endpoint_url=endpoint, aws_access_key_id=storage.acc_key, aws_secret_access_key=secret, verify=False,
                          config=botocore.config.Config(signature_version='s3v4', retries={'max_attempts': 1}, connect_timeout=5, read_timeout=5))


async def test_gate_keeps_s3_signatures_valid(gated_store):
    def put(endpoint, secret, key):
        _resource(gated_store, endpoint, secret).Object(gated_store.bucket_name, key).put(Body=b'signed')

    await asyncio.to_thread(put, gated_store.address, gated_store.secret_key, 'through-the-gate')
    assert await pitr.get_object(gated_store, 'through-the-gate') == b'signed'

    # The store checks signatures, or the line above proves nothing.
    with pytest.raises(botocore.exceptions.ClientError):
        await asyncio.to_thread(put, gated_store.address, 'a-wrong-secret', 'wrong-secret')

    gated_store.fail()
    with pytest.raises(botocore.exceptions.BotoCoreError):
        await asyncio.to_thread(put, gated_store.address, gated_store.secret_key, 'while-down')
    assert 'while-down' not in await pitr.objects_of(gated_store, '')

    gated_store.recover()
    await asyncio.to_thread(put, gated_store.address, gated_store.secret_key, 'after-recovery')
    assert await pitr.get_object(gated_store, 'after-recovery') == b'signed'


async def test_backup_and_restore_through_the_gate(manager: ScyllaClusterManager, gated_store):
    """An existing backup and restore through the gate, and the cut T23 relies on: a slowed upload, cut when its first object
    appears, ends failed and partial. Finishing it afterwards works."""
    server = await manager.server_add(config=pitr.node_config(gated_store, shipping=False))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t {pitr.TABLE_SCHEMA} WITH compression = {{'sstable_compression': ''}}")
        for first in range(0, 1200, 300):
            await pitr.apply(cql, ks, 't', pitr.simple_rows(range(first, first + 300), pitr.now_us() - 3600_000_000, 'x' * 1024))
            await manager.api.keyspace_flush(server.ip_addr, ks, 't')
        rows = await pitr.read_all(cql, ks, 't')
        size = await manager.api.get_table_disk_space_used(server.ip_addr, ks, 't')
        assert size > 1024 * 1024

        snap, tocs = await take_snapshot_on_one_server(ks, server, manager, logger)
        cut_prefix = f'cut/{snap}'
        gated_store.throttle(pitr.THROTTLE_BYTES_PER_SECOND)
        tid = await manager.api.backup(server.ip_addr, ks, 't', snap, gated_store.address, gated_store.bucket_name, cut_prefix)
        async def some_uploaded():
            return True if await pitr.objects_of(gated_store, f'{cut_prefix}/') else None
        await wait_for(some_uploaded, pitr.deadline(), period=0.05, backoff_factor=1)
        gated_store.fail()
        try:
            await manager.api.abort_task(server.ip_addr, tid)
        except HTTPError as e:
            logger.info(f'The task had already ended: {e}')
        assert (await manager.api.wait_task(server.ip_addr, tid))['state'] == 'failed'
        uploaded = sum(o.size for o in (await pitr.objects_of(gated_store, f'{cut_prefix}/')).values())
        assert 0 < uploaded < size, f'the upload must be partial: {uploaded} of {size} bytes'
        gated_store.recover()

        prefix = f'whole/{snap}'
        await do_backup(server, snap, prefix, ks, 't', gated_store, manager, logger)
        assert gated_store.connections > 0, 'the node must have gone through the gate'
        await cql.run_async(f'TRUNCATE TABLE {ks}.t')
        assert await pitr.read_all(cql, ks, 't') == []
        tid = await manager.api.restore(server.ip_addr, ks, 't', gated_store.address, gated_store.bucket_name, prefix, tocs)
        assert (await manager.api.wait_task(server.ip_addr, tid))['state'] == 'done'
        assert await pitr.read_all(cql, ks, 't') == rows


# The reference table. These answers are worked out by hand from change_statements(), with timestamps 1000 to 13000 in the
# order of its stages, and seed_statements() at 500. Columns: pk, ck, v, w, s, m, st, then WRITETIME of v, w, s, m[1], m[2], st[1], st[2].

def row(pk, ck, v, w, s, m, st, wt_v, wt_w, wt_s, wt_m1=None, wt_m2=None, wt_st1=None, wt_st2=None):
    return repr((pk, ck, v, w, s, m, st, wt_v, wt_w, wt_s, wt_m1, wt_m2, wt_st1, wt_st2))


AT_SEED = sorted([
    row(1, 0, 'seed', 0, 1, [(1, 'a'), (2, 'b')], [1], 500, 500, 500, 500, 500, 500),
    row(1, 2, 'seed', 2, 1, None, None, 500, 500, 500),
    row(1, 10, 'seed', 10, 1, None, None, 500, 500, 500),
    row(1, 11, 'seed', 11, 1, None, None, 500, 500, 500),
    row(1, 25, 'seed', 25, 1, None, None, 500, 500, 500),
    row(101, 0, 'seed', None, None, None, None, 500, None, None),
])
# After the insert, update, row, partition and range deletes, and the map element.
AT_6000 = sorted([
    row(1, 0, 'updated', 0, 1, [(1, 'changed'), (2, 'b')], [1], 2000, 500, 500, 6000, 500, 500),
    row(1, 1, 'new', 1, 1, None, None, 1000, 1000, 500),
    row(1, 25, 'seed', 25, 1, None, None, 500, 500, 500),
])
# The map overwrite took m[1] away, the set gained 2, the static changed, and only the low half of the batch is in.
AT_10000 = sorted([
    row(1, 0, 'updated', 0, 2, [(2, 'over')], [1, 2], 2000, 500, 9000, None, 7000, 500, 8000),
    row(1, 1, 'new', 1, 2, None, None, 1000, 1000, 9000),
    row(1, 20, 'low', None, 2, None, None, 10000, None, 9000),
    row(1, 25, 'seed', 25, 2, None, None, 500, 500, 9000),
])
# Both halves of the batch. The delete wins the tie, so ck 30 isn't there, and w of ck 25 has changed.
AT_END = sorted([
    row(1, 0, 'updated', 0, 2, [(2, 'over')], [1, 2], 2000, 500, 9000, None, 7000, 500, 8000),
    row(1, 1, 'new', 1, 2, None, None, 1000, 1000, 9000),
    row(1, 20, 'low', None, 2, None, None, 10000, None, 9000),
    row(1, 21, 'high', None, 2, None, None, 11000, None, 9000),
    row(1, 25, 'seed', 99, 2, None, None, 500, 13000, 9000),
])


async def test_reference_table_gives_the_answers_worked_out_by_hand(manager: ScyllaClusterManager):
    await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        seed = pitr.seed_statements([1], 500)
        changes, ts = pitr.change_statements([1], 0)
        assert ts['insert'] == 1000 and ts['map_element'] == 6000 and ts['batch_low'] == 10000 and ts['tie'] == 12000 and ts['last'] == 13000
        history = seed + changes
        for t, expected in ((500, AT_SEED), (999, AT_SEED), (6000, AT_6000), (6999, AT_6000), (10000, AT_10000), (10999, AT_10000), (13000, AT_END)):
            assert await pitr.expected_at(cql, ks, history, t) == expected, f'at {t}'

        # A tie at T: the insert and the delete are both in, and the delete wins.
        at_tie = await pitr.expected_at(cql, ks, history, 12000)
        assert not [r for r in at_tie if r.startswith('(1, 30,')]

        # The source gets real batches, and the reference their parts. They must agree.
        await cql.run_async(f'CREATE TABLE {ks}.src {pitr.TABLE_SCHEMA}')
        await pitr.apply(cql, ks, 'src', history)
        assert await pitr.read_exact(cql, ks, 'src') == AT_END

        # assert_exact() has to be able to fail.
        assert await pitr.assert_exact(cql, ks, 'src', history, 13000) == AT_END
        with pytest.raises(AssertionError):
            await pitr.assert_exact(cql, ks, 'src', history, 10000)
        await cql.run_async(f"UPDATE {ks}.src USING TIMESTAMP 13000 SET v = 'changed' WHERE pk = 1 AND ck = 25")
        with pytest.raises(AssertionError):
            await pitr.assert_exact(cql, ks, 'src', history, 13000)


async def test_reference_table_merges_batches_by_hand(manager: ScyllaClusterManager):
    """T34's statements, with timestamps 1000 for the overwrite, 2000 for the empty overwrite, 3000 and 3001 for the delete and its batched
    element, and 4000 and 5000 for the batch that overwrites itself, after a seed at 500."""
    await manager.server_add()
    cql = manager.get_cql()

    def edge(ck, v, m, wt_v, wt_m1, wt_m2):
        return row(1, ck, v, None, None, m, None, wt_v, None, None, wt_m1, wt_m2)
    seeded = [(1, 'a'), (2, 'b')]
    # Every later row is the same at each of these except as noted.
    overwritten = edge(0, 'seed', [(2, 'over')], 500, None, 1000)
    emptied = edge(1, 'seed', None, 500, None, None)
    deleted = edge(2, 'seed', None, 500, None, None)
    added = edge(2, 'seed', [(2, 'added')], 500, None, 3001)
    cell_before, cell_after = edge(3, 'seed', seeded, 500, 500, 500), edge(3, 'kept', seeded, 5000, 500, 500)
    element_before, element_after = edge(4, 'seed', seeded, 500, 500, 500), edge(4, 'seed', [(1, 'kept'), (2, 'b')], 500, 5000, 500)
    at_seed = [edge(ck, 'seed', seeded, 500, 500, 500) for ck in range(5)]
    answers = {999: at_seed,
               1000: [overwritten, *at_seed[1:]],
               1999: [overwritten, *at_seed[1:]],
               2000: [overwritten, emptied, *at_seed[2:]],
               2999: [overwritten, emptied, *at_seed[2:]],
               3000: [overwritten, emptied, deleted, cell_before, element_before],
               3001: [overwritten, emptied, added, cell_before, element_before],
               4000: [overwritten, emptied, added, cell_before, element_before],
               4999: [overwritten, emptied, added, cell_before, element_before],
               5000: [overwritten, emptied, added, cell_after, element_after]}
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        seed, changes, ts = pitr.collection_edge_statements([1], 500, 0)
        assert ts == {'overwrite': 1000, 'empty': 2000, 'delete': 3000, 'added': 3001, 'low': 4000, 'high': 5000}
        history = seed + changes
        for t, expected in answers.items():
            assert await pitr.expected_at(cql, ks, history, t) == sorted(expected), f'at {t}'

        # The real batches give the merged result, which is what the hand answers above assume.
        await cql.run_async(f'CREATE TABLE {ks}.src {pitr.TABLE_SCHEMA}')
        await pitr.apply(cql, ks, 'src', history)
        assert await pitr.read_exact(cql, ks, 'src') == sorted(answers[5000])


async def test_scenario_statements_are_valid_and_reach_every_shard(manager: ScyllaClusterManager):
    """Every statement the scenarios write, on the keys they use, and the shard arithmetic against the real tool."""
    server = await manager.server_add()
    cql = manager.get_cql()
    pks = list(range(8))
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {pitr.TABLE_SCHEMA}')
        history = pitr.seed_statements(pks, 500) + pitr.change_statements(pks, 0)[0]
        await pitr.apply(cql, ks, 't', history)
        assert await pitr.read_exact(cql, ks, 't') == await pitr.expected_at(cql, ks, history, 13000)
        await pitr.assert_every_shard(cql, ks, 't', pks)
        with pytest.raises(AssertionError):
            await pitr.assert_every_shard(cql, ks, 't', [pks[0]])

        tokens = {r[0]: r[1] for r in await cql.run_async(f'SELECT pk, token(pk) FROM {ks}.t') if r[0] in pks}
        exe = await manager.server_get_exe(server.server_id)
        for pk in pks:
            # A partition key in the form of the tool: int32, big-endian, length-prefixed.
            key = (4).to_bytes(2, 'big').hex() + pk.to_bytes(4, 'big').hex()
            proc = await asyncio.create_subprocess_exec(exe, 'types', 'shardof', '--full-compound', '-t', 'Int32Type', f'--shards={pitr.SHARDS}', key,
                                                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out, err = await proc.communicate()
            assert proc.returncode == 0, err
            assert f'token: {tokens[pk]}' in out.decode() and out.decode().rstrip().endswith(f'shard: {pitr.shard_of(tokens[pk])}'), f'{pk}: {out} {err}'

        # What T06 reads back with its own query.
        await cql.run_async(f"CREATE TABLE {ks}.writer {pitr.TABLE_SCHEMA}")
        writer = pitr.Writer(cql, ks, 'writer').start()
        await writer.wait_acked(3)
        await writer.stop()
        await pitr.assert_acked_writes_present(cql, ks, 'writer', [writer], pitr.now_us() + 2 * pitr.CLOCK_MARGIN_US, [])


async def test_scenario_schemas_are_valid(manager: ScyllaClusterManager, s3_storage):
    """Every table and schema change the scenarios make, on the node configurations they use."""
    server = await manager.server_add(config=pitr.unsupported_tables_config(pitr.node_config(s3_storage, shipping=False)))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks, \
               new_test_keyspace(manager, pitr.TABLETS_KS) as tablet_ks, \
               new_test_keyspace(manager, pitr.STRONG_KS) as strong_ks, \
               new_test_keyspace(manager, pitr.NONDURABLE_KS) as nondurable_ks, \
               new_test_keyspace(manager, pitr.RF2_KS) as rf2_ks:
        cases = await pitr.create_unsupported_tables(server, cql, ks, tablet_ks, strong_ks, nondurable_ks)
        for k, cf, _ in cases:
            # A strongly consistent table can't be scanned, as assert_refused() allows for.
            if k != strong_ks:
                assert await cql.run_async(f'SELECT * FROM {k}.{cf} LIMIT 1') is not None

        # T19
        await cql.run_async(f'CREATE TABLE {rf2_ks}.t {pitr.TABLE_SCHEMA}')
        assert await pitr.read_all(cql, rf2_ks, 't') == []
        await cql.run_async(f'CREATE TABLE {ks}.altered {pitr.TABLE_SCHEMA}')
        await cql.run_async(pitr.ALTER_KEYSPACE_RF2.format(ks=ks))
        await cql.run_async(pitr.ALTER_KEYSPACE_RF1_NONDURABLE.format(ks=ks))


async def test_scenario_schema_changes_are_valid(manager: ScyllaClusterManager):
    await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        describe, rows = await pitr.create_rich_table(cql, ks)    # T15
        assert 'gone' not in describe and len(rows) == 1
        await cql.run_async(f'ALTER TABLE {ks}.src ADD later int')
        assert (await cql.run_async(f'DESCRIBE TABLE {ks}.src'))[0].create_statement != describe

        await pitr.create_alterable_table(cql, ks)      # T24
        for change in pitr.SCHEMA_CHANGES:
            before = (await cql.run_async(f'DESC KEYSPACE {ks}'))
            await cql.run_async(pitr.schema_change_statement(change, ks))
            assert [r.create_statement for r in await cql.run_async(f'DESC KEYSPACE {ks}')] != [r.create_statement for r in before], change

        # T33
        await cql.run_async(f'CREATE TYPE {ks}.unused (zip int)')
        await cql.run_async(f'ALTER TYPE {ks}.unused ADD extra int')


async def test_conditional_history_reads_back_real_timestamps(manager: ScyllaClusterManager):
    """T29: conditional writes, one not applied, against the timestamps the source reports."""
    await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {pitr.TABLE_SCHEMA}')
        history = await pitr.conditional_history(cql, ks)
        assert await pitr.read_exact(cql, ks, 't') == await pitr.expected_at(cql, ks, history, max(w.ts for w in history))
        first, second, _ = history
        for t, value in ((first.ts, 'first'), (second.ts - 1, 'first'), (second.ts, 'second')):
            at_t = await pitr.expected_at(cql, ks, history, t)
            assert [row for row in at_t if row.startswith('(1, 0,')] == [repr((1, 0, value, None, None, None, None,
                                                                              first.ts if value == 'first' else second.ts,
                                                                              None, None, None, None, None, None))], f'at {t}'


async def test_tombstone_is_purged_before_a_backup(manager: ScyllaClusterManager):
    """T13's precondition: the row and its tombstone are gone from the data, whichever second the delete fell in."""
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.purged {pitr.TABLE_SCHEMA} {pitr.PURGEABLE_OPTIONS}')
        await pitr.write_and_purge(manager, server, cql, ks)
        assert await pitr.pks_of(cql, ks, 'purged') == {2}


async def test_snapshot_backup_and_tampering(manager: ScyllaClusterManager, s3_storage):
    """take_snapshot_on_one_server and do_backup on several tables, one empty. Then the bucket helpers T20 uses, and the
    backup routes T30 uses, with real nodetool."""
    server = await manager.server_add(config=pitr.node_config(s3_storage, shipping=False))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        for cf in ('empty', 'populated'):
            await cql.run_async(f'CREATE TABLE {ks}.{cf} {pitr.TABLE_SCHEMA}')
        await pitr.apply(cql, ks, 'populated', pitr.simple_rows(range(4), pitr.now_us()))
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        for cf in ('empty', 'populated'):
            await do_backup(server, snap, pitr.backup_prefix(ks, cf, snap), ks, cf, s3_storage, manager, logger)
        objects = await pitr.objects_of(s3_storage, pitr.backup_prefix(ks, 'populated', snap))
        assert objects

        key = sorted(k for k, o in objects.items() if o.size >= 2)[0]
        original = await pitr.get_object(s3_storage, key)
        assert objects[key].last_modified.tzinfo is not None
        await pitr.put_object(s3_storage, key, original[:len(original) // 2])
        assert (await pitr.objects_of(s3_storage, key))[key].size == len(original) // 2
        await pitr.delete_object(s3_storage, key)
        assert key not in await pitr.objects_of(s3_storage, key)
        await pitr.put_object(s3_storage, key, original)
        assert await pitr.get_object(s3_storage, key) == original
        assert pitr.is_immutable(key) and not pitr.is_immutable(f'{pitr.SHIPPING_PREFIX}/head')

        # T30
        skip_flush = unique_name('skipflush_')
        await pitr.run_nodetool(manager, server, ['nodetool', 'snapshot', '--skip-flush', '--tag', skip_flush, ks])
        await do_backup(server, skip_flush, f'elsewhere/{skip_flush}', ks, 'populated', s3_storage, manager, logger)
        bucket = f'pitr-other-{uuid.uuid4().hex[:12]}'
        await asyncio.to_thread(create_bucket, s3_storage.get_resource(), bucket)
        tid = await manager.api.backup(server.ip_addr, ks, 'populated', snap, s3_storage.address, bucket, 'other')
        assert (await manager.api.wait_task(server.ip_addr, tid))['state'] == 'done'
        other = s3_storage.get_resource().Bucket(bucket)
        assert list(other.objects.all())
        await asyncio.to_thread(lambda: (other.objects.all().delete(), other.delete()))

        # What the exact commands in status would be, and that a wrong one is noticed.
        tag = unique_name('tag_')
        await pitr.run_status_commands(manager, server, [f'nodetool snapshot --tag {tag} {ks}',
                                                         f'nodetool backup --keyspace {ks} --table populated --snapshot {tag} --endpoint {s3_storage.address} '
                                                         f'--bucket {s3_storage.bucket_name} --prefix {pitr.backup_prefix(ks, "populated", tag)}'])
        assert await pitr.objects_of(s3_storage, pitr.backup_prefix(ks, 'populated', tag))
        for bad in ([], ['nodetool snapshot --tag <tag>'], ['scylla snapshot'], ['nodetool no-such-command']):
            with pytest.raises(AssertionError):
                await pitr.run_status_commands(manager, server, bad)


@pytest.mark.parametrize('kill', [False, True], ids=['clean_restart', 'kill'])
async def test_restart_ends_a_writer_and_the_snapshot_survives(manager: ScyllaClusterManager, s3_storage, kill):
    """T06 and T21 stop a node under a Writer, then go on. Neither may hang, and a snapshot from before must still upload."""
    server = await manager.server_add(config=pitr.node_config(s3_storage, shipping=False))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {pitr.TABLE_SCHEMA}')
        await pitr.apply(cql, ks, 't', pitr.simple_rows(range(4), pitr.now_us()))
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        writer = pitr.Writer(cql, ks, 't', first_pk=1000).start()
        await writer.wait_acked(20)
        stopping = asyncio.create_task(pitr.restart(manager, server, kill))
        cql = await stopping
        writer.stopping = True
        await asyncio.wait_for(writer.task, 120)
        assert len(writer.acked) >= 20

        assert {0, 1, 2, 3} <= await pitr.pks_of(cql, ks, 't')
        await do_backup(server, snap, pitr.backup_prefix(ks, 't', snap), ks, 't', s3_storage, manager, logger)
        assert await pitr.objects_of(s3_storage, pitr.backup_prefix(ks, 't', snap))


# Helpers that must fail when they should. These feed them what the feature would report.

def entry(**kwargs):
    fields = dict(keyspace='ks', table='t', state=pitr.PROTECTED, reason=None, commands=[], restart_required=False, break_cause=None, break_time_us=None,
                  backups=[], latest_us=None, lag_seconds=None, step=None, text='')
    return pitr.TableStatus(**(fields | kwargs))


async def test_polling_helpers_time_out_and_succeed(monkeypatch):
    monkeypatch.setattr(pitr, 'deadline', lambda: time.time() + 1)
    current = entry(latest_us=100)

    async def fake_status(*args):
        return current
    monkeypatch.setattr(pitr, 'table_status', fake_status)

    assert await pitr.wait_covered(None, None, 'ks', 't', 100) == 100
    with pytest.raises(AssertionError):
        await pitr.wait_covered(None, None, 'ks', 't', 101)
    with pytest.raises(AssertionError):
        await pitr.wait_covered(None, None, 'ks', 't')

    backup = pitr.Backup('b', 1, 2)
    current = entry(backups=[backup])
    assert await pitr.wait_protected(None, None, 'ks', 't', 'b') == backup
    assert await pitr.wait_protected(None, None, 'ks', 't') == backup
    with pytest.raises(AssertionError):
        await pitr.wait_protected(None, None, 'ks', 't', 'other')
    current = entry(state=pitr.NOT_PROTECTED, backups=[backup])
    with pytest.raises(AssertionError):
        await pitr.wait_protected(None, None, 'ks', 't', 'b')


async def test_absence_helpers_fail_when_something_appears(monkeypatch, s3_storage):
    started = time.time()
    await pitr.assert_nothing_appears(s3_storage)
    assert time.time() - started >= pitr.ABSENCE_WINDOW_SECONDS, 'the window must really be watched'

    async def put_late():
        await asyncio.sleep(1)
        await pitr.put_object(s3_storage, f'{pitr.SHIPPING_PREFIX}/late', b'x')
    late = asyncio.create_task(put_late())
    started = time.time()
    with pytest.raises(AssertionError):
        await pitr.assert_nothing_appears(s3_storage)
    assert time.time() - started < pitr.ABSENCE_WINDOW_SECONDS, 'it must fail when the object appears'
    await late
    await pitr.assert_nothing_appears(s3_storage, ('sstables/', pitr.SHIPPING_PREFIX))

    calls = 0

    async def turns_protected(*args):
        nonlocal calls
        calls += 1
        return entry(state=pitr.NOT_PROTECTED) if calls < 3 else entry(state=pitr.PROTECTED)
    monkeypatch.setattr(pitr, 'table_status', turns_protected)
    with pytest.raises(AssertionError):
        await pitr.assert_stays_unprotected(None, None, 'ks', 't')

    async def stays_not_protected(*args):
        return entry(state=pitr.NOT_PROTECTED)
    monkeypatch.setattr(pitr, 'table_status', stays_not_protected)
    assert (await pitr.assert_stays_unprotected(None, None, 'ks', 't')).state == pitr.NOT_PROTECTED


async def test_row_helpers_fail_when_they_should(manager: ScyllaClusterManager):
    """wait_acked() ends as soon as a write fails. assert_acked_writes_present() notices a missing and an unexpected row."""
    await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        failing = pitr.Writer(cql, ks, 'no_such_table').start()
        started = time.time()
        with pytest.raises(AssertionError, match='a write failed'):
            await failing.wait_acked(1)
        assert time.time() - started < pitr.DEADLINE_SECONDS / 2, 'wait_for() must not sit on the failure until its deadline'

        await cql.run_async(f'CREATE TABLE {ks}.t {pitr.TABLE_SCHEMA}')
        writer = pitr.Writer(cql, ks, 't').start()
        await writer.wait_acked(5)
        await writer.stop()
        t = pitr.now_us() + 2 * pitr.CLOCK_MARGIN_US
        assert len(await pitr.assert_acked_writes_present(cql, ks, 't', [writer], t, [])) == len(writer.acked)

        # A written row too new for T, or one nobody wrote.
        with pytest.raises(AssertionError, match='unexpected row'):
            await pitr.assert_acked_writes_present(cql, ks, 't', [writer], min(ts for ts, _ in writer.acked.values()) - 1, [])
        await pitr.apply(cql, ks, 't', pitr.simple_rows([7], 1))
        with pytest.raises(AssertionError, match='unexpected row'):
            await pitr.assert_acked_writes_present(cql, ks, 't', [writer], t, [])
        assert await pitr.assert_acked_writes_present(cql, ks, 't', [writer], t, [7])

        # A row that went missing.
        await cql.run_async(f'DELETE FROM {ks}.t WHERE pk = {min(writer.acked)}')
        with pytest.raises(AssertionError, match='missing'):
            await pitr.assert_acked_writes_present(cql, ks, 't', [writer], t, [7])
        with pytest.raises(AssertionError, match='rows missing'):
            await pitr.assert_acked_writes_present(cql, ks, 't', [writer], t, [12345])


async def test_assert_refused_catches_each_rule_of_a_refusal(monkeypatch, manager: ScyllaClusterManager):
    await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {pitr.TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {ks}.watched {pitr.TABLE_SCHEMA}')
        await pitr.apply(cql, ks, 't', pitr.simple_rows([1], 1))
        refusal = pitr.Result(state=pitr.FAILED, reason='code', message='why')
        action = None

        async def fake_restore(manager, server, ks, cf, backup, t, new_table, new_keyspace=None):
            if action:
                await cql.run_async(action.format(new=new_table))
            return fake_restore.result
        fake_restore.result = refusal
        monkeypatch.setattr(pitr, 'run_restore', fake_restore)

        async def refused(reason='code'):
            return await pitr.assert_refused(manager, None, cql, ks, 't', 'b', 1, reason, ['watched'])

        assert await refused() == refusal
        assert await refused({'code', 'other'}) == refusal
        with pytest.raises(AssertionError, match='expected reason'):
            await refused('other')
        for bad, words in ((pitr.Result(state=pitr.DONE), 'must be refused'),
                           (pitr.Result(state=pitr.FAILED, reason='code'), 'states the reason'),
                           (pitr.Result(state=pitr.FAILED, reason='code', message='why', cells_applied=1), 'before any data')):
            fake_restore.result = bad
            with pytest.raises(AssertionError, match=words):
                await refused()
        fake_restore.result = refusal

        action = f'INSERT INTO {ks}.watched (pk, ck, v) VALUES (5, 0, \'x\')'
        with pytest.raises(AssertionError, match='changed'):
            await refused()
        await cql.run_async(f'TRUNCATE TABLE {ks}.watched')
        action = f'INSERT INTO {ks}.t (pk, ck, v) VALUES (6, 0, \'x\')'
        with pytest.raises(AssertionError, match='changed'):
            await refused()
        action = f'CREATE TABLE {ks}.{{new}} (pk int PRIMARY KEY)'
        with pytest.raises(AssertionError, match='no new table'):
            await refused()


async def test_adapter_parses_what_it_proposes():
    """The adapter's parsing against the surface it proposes, so a typo in it fails here rather than as a puzzling result later."""
    t_text, later = '2026-11-04T14:05:00.000001Z', '2026-11-04T14:05:01Z'
    t_us = pitr.from_iso(t_text)
    assert pitr.iso(t_us) == t_text and pitr.from_iso(later) == t_us + 999_999
    assert pitr.iso_times(f'from {t_text} to {later}, not 2026-11-04') == [t_us, t_us + 999_999]

    status = pitr._parse_status({'shipping': 'on', 'store': {'reachable': True, 'error': ''}, 'limits': ['one', 'two'], 'tables': [
        {'keyspace': 'ks', 'table': 't', 'state': pitr.NOT_PROTECTED, 'reason': pitr.Reason.BROKEN, 'backup_commands': ['nodetool x'], 'restart_required': True,
         'break': {'cause': pitr.CAUSE_OUTAGE, 'time': t_text}, 'backups': [{'name': 'b', 'start': t_text, 'end': later}],
         'latest_restorable': later, 'lag_seconds': 1.5, 'step': 'load'}]})
    assert status.shipping_on and status.store_reachable and status.limits_text == 'one two'
    parsed = pitr.table_entry(status, 'ks', 't')
    assert parsed.break_time_us == t_us and parsed.backups == [pitr.Backup('b', t_us, t_us + 999_999)] and parsed.latest_us == t_us + 999_999
    assert parsed.restart_required and parsed.commands == ['nodetool x'] and parsed.lag_seconds == 1.5 and parsed.step == 'load'
    with pytest.raises(AssertionError):
        pitr.table_entry(status, 'ks', 'other')
    two = pitr.Status(True, '', True, '', '', [parsed, parsed])
    with pytest.raises(AssertionError, match='one answer'):
        pitr.table_entry(two, 'ks', 't')

    result = pitr._parse_result({'state': pitr.FAILED, 'reason': 'r', 'message': 'm', 'notices': ['a', 'b'], 'backup_commands': ['c'], 't': t_text,
                                 'backup': 'b',
                                 'objects_read': 1, 'bytes_read': 2, 'cells_applied': 3, 'cells_dropped_after_t': 4, 'flags': ['f'],
                                 'steps': {'check': {'seconds': 1.0, 'bytes': 5}}, 'total_seconds': 1.0, 'other_tables_bytes': 6,
                                 'needed_bytes': 7, 'free_bytes': 8, 'reserve_bytes': 9, 'remaining_interval': {'start': t_text, 'end': later}})
    assert (result.reason, result.message, result.notices, result.t_us, result.cells_dropped) == ('r', 'm', 'a\nb', t_us, 4)
    assert result.steps == {'check': pitr.Step(1.0, 5)} and (result.remaining_start_us, result.remaining_end_us) == (t_us, t_us + 999_999)
    assert pitr._parse_result({'state': pitr.FAILED}).remaining_start_us is None
