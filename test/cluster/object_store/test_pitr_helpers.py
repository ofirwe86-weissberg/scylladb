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
from test.cluster.object_store.pitr_util import GatedStore, gated_store  # noqa: F401 (gated_store is a fixture)
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


async def test_gate_cuts_after_a_number_of_bytes():
    async with EchoServer() as echo:
        gate = gate_for(echo)
        gate.fail_after_bytes(10)
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        writer.write(b'12345')
        assert await asyncio.wait_for(reader.readexactly(5), 10) == b'12345'
        assert not gate.down
        writer.write(b'1234567890')
        assert await is_cut(reader), 'the gate must cut once the bytes are used up'
        assert gate.down and echo.received == 5
        gate.stop()


async def test_gate_holds_traffic_until_it_recovers():
    async with EchoServer() as echo:
        gate = gate_for(echo)
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        gate.hold()
        writer.write(b'held')
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(reader.readexactly(4), 0.5)
        assert echo.received == 0, 'nothing may reach the store while it is held'
        gate.recover()
        assert await asyncio.wait_for(reader.readexactly(4), 10) == b'held'
        gate.stop()


async def test_gate_holds_only_the_requests_its_rule_picks():
    """hold_when() holds the connection of a request its rule picks, and nothing else: not other connections, and not bytes in a
    body that look like a request. T3 and T10 hold the first read of the history with it, and T4 every shipping upload."""
    starts = pitr.RequestStarts()
    assert starts.feed(b'GET /b/k HTTP/1.1\r\nHo') == [], 'a request counts once its head is whole'
    assert starts.feed(b'st: h\r\nContent-Length: 4\r\n\r\nGET ') == [('GET', '/b/k')]
    assert starts.feed(b'PUT /b/c HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n') == [('PUT', '/b/c')]
    assert starts.feed(b'5\r\nGET /\r\n0\r\n\r\n') == [], 'a chunked body is skipped'
    assert starts.feed(b'HEAD /b/n HTTP/1.1\r\n\r\n') == [('HEAD', '/b/n')]

    async with EchoServer() as echo:
        gate = gate_for(echo)
        gate.hold_when(lambda method, path: path.startswith('/b/pitr-log/'))
        body = b'GET /b/pitr-log/in-a-body HTTP/1.1\r\n\r\n'
        other_request = b'PUT /b/backups/x HTTP/1.1\r\nContent-Length: %d\r\n\r\n%s' % (len(body), body)
        other_reader, other_writer = await asyncio.open_connection('127.0.0.1', gate.port)
        other_writer.write(other_request)
        assert await asyncio.wait_for(other_reader.readexactly(len(other_request)), 10) == other_request
        assert not gate.holding.is_set(), 'bytes in a body are not a request'

        picked = b'GET /b/pitr-log/seg HTTP/1.1\r\nHost: h\r\n\r\n'
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        writer.write(picked)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(reader.readexactly(len(picked)), 0.5)
        assert gate.holding.is_set()

        later = b'GET /b/backups/y HTTP/1.1\r\n\r\n'
        other_writer.write(later)
        assert await asyncio.wait_for(other_reader.readexactly(len(later)), 10) == later, 'another connection must go on'

        gate.release()
        assert await asyncio.wait_for(reader.readexactly(len(picked)), 10) == picked, 'release() must let the held request through'
        assert ('GET', '/b/pitr-log/seg') in gate.requests and ('GET', '/b/pitr-log/in-a-body') not in gate.requests
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
    """An existing backup and restore through the gate, and the cut T23 relies on: an upload cut after half its bytes ends failed once
    aborted. Finishing it afterwards works."""
    server = await manager.server_add(config=pitr.node_config(gated_store, shipping=False))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {pitr.UNCOMPRESSED_SCHEMA}')
        await pitr.write_several_sstables(manager, server, cql, ks)
        rows = await pitr.read_all(cql, ks, 't')
        size = await manager.api.get_table_disk_space_used(server.ip_addr, ks, 't')
        assert size > 1024 * 1024

        snap, tocs = await take_snapshot_on_one_server(ks, server, manager, logger)
        cut_prefix = f'cut/{snap}'
        gated_store.fail_after_bytes(size // 2)
        tid = await manager.api.backup(server.ip_addr, ks, 't', snap, gated_store.address, gated_store.bucket_name, cut_prefix)

        async def went_down():
            return True if gated_store.down else None
        await wait_for(went_down, pitr.deadline())
        try:
            await manager.api.abort_task(server.ip_addr, tid)
        except HTTPError as e:
            logger.info(f'The task had already ended: {e}')
        assert (await manager.api.wait_task(server.ip_addr, tid))['state'] == 'failed'
        uploaded = sum(o.size for o in (await pitr.objects_of(gated_store, f'{cut_prefix}/')).values())
        assert 0 < uploaded < size, f'the upload must be part-way: {uploaded} of {size} bytes'
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
        # keys_on_shard() places keys with the client's own token, so it must agree with the node's.
        assert {pk: pitr.token_of(pk) for pk in pks} == tokens
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


async def test_flush_keeps_a_row_and_its_tombstone_with_expired_grace(manager: ScyllaClusterManager):
    """The expired-grace case in test_pitr_deleted.py: gc_grace_seconds = 0 and no compaction leave the row and its tombstone in the data."""
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {pitr.TABLE_SCHEMA} WITH gc_grace_seconds = 0')
        await pitr.write_row_flush_then_delete_it(manager, server, cql, ks)
        await take_snapshot_on_one_server(ks, server, manager, logger)
        assert await pitr.row_is_in_the_data(cql, ks, 0), 'expected row 0 still in the data'
        assert not await pitr.row_is_in_the_data(cql, ks, 2), 'expected no row 2: the check must be able to say no'
        assert await pitr.pks_of(cql, ks, 't') == {1}


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




async def test_target_from_the_backup_schema_is_empty_and_uncompacted(manager: ScyllaClusterManager, s3_storage):
    """create_target() follows the README: the backup's schema.cql, renamed, with compaction off. Every replay starts from it,
    and the endpoint refuses a target whose schema differs from the backup's in more than name, ID and compaction."""
    server = await manager.server_add(config=pitr.node_config(s3_storage, shipping=False))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {pitr.TABLE_SCHEMA} {pitr.PURGEABLE_OPTIONS}')
        await pitr.apply(cql, ks, 't', pitr.simple_rows(range(4), pitr.old_ts()))
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        prefix = pitr.backup_prefix(ks, 't', snap)
        await do_backup(server, snap, prefix, ks, 't', s3_storage, manager, logger)

        await pitr.create_target(cql, s3_storage, ks, 'target', prefix)
        assert await pitr.read_all(cql, ks, 'target') == []

        def columns(table):
            return cql.run_async(f"SELECT column_name, type, kind FROM system_schema.columns WHERE keyspace_name = '{ks}' AND table_name = '{table}'")
        assert sorted(await columns('target')) == sorted(await columns('t'))
        options = (await cql.run_async(f"SELECT compaction, gc_grace_seconds, id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name IN ('t', 'target')"))
        source, target = sorted(options, key=lambda r: r.compaction.get('enabled', 'true') == 'false')
        assert target.compaction.get('enabled') == 'false' and source.compaction.get('enabled', 'true') != 'false'
        assert target.gc_grace_seconds == source.gc_grace_seconds == 0 and target.id != source.id


async def test_polling_helpers_time_out_and_succeed(monkeypatch):
    monkeypatch.setattr(pitr, 'deadline', lambda: time.time() + 1)
    p = pitr.Pitr(None, None, None, None, 'ks')
    current = 100

    async def covered_through(self):
        return current

    async def no_pulse(self):
        pass
    monkeypatch.setattr(pitr.Pitr, 'covered_through', covered_through)
    monkeypatch.setattr(pitr.Pitr, 'pulse', no_pulse)
    assert await p.covered(100) == 100
    with pytest.raises(AssertionError):
        await p.covered(101)
    with pytest.raises(AssertionError):
        await p.covered()
    current = None
    with pytest.raises(AssertionError):
        await p.covered(1)

    states = []

    async def task_status(ip, tid):
        return {'state': states.pop(0) if len(states) > 1 else states[0], 'error': ''}
    manager = SimpleNamespace(api=SimpleNamespace(get_task_status=task_status))
    server = SimpleNamespace(ip_addr='127.0.0.1')
    states[:] = ['created', 'running', 'done']
    assert (await pitr.wait_replay(manager, server, 'id')).state == pitr.DONE
    states[:] = ['running']
    with pytest.raises(AssertionError, match='replay task'):
        await pitr.wait_replay(manager, server, 'id')


async def test_coverage_probe_reads_a_refusal_and_reuses_its_target(monkeypatch):
    """Until status lands, covered_through() asks for a far-future T on one throwaway target, and reads the latest restorable time
    from the refusal. Anything but that refusal gives no answer, and a refusal saying the probe was used up fails the test."""
    async def no_status(*args):
        return None
    monkeypatch.setattr(pitr, 'get_status', no_status)
    targets, asked = [], []

    async def new_target(self):
        targets.append(f'probe{len(targets)}')
        return targets[-1]
    answer = {}

    async def replay(self, t, target=None, storage=None):
        asked.append(target)
        return answer['outcome'](t), target
    monkeypatch.setattr(pitr.Pitr, 'new_target', new_target)
    monkeypatch.setattr(pitr.Pitr, 'replay', replay)
    p = pitr.Pitr(None, None, None, None, 'ks')

    latest = pitr.from_iso('2026-11-04T14:05:00.000001Z')
    answer['outcome'] = lambda t: pitr.Outcome(pitr.FAILED, pitr.Reason.T_PAST_COVERAGE,
                                               f't_past_coverage: T {pitr.iso(t)} is past {pitr.iso(latest)}; lag 3 s', '')
    assert await p.covered_through() == latest
    assert await p.covered_through() == latest
    assert asked == ['probe0', 'probe0'], 'the probe target is made once and used again'

    answer['outcome'] = lambda t: pitr.Outcome(pitr.FAILED, pitr.Reason.RECORDS_MISSING, 'protection_records_missing: none yet', '')
    assert await p.covered_through() is None
    answer['outcome'] = lambda t: pitr.Outcome(pitr.FAILED, pitr.Reason.TARGET_ALREADY_REPLAYED, 'target_already_replayed: x', '')
    with pytest.raises(AssertionError, match='probe usable'):
        await p.covered_through()


async def test_assert_refused_catches_each_rule_of_a_refusal(monkeypatch, manager: ScyllaClusterManager):
    await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, pitr.VNODES_KS) as ks:
        for cf in ('t', 'watched', 'target'):
            await cql.run_async(f'CREATE TABLE {ks}.{cf} {pitr.TABLE_SCHEMA}')
        await pitr.apply(cql, ks, 't', pitr.simple_rows([1], 1))
        refusal = pitr.Outcome(state=pitr.FAILED, reason='code', message='code: why, and what to do', result_text='')
        action = None
        status = None

        async def fake_replay(self, t, target=None, storage=None):
            if action:
                await cql.run_async(action)
            return fake_replay.outcome, 'target'
        fake_replay.outcome = refusal

        async def fake_status(*args):
            return status
        monkeypatch.setattr(pitr.Pitr, 'replay', fake_replay)
        monkeypatch.setattr(pitr, 'get_status', fake_status)
        p = pitr.Pitr(manager, None, cql, None, ks)

        async def refused(reasons='code', **kwargs):
            return (await p.refused(1, reasons, ['watched'], **kwargs))[0]

        assert await refused() == refusal
        assert await refused({'code', 'other'}) == refusal
        with pytest.raises(AssertionError, match='expected a reason'):
            await refused('other')
        for bad, words in ((pitr.Outcome(pitr.DONE, None, '', ''), 'must be refused'),
                           (pitr.Outcome(pitr.FAILED, 'code', '', ''), 'states the reason'),
                           (pitr.Outcome(pitr.FAILED, 'code', 'code', ''), 'states the reason')):
            fake_replay.outcome = bad
            with pytest.raises(AssertionError, match=words):
                await refused()
        fake_replay.outcome = refusal

        action = f"INSERT INTO {ks}.watched (pk, ck, v) VALUES (5, 0, 'x')"
        with pytest.raises(AssertionError, match='changed'):
            await refused()
        await cql.run_async(f'TRUNCATE TABLE {ks}.watched')
        action = f"INSERT INTO {ks}.t (pk, ck, v) VALUES (6, 0, 'x')"
        with pytest.raises(AssertionError, match='changed'):
            await refused()

        # Rows in the target: a refusal before load fails, and a late one needs the target listed incomplete once status exists.
        action = None
        await cql.run_async(f"INSERT INTO {ks}.target (pk, ck, v) VALUES (7, 0, 'x')")
        with pytest.raises(AssertionError, match='empty'):
            await refused()
        status = pitr.Status('on', None, None, incomplete=[])
        with pytest.raises(AssertionError, match='listed incomplete'):
            await refused(loaded_ok=True)
        status = pitr.Status('on', None, None, incomplete=[(ks, 'target')])
        assert await refused(loaded_ok=True) == refusal
        await cql.run_async(f'TRUNCATE TABLE {ks}.target')
        with pytest.raises(AssertionError, match='must not list'):
            await refused()


async def test_adapter_parses_what_it_proposes():
    """The adapter's parsing against the surface it assumes, so a typo in it fails here rather than as a puzzling result later."""
    t_text, later = '2026-11-04T14:05:00.000001Z', '2026-11-04T14:05:01Z'
    t_us = pitr.from_iso(t_text)
    assert pitr.iso(t_us) == t_text and pitr.from_iso(later) == t_us + 999_999
    assert pitr.iso_times(f'from {t_text} to {later}, not 2026-11-04') == [t_us, t_us + 999_999]

    assert pitr.reason_in('target_missing: no table ks.x') == pitr.Reason.TARGET_MISSING
    assert pitr.reason_in('refused (t_past_coverage), not target_missing') == pitr.Reason.T_PAST_COVERAGE
    assert pitr.reason_in('error: the target_not_empty check') == pitr.Reason.TARGET_NOT_EMPTY
    assert pitr.reason_in('not_t_past_coverage_at_all') is None and pitr.reason_in('') is None

    refused = pitr.parse_outcome({'id': 'x', 'state': 'failed', 'error': f't_past_coverage: T {later} is past {t_text}; lag 1 s'})
    assert (refused.state, refused.reason) == (pitr.FAILED, pitr.Reason.T_PAST_COVERAGE) and t_text in refused.result_text
    assert pitr.latest_restorable(refused, t_us + 999_999) == t_us
    with pytest.raises(AssertionError):
        pitr.latest_restorable(refused, t_us)
    done = pitr.parse_outcome({'state': 'done', 'error': '', 'children': [{'text': pitr.WRONG_WAY_BACK}]})
    assert done.reason is None and pitr.WRONG_WAY_BACK in done.result_text

    status = pitr.parse_status({'state': 'shipping', 'covered_through': later, 'backups': [{'snapshot': 's', 'time': t_text}],
                                'incomplete': [{'keyspace': 'ks', 'table': 'x'}]})
    assert status.covered_through_us == t_us + 999_999 and status.backups == [('s', t_us)] and status.incomplete == [('ks', 'x')]

    schema = ("-- the schema of ks.t\nCREATE TABLE IF NOT EXISTS ks.t (\n    pk int,\n    v text,\n    PRIMARY KEY (pk)\n) WITH ID = "
              "01234567-89ab-cdef-0123-456789abcdef\n    AND compaction = {'class': 'LeveledCompactionStrategy'}\n    AND gc_grace_seconds = 0;\n")
    target = pitr.target_statement(schema, 'other', 'target')
    assert target.startswith('CREATE TABLE other.target (') and 'ID' not in target and 'Leveled' not in target, target
    assert "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}" in target and 'gc_grace_seconds = 0' in target, target
    bare = pitr.target_statement('CREATE TABLE ks.t (pk int PRIMARY KEY) WITH comment = \'c\';', 'ks', 'n')
    assert bare.endswith("AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}"), bare
    assert pitr.target_statement('CREATE TABLE ks.t (pk int PRIMARY KEY)', 'ks', 'n').endswith("WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}")
    with pytest.raises(AssertionError):
        pitr.target_statement('CREATE TYPE ks.a (x int);', 'ks', 'n')


class BigResponseServer:
    """Stands in for the store: it answers each request with a response of `size` bytes, and counts the requests."""
    def __init__(self, size):
        self.size = size
        self.requests = 0
        self.server = None
        self.port = None
        self.answer = None      # if set, a response waits until this asyncio.Event is set

    async def _serve(self, reader, writer):
        try:
            while data := await reader.readuntil(b'\r\n\r\n'):
                self.requests += 1
                if self.answer is not None:
                    await self.answer.wait()
                writer.write(b'x' * self.size)
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    async def __aenter__(self):
        self.server = await asyncio.start_server(self._serve, '127.0.0.1', 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *args):
        self.server.close()


async def test_gate_holds_a_response_part_way_and_only_for_the_request_its_rule_picks():
    """T: hold_after_response_bytes lets `count` bytes of a picked request's response through and holds the rest until release().
    R11 holds a segment's read half way with it, and counts the files on disk."""
    async with BigResponseServer(1000) as store:
        gate = GatedStore(SimpleNamespace(ip='127.0.0.1', port=store.port, region='us-east-1'))
        gate.start()
        gate.hold_after_response_bytes(lambda method, path: path.startswith('/b/pitr-log/'), 400)
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)

        other = b'GET /b/backups/x HTTP/1.1\r\n\r\n'
        writer.write(other)
        assert len(await asyncio.wait_for(reader.readexactly(1000), 10)) == 1000, 'a request the rule does not pick is carried whole'
        assert not gate.holding.is_set()

        writer.write(b'GET /b/pitr-log/seg HTTP/1.1\r\n\r\n')
        assert len(await asyncio.wait_for(reader.readexactly(400), 10)) == 400, 'the first count bytes must get through'
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(reader.readexactly(1), 0.5)
        assert gate.holding.is_set(), 'the gate must say it is holding'
        gate.release()
        assert not gate.holding.is_set(), 'release() must clear holding'
        assert len(await asyncio.wait_for(reader.readexactly(600), 10)) == 600, 'release() must let the rest of the response through'

        # A response shorter than count is never held, and holding stays cleared for the next use of the gate.
        gate.hold_after_response_bytes(lambda method, path: True, 5000)
        assert not gate.holding.is_set()
        writer.write(b'GET /b/pitr-log/small HTTP/1.1\r\n\r\n')
        assert len(await asyncio.wait_for(reader.readexactly(1000), 10)) == 1000
        assert not gate.holding.is_set(), 'a response shorter than count must not be held'
        writer.close()
        gate.stop()


async def test_gate_holds_a_response_and_recover_frees_it():
    async with BigResponseServer(1000) as store:
        gate = GatedStore(SimpleNamespace(ip='127.0.0.1', port=store.port, region='us-east-1'))
        gate.start()
        gate.hold_after_response_bytes(lambda method, path: True, 400)
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        writer.write(b'GET /b/k HTTP/1.1\r\n\r\n')
        assert len(await asyncio.wait_for(reader.readexactly(400), 10)) == 400
        # The gate sets holding after it has written the first bytes, so the client can read them before it is set.
        async def holding():
            return gate.holding.is_set() or None
        await wait_for(holding, time.time() + 5)
        assert gate.holding.is_set()
        gate.recover()
        assert not gate.holding.is_set(), 'recover() must clear holding'
        assert len(await asyncio.wait_for(reader.readexactly(600), 10)) == 600, 'recover() must free the held response'

        gate.hold_after_response_bytes(lambda method, path: True, 1000)
        writer.write(b'GET /b/k HTTP/1.1\r\n\r\n')
        assert len(await asyncio.wait_for(reader.readexactly(1000), 10)) == 1000, 'a response of exactly count bytes is carried whole'
        assert not gate.holding.is_set(), 'a response of exactly count bytes is not held'
        gate.recover()
        writer.write(b'GET /b/k HTTP/1.1\r\n\r\n')
        assert len(await asyncio.wait_for(reader.readexactly(1000), 10)) == 1000, 'recover() must clear the hold'
        assert store.requests == 3
        writer.close()
        gate.stop()


async def test_gate_does_not_hold_the_next_request_after_a_short_picked_response():
    """The budget of a picked response shorter than count must not carry over to the next request on the connection."""
    async with BigResponseServer(1000) as store:
        gate = GatedStore(SimpleNamespace(ip='127.0.0.1', port=store.port, region='us-east-1'))
        gate.start()
        gate.hold_after_response_bytes(lambda method, path: path.startswith('/b/pick/'), 1500)
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        writer.write(b'GET /b/pick/small HTTP/1.1\r\n\r\n')
        assert len(await asyncio.wait_for(reader.readexactly(1000), 10)) == 1000
        writer.write(b'GET /b/other HTTP/1.1\r\n\r\n')
        assert len(await asyncio.wait_for(reader.readexactly(1000), 10)) == 1000, 'an unrelated request must be carried whole'
        assert not gate.holding.is_set(), 'an unrelated request must not be held'
        writer.close()
        gate.stop()


async def test_gate_does_not_hold_a_response_that_arrives_after_release():
    async with BigResponseServer(1000) as store:
        store.answer = asyncio.Event()
        gate = GatedStore(SimpleNamespace(ip='127.0.0.1', port=store.port, region='us-east-1'))
        gate.start()
        gate.hold_after_response_bytes(lambda method, path: True, 400)
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        writer.write(b'GET /b/k HTTP/1.1\r\n\r\n')

        async def arrived():
            return True if store.requests else None
        await wait_for(arrived, time.time() + 10)
        gate.release()
        store.answer.set()
        assert len(await asyncio.wait_for(reader.readexactly(1000), 10)) == 1000, 'a released hold must carry the response whole'
        assert not gate.holding.is_set(), 'holding must not turn true after release()'
        writer.close()
        gate.stop()


async def test_gate_stop_frees_a_held_response_without_release():
    async with BigResponseServer(1000) as store:
        gate = GatedStore(SimpleNamespace(ip='127.0.0.1', port=store.port, region='us-east-1'))
        gate.start()
        gate.hold_after_response_bytes(lambda method, path: True, 400)
        reader, writer = await asyncio.open_connection('127.0.0.1', gate.port)
        writer.write(b'GET /b/k HTTP/1.1\r\n\r\n')
        assert len(await asyncio.wait_for(reader.readexactly(400), 10)) == 400
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(reader.readexactly(1), 0.5)
        gate.stop()
        try:
            await asyncio.wait_for(reader.read(), 5)
        except ConnectionError:
            pass
        writer.close()


async def test_segment_keys_lists_one_shard_by_its_zero_padded_directory(monkeypatch):
    """The product names a shard's directory with ten digits. Segments of the chain are found and ledger entries are not."""
    keys = {
        'pitr-log/h/0000000000/c1/segments/CommitLog-3-1.log': 1, 'pitr-log/h/0000000000/c1/00000000000000000042.entry': 1,
        'pitr-log/h/0000000000/c1/head': 1, 'pitr-log/h/0000000001/c1/segments/CommitLog-3-2.log': 1,
    }
    asked = []

    async def objects_of(storage, prefix):
        asked.append(prefix)
        return {k: v for k, v in keys.items() if k.startswith(prefix)}
    monkeypatch.setattr(pitr, 'objects_of', objects_of)
    assert await pitr.segment_keys(None, 'h', 0) == {'pitr-log/h/0000000000/c1/segments/CommitLog-3-1.log'}
    assert await pitr.segment_keys(None, 'h', 1) == {'pitr-log/h/0000000001/c1/segments/CommitLog-3-2.log'}
    assert asked == ['pitr-log/h/0000000000/', 'pitr-log/h/0000000001/']


def test_entry_uploads_are_told_from_segment_uploads_and_reads():
    entry = '/bkt/pitr-log/host/0/chain/000042.entry'
    segment = '/bkt/pitr-log/host/0/chain/segments/CommitLog-3-1.log'
    assert pitr.is_entry_upload('PUT', entry) and pitr.is_entry_upload('PUT', entry + '?x-id=PutObject')
    assert not pitr.is_entry_upload('PUT', segment), 'a segment upload is not an entry upload'
    assert not pitr.is_entry_upload('GET', entry) and not pitr.is_entry_upload('HEAD', entry), 'a read of an entry is not an upload'
    assert not pitr.is_entry_upload('PUT', '/bkt/backups/ks/t/snap/x.entry'), 'only the shipping prefix counts'
    assert pitr.is_shipping_upload('PUT', entry) and pitr.is_shipping_upload('PUT', segment)


def test_lag_is_read_in_microseconds_whatever_the_unit_spelling():
    assert pitr.lag_us('T 2026-11-04T14:05:01Z is past ..., a lag of 1500000us') == 1500000
    assert pitr.lag_us('with a lag of 1µs.') == 1 and pitr.lag_us('a lag of 7μs') == 7
    assert pitr.lag_us('a lag of 12 us') == 12
    for text in ('lag 1 s', 'a lag of us', 'a lag of 5ms', ''):
        with pytest.raises(AssertionError, match='no `a lag of'):
            pitr.lag_us(text)


async def test_log_wait_finds_a_line_from_a_mark_and_fails_at_the_deadline(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from test.pylib.log_browsing import ScyllaLogFile
    path = tmp_path / 'node.log'
    path.write_text('INFO fetching the history into /w/pitr/restore/old\n')
    with ThreadPoolExecutor(1) as pool:
        log = ScyllaLogFile(pool, path)
        monkeypatch.setattr(pitr, 'deadline', lambda: time.time() + 1)
        mark = await log.mark()
        with pytest.raises(AssertionError, match='no line matching'):
            await pitr.wait_log(log, 'never written')
        assert await pitr.fetch_dir(log) == '/w/pitr/restore/old', 'without a mark, the log is read from its start'
        with pytest.raises(AssertionError, match='no line matching'):
            await pitr.fetch_dir(log, from_mark=mark)   # the old line is before the mark
        with path.open('a') as f:
            f.write('INFO other\nINFO [shard 0] fetching the history into /w/pitr/restore/new-1\n')
        assert await pitr.fetch_dir(log, from_mark=mark) == '/w/pitr/restore/new-1'
        assert (await pitr.wait_log(log, r'other(\n)?', from_mark=mark)).group(0).startswith('other')


def test_later_than_is_after_its_argument_when_the_clock_stepped_back(monkeypatch):
    monkeypatch.setattr(pitr, 'now_us', lambda: 1000)
    assert pitr.later_than(500) == 1000
    assert pitr.later_than(1000) == 1001
    assert pitr.later_than(1150) == 1151, 'a clock 150 us behind must not give an earlier timestamp'


def test_count_files_counts_every_file_whatever_its_size(tmp_path):
    assert pitr.count_files(str(tmp_path / 'absent')) == 0
    assert pitr.count_files(str(tmp_path)) == 0
    (tmp_path / 'a').write_bytes(b'')
    (tmp_path / 'sub').mkdir()
    (tmp_path / 'sub' / 'b').write_bytes(b'x' * 10)
    assert pitr.count_files(str(tmp_path)) == 2, 'an empty file and one in a subdirectory count'


class FlakyStore:
    """Hands out resources whose put and delete fail as scripted, and counts the resources handed out."""
    bucket_name = 'b'

    def __init__(self, failures):
        self.failures = list(failures)
        self.resources = 0
        self.calls = []

    def get_resource(self):
        self.resources += 1
        store = self

        def act(name):
            def run(**kwargs):
                store.calls.append(name)
                if store.failures:
                    raise store.failures.pop(0)
            return run
        return SimpleNamespace(Object=lambda bucket, key: SimpleNamespace(put=act('put'), delete=act('delete')))


def closed():
    return botocore.exceptions.ConnectionClosedError(endpoint_url='http://x')


class FakeClock:
    """Stands in for the time module in pitr_util: sleeping advances the clock and is recorded, so that nothing waits."""
    def __init__(self):
        self.now = 1000.0
        self.slept = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


async def test_tamper_calls_retry_a_dropped_connection_with_backoff_and_a_fresh_resource(caplog, monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(pitr, 'time', clock)
    monkeypatch.setattr(pitr, 'deadline', lambda: clock.now + 60)
    store = FlakyStore([closed(), ConnectionResetError()])
    with caplog.at_level(logging.WARNING):
        await pitr.put_object(store, 'k', b'v')
    assert store.calls == ['put'] * 3 and store.resources == 3, 'each attempt must use a fresh resource'
    assert sum('retry' in r.getMessage() for r in caplog.records) == 2, 'each retry must be logged'
    assert clock.slept == [0.2, 0.5], 'the pause must grow'

    clock.slept.clear()
    store = FlakyStore([closed()] * 6)
    await pitr.delete_object(store, 'k')
    assert len(store.calls) == 7, 'more than three drops must still be retried'
    assert clock.slept == [0.2, 0.5, 1, 2, 2, 2], 'the pause must grow to 2 s and stay there'

    clock.slept.clear()
    start = clock.now
    store = FlakyStore([closed()] * 1000)
    with pytest.raises(botocore.exceptions.ConnectionClosedError):
        await pitr.delete_object(store, 'k')
    assert 58 <= sum(clock.slept) <= 60 and clock.now - start == pytest.approx(sum(clock.slept)), 'a persistent drop must be retried up to the deadline and then fail'

    denied = botocore.exceptions.ClientError({'Error': {'Code': 'AccessDenied', 'Message': 'no'}}, 'PutObject')
    store = FlakyStore([denied])
    with pytest.raises(botocore.exceptions.ClientError):
        await pitr.put_object(store, 'k', b'v')
    assert store.calls == ['put'], 'only a dropped connection is retried: a real error must reach the test'


async def test_raw_post_returns_the_status_and_body_of_a_rejection_and_of_a_success():
    seen = []

    async def serve(reader, writer):
        head = await reader.readuntil(b'\r\n\r\n')
        seen.append(head.split(b'\r\n')[0].decode())
        status, body = (b'400 Bad Request', b'{"message": "Missing mandatory parameter \'t\'"}') if b't=' not in head.split(b'\r\n')[0] \
            else (b'200 OK', b'"task-id"')
        writer.write(b'HTTP/1.1 %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s' % (status, len(body), body))
        await writer.drain()
        writer.close()
    server = await asyncio.start_server(serve, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    manager = SimpleNamespace(api=SimpleNamespace(client=SimpleNamespace(default_port=port)))
    node = SimpleNamespace(ip_addr='127.0.0.1')
    try:
        status, body = await pitr.post_raw(manager, node, pitr.REPLAY_URI, {'keyspace': 'ks'})
        assert status == 400 and "Missing mandatory parameter 't'" in body
        status, body = await pitr.post_raw(manager, node, pitr.REPLAY_URI, {'keyspace': 'ks', 't': ''})
        assert (status, body) == (200, '"task-id"'), 'an empty value is sent, not dropped'
        assert seen[0].startswith('POST /storage_service/pitr/replay_to?keyspace=ks ') and 't=' in seen[1]
    finally:
        server.close()


async def test_abort_task_posts_to_the_task_and_surfaces_a_refusal():
    aborted = []

    async def abort(ip, tid):
        if tid == 'bad':
            raise HTTPError('uri', 404, {}, None, 'no such task')
        aborted.append((ip, tid))
    manager = SimpleNamespace(api=SimpleNamespace(abort_task=abort))
    node = SimpleNamespace(ip_addr='127.0.0.1')
    await pitr.abort_task(manager, node, 'id1')
    assert aborted == [('127.0.0.1', 'id1')]
    with pytest.raises(HTTPError):
        await pitr.abort_task(manager, node, 'bad')
