#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""PITR refusals: each condition restore must refuse, and that nothing changes when it does."""

import logging
import random

import pytest

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    ALTER_KEYSPACE_RF1_NONDURABLE, ALTER_KEYSPACE_RF2, apply, assert_refused, assert_status_stays, assert_stays_unprotected, backup_prefix,
    CFG_DISK_RESERVE_PERCENT, CLOCK_MARGIN_US, create_alterable_table, create_unsupported_tables, deadline, delete_object, FAILED, get_object,
    is_immutable, iso, ISO_RE, iso_times, needs_phase, NEWER_BACKUP_WORDS, node_config, NONDURABLE_KS, now_us, objects_of, old_ts,
    PAST_COVERAGE_WORDS, pks_of, protect_table, protected_table, put_object, Reason, report_stalls, restart, restore_ok, RF2_KS, run_restore,
    schema_change_statement, SCHEMA_CHANGES, shipping_options, SHIPPING_PREFIX, simple_rows, STEP_US, STRONG_KS, table_exists, TABLE_SCHEMA,
    TABLETS_KS, TOPOLOGY_LIMIT_WORDS, unsupported_tables_config, VNODES_KS, wait_covered
)
from test.cluster.object_store.test_backup import do_backup, take_snapshot_on_one_server
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name, wait_for

logger = logging.getLogger(__name__)

# Which table of create_unsupported_tables() each kind is: where to find it among the returned cases, by reason and name.
UNSUPPORTED_KINDS = {'tablets': ('t', Reason.TABLETS), 'strongly_consistent': ('t', Reason.STRONG), 'durable_writes_false': ('t', Reason.DURABLE),
                     'counters': ('counters', Reason.COUNTERS), 'view_base': ('viewed', Reason.VIEW_BASE), 'view': ('a_view', Reason.VIEW_TABLE),
                     'indexed': ('indexed', Reason.INDEXED), 'index_table': ('an_index_index', Reason.INDEX_TABLE),
                     'cdc': ('cdc', Reason.CDC), 'cdc_log': ('cdc_scylla_cdc_log', Reason.CDC), 'logstor': ('logstor', Reason.LOGSTOR),
                     'system': ('local', Reason.SYSTEM), 'system_schema': ('tables', Reason.SYSTEM), 'alternator': ('alt', Reason.ALTERNATOR)}


@needs_phase('1.3')
@pytest.mark.parametrize('kind', UNSUPPORTED_KINDS)
async def test_refuse_unsupported_table(manager: ScyllaClusterManager, s3_storage, kind):
    """T18, REF-1."""
    server = await manager.server_add(config=unsupported_tables_config(node_config(s3_storage)))
    cql = manager.get_cql()
    name, reason = UNSUPPORTED_KINDS[kind]
    async with new_test_keyspace(manager, VNODES_KS) as ks, \
               new_test_keyspace(manager, TABLETS_KS) as tablet_ks, \
               new_test_keyspace(manager, STRONG_KS) as strong_ks, \
               new_test_keyspace(manager, NONDURABLE_KS) as nondurable_ks:
        cases = await create_unsupported_tables(server, cql, ks, tablet_ks, strong_ks, nondurable_ks)
        keyspace = next(k for k, cf, r in cases if (cf, r) == (name, reason))
        # A strongly consistent table can't be scanned, so whether it stays the same isn't checked.
        await assert_refused(manager, server, cql, keyspace, name, 'any_backup', now_us(), reason, read_source=keyspace != strong_ks)


@needs_phase('1.3')
async def test_refuse_rf_above_1_from_creation(manager: ScyllaClusterManager, s3_storage):
    """T19, REF-2."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF2_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        await do_backup(server, snap, backup_prefix(ks, 't', snap), ks, 't', s3_storage, manager, logger)
        result = await assert_refused(manager, server, cql, ks, 't', snap, now_us(), Reason.RF)
        assert TOPOLOGY_LIMIT_WORDS[Reason.RF] in result.message.lower(), f'expected the RF=1 limit stated, saw: {result.message}'


@needs_phase('1.3')
async def test_refuse_rf_altered_after_protection_began(manager: ScyllaClusterManager, s3_storage):
    """T19, REF-2."""
    async with protected_table(manager, s3_storage) as p:
        t = await p.covered()
        await p.cql.run_async(ALTER_KEYSPACE_RF2.format(ks=p.ks))
        result = await p.refused(t, Reason.RF)
        assert TOPOLOGY_LIMIT_WORDS[Reason.RF] in result.message.lower(), f'expected the RF=1 limit stated, saw: {result.message}'


@needs_phase('1.3')
async def test_refuse_node_added_after_protection_began(manager: ScyllaClusterManager, s3_storage):
    """T19, REF-2."""
    async with protected_table(manager, s3_storage) as p:
        t = await p.covered()
        await manager.server_add(config=node_config(s3_storage))
        result = await p.refused(t, Reason.NODES)
        assert TOPOLOGY_LIMIT_WORDS[Reason.NODES] in result.message.lower(), f'expected the one-node limit stated, saw: {result.message}'


# Gaps and damage in the shipped history

MISSING = {Reason.OBJECT_MISSING, Reason.RECORDS_MISSING}
DAMAGED = {Reason.OBJECT_DAMAGED, Reason.RECORDS_DAMAGED}


# Bigger than any idle seal may be (4 KiB, req:82), and random, so that compression can't shrink it.
PAYLOAD = random.Random(22).randbytes(4096).hex()


async def shipped_object_holding_writes(p, table, known) -> tuple[list, int, str]:
    """Writes after the backup, then waits until T is past every upload. Returns the writes, T, and the biggest new object: it holds the writes, because
    an object of an idle seal stays under 4 KiB. Works without knowing the format: an object can't hold anything newer than its upload, and S3 reports
    upload times in seconds, hence the 2 seconds."""
    writes = simple_rows(range(8, 16), max(now_us(), p.backup.start_us + STEP_US), PAYLOAD)
    await apply(p.cql, p.ks, table, writes)
    await p.covered()
    objects = await objects_of(p.storage, SHIPPING_PREFIX)
    newest = max(o.last_modified for o in objects.values()).timestamp()
    t = await p.covered(int(newest * 1e6) + 2 * CLOCK_MARGIN_US)
    window = {k: o for k, o in objects.items() if k not in known and is_immutable(k)}
    assert window, 'expected a new shipped object, or this test proves nothing'
    key = max(window, key=lambda k: (window[k].size, k))
    assert window[key].size > 4096, f'expected the object holding the writes to be over 4 KiB, or it may be an idle seal: {key} is {window[key].size} bytes'
    return writes, t, key


async def shipped_object_in_window(p, table='t') -> tuple[int, str, list]:
    await p.covered()
    known = await objects_of(p.storage, SHIPPING_PREFIX)
    writes, t, key = await shipped_object_holding_writes(p, table, known)
    return t, key, writes


async def probe(p, t, key, reasons):
    """A restore that has to be refused for this object."""
    new = unique_name('tampered_')
    result = await run_restore(p.manager, p.server, p.ks, p.cf, p.backup.name, t, new)
    assert result.state == FAILED and result.reason in reasons, f'expected a refusal with one of {reasons}, saw: {result}'
    assert key.rsplit('/', 1)[-1] in result.message, f'expected the refusal to name {key}: {result.message}'
    assert not result.cells_applied, f'expected a refusal before any data was loaded: {result}'
    assert not await table_exists(p.cql, p.ks, new), f'expected no new table: {result}'
    return result


WRITTEN_TO = ['this_table', 'other_table']


async def tamper_setup(p, written):
    if written == 'other_table':
        await p.cql.run_async(f'CREATE TABLE {p.ks}.other {TABLE_SCHEMA}')
    return await shipped_object_in_window(p, 't' if written == 'this_table' else 'other')


@needs_phase('1.3')
@pytest.mark.parametrize('written', WRITTEN_TO)
async def test_refuse_a_removed_shipped_object(manager: ScyllaClusterManager, s3_storage, written):
    """T20, REF-4."""
    async with protected_table(manager, s3_storage) as p:
        t, key, _ = await tamper_setup(p, written)
        await delete_object(s3_storage, key)
        await probe(p, t, key, MISSING)


@needs_phase('1.3')
@pytest.mark.parametrize('written', WRITTEN_TO)
async def test_refuse_a_shipped_object_cut_short(manager: ScyllaClusterManager, s3_storage, written):
    """T20, REF-4."""
    async with protected_table(manager, s3_storage) as p:
        t, key, _ = await tamper_setup(p, written)
        original = await get_object(s3_storage, key)
        await put_object(s3_storage, key, original[:len(original) // 2])
        await probe(p, t, key, DAMAGED)


@needs_phase('1.3')
@pytest.mark.parametrize('written', WRITTEN_TO)
async def test_refuse_a_shipped_object_with_one_byte_changed(manager: ScyllaClusterManager, s3_storage, written):
    """T20, REF-4."""
    async with protected_table(manager, s3_storage) as p:
        t, key, _ = await tamper_setup(p, written)
        changed = bytearray(await get_object(s3_storage, key))
        changed[len(changed) // 2] ^= 0x01
        await put_object(s3_storage, key, bytes(changed))
        await probe(p, t, key, DAMAGED)


@needs_phase('1.3')
async def test_restore_works_once_a_tampered_object_is_put_back(manager: ScyllaClusterManager, s3_storage):
    """T20, REF-4. The control: the tampering, and nothing else, caused the refusals."""
    async with protected_table(manager, s3_storage) as p:
        t, key, writes = await shipped_object_in_window(p)
        original = await get_object(s3_storage, key)
        await delete_object(s3_storage, key)
        await probe(p, t, key, MISSING)
        await put_object(s3_storage, key, original)
        await p.assert_restored_exact(p.seed + writes, t)


async def gap_after_some_writes(p) -> tuple[int, int, list, str]:
    """Writes, then later writes, and removes a shipped object holding only the later ones. Returns the coverage from before the gap,
    a T past it, the earlier history, and the object."""
    early = simple_rows(range(8, 12), p.backup.start_us + STEP_US)
    await p.write(early)
    before_gap = await p.covered()
    known = await objects_of(p.storage, SHIPPING_PREFIX)    # everything up to before_gap is in these
    _, end, key = await shipped_object_holding_writes(p, p.cf, known)
    await delete_object(p.storage, key)
    return before_gap, end, p.seed + early, key


@needs_phase('1.3')
async def test_gap_refusal_gives_the_remaining_interval(manager: ScyllaClusterManager, s3_storage):
    """T31, REF-4."""
    async with protected_table(manager, s3_storage) as p:
        before_gap, end, _, _ = await gap_after_some_writes(p)
        result = await p.refused(end, MISSING)
        assert result.remaining_start_us == p.backup.start_us, f'expected it to start at the backup: {result}'
        assert before_gap <= result.remaining_end_us < end, f'expected the end between {iso(before_gap)} and {iso(end)}: {result}'


@needs_phase('1.3')
async def test_gap_refusal_message_states_the_remaining_interval(manager: ScyllaClusterManager, s3_storage):
    """T31, REF-4."""
    async with protected_table(manager, s3_storage) as p:
        _, end, _, _ = await gap_after_some_writes(p)
        result = await p.refused(end, MISSING)
        times = iso_times(result.message)
        assert result.remaining_start_us in times and result.remaining_end_us in times, f'expected both times in the message: {result}'


@needs_phase('1.3')
async def test_t_inside_the_remaining_interval_is_exact_with_the_gap_still_there(manager: ScyllaClusterManager, s3_storage):
    """T31, REF-4."""
    async with protected_table(manager, s3_storage) as p:
        before_gap, _, history, key = await gap_after_some_writes(p)
        await p.assert_restored_exact(history, before_gap)
        assert key not in await objects_of(s3_storage, SHIPPING_PREFIX), 'expected the gap to still be there'


@needs_phase('1.3')
async def test_t_at_the_end_of_the_remaining_interval_is_accepted(manager: ScyllaClusterManager, s3_storage):
    """T31, REF-4."""
    async with protected_table(manager, s3_storage) as p:
        _, end, _, _ = await gap_after_some_writes(p)
        result = await p.refused(end, MISSING)
        _, new = await p.restore(result.remaining_end_us)
        assert set(range(12)) <= await pks_of(p.cql, p.ks, new)


@needs_phase('1.3')
async def test_t_1us_past_the_remaining_interval_is_refused(manager: ScyllaClusterManager, s3_storage):
    """T31, REF-4."""
    async with protected_table(manager, s3_storage) as p:
        _, end, _, _ = await gap_after_some_writes(p)
        result = await p.refused(end, MISSING)
        await p.refused(result.remaining_end_us + 1, MISSING)


@needs_phase('1.3')
@pytest.mark.parametrize('upload', ['before_shipping', 'after_shipping'])
async def test_refuse_a_snapshot_from_before_shipping(manager: ScyllaClusterManager, s3_storage, upload):
    """T21, REF-4, SHP-5. Writes between the snapshot and shipping's start are in neither, even if the upload comes after."""
    server = await manager.server_add(config=node_config(s3_storage, shipping=False))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows(range(4), old_ts()))
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        prefix = backup_prefix(ks, 't', snap)
        if upload == 'before_shipping':
            await do_backup(server, snap, prefix, ks, 't', s3_storage, manager, logger)
        await manager.server_update_config(server.server_id, config_options=shipping_options(s3_storage))
        cql = await restart(manager, server)
        if upload == 'after_shipping':
            await do_backup(server, snap, prefix, ks, 't', s3_storage, manager, logger)
        result = await assert_refused(manager, server, cql, ks, 't', snap, now_us() - CLOCK_MARGIN_US, Reason.BACKUP_BEFORE_SHIPPING)
        assert 'nodetool backup' in result.message, f'expected the next action, saw: {result.message}'


@needs_phase('1.2')
@pytest.mark.parametrize('upload', ['before_shipping', 'after_shipping'])
async def test_status_doesnt_list_a_snapshot_from_before_shipping(manager: ScyllaClusterManager, s3_storage, upload):
    """T21, SHP-5."""
    server = await manager.server_add(config=node_config(s3_storage, shipping=False))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        prefix = backup_prefix(ks, 't', snap)
        if upload == 'before_shipping':
            await do_backup(server, snap, prefix, ks, 't', s3_storage, manager, logger)
        await manager.server_update_config(server.server_id, config_options=shipping_options(s3_storage))
        await restart(manager, server)
        if upload == 'after_shipping':
            await do_backup(server, snap, prefix, ks, 't', s3_storage, manager, logger)
        await assert_stays_unprotected(manager, server, ks, 't')


# T outside coverage

FUTURE_US = 86400_000_000


async def assert_refusal_gives_latest_and_lag(result, t):
    missing = [word for word in PAST_COVERAGE_WORDS if word not in result.message.lower()]
    assert not missing, f'expected {missing} in: {result.message}'
    assert any(x < t for x in iso_times(result.message)), f'expected the latest restorable time in: {result.message}'


@needs_phase('1.3')
async def test_refuse_t_in_the_future(manager: ScyllaClusterManager, s3_storage):
    """T22, REF-5."""
    async with protected_table(manager, s3_storage) as p:
        t = now_us() + FUTURE_US
        result = await p.refused(t, Reason.T_PAST_COVERAGE)
        await assert_refusal_gives_latest_and_lag(result, t)


@needs_phase('1.3')
async def test_refuse_t_above_coverage_with_data_there_and_a_shard_idle(manager: ScyllaClusterManager, s3_storage):
    """T22, REF-5. One key, so the other shard is idle, with a timestamp far ahead: data exists at T, but T isn't covered."""
    async with protected_table(manager, s3_storage) as p:
        t = now_us() + FUTURE_US
        await p.write(simple_rows([50], t))
        await p.covered()
        result = await p.refused(t, Reason.T_PAST_COVERAGE)
        await assert_refusal_gives_latest_and_lag(result, t)


@needs_phase('1.3')
async def test_refuse_t_1us_below_the_interval(manager: ScyllaClusterManager, s3_storage):
    """T22, T32, REF-6."""
    async with protected_table(manager, s3_storage) as p:
        result = await p.refused(p.backup.start_us - 1, Reason.T_BEFORE_COVERAGE)
        assert p.backup.start_us in iso_times(result.message), f'expected the interval in: {result.message}'


@needs_phase('1.3')
async def test_refuse_a_backup_that_doesnt_exist(manager: ScyllaClusterManager, s3_storage):
    """T22, REF-6."""
    async with protected_table(manager, s3_storage) as p:
        result = await p.refused(p.backup.start_us, Reason.NO_SUCH_BACKUP, backup='no_such_backup')
        assert p.backup.name in result.message or p.backup.start_us in iso_times(result.message), f'expected the usable backup or interval in: {result.message}'


@needs_phase('1.3')
async def test_refuse_when_there_is_no_backup(manager: ScyllaClusterManager, s3_storage):
    """T22, REF-6."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        result = await assert_refused(manager, server, cql, ks, 't', 'no_such_backup', now_us(), Reason.NO_BACKUP)
        assert 'nodetool snapshot' in result.message and 'nodetool backup' in result.message, f'expected the exact command in: {result.message}'


@needs_phase('1.3')
async def test_refuse_a_snapshot_never_uploaded(manager: ScyllaClusterManager, s3_storage):
    """T23, REF-7."""
    async with protected_table(manager, s3_storage) as p:
        never, _ = await take_snapshot_on_one_server(p.ks, p.server, manager, logger)
        result = await p.refused(await p.covered(), Reason.BACKUP_UNFINISHED, backup=never)
        assert p.backup.name in result.message, f'expected the usable backups listed in: {result.message}'


@needs_phase('1.2')
async def test_status_doesnt_list_a_snapshot_never_uploaded(manager: ScyllaClusterManager, s3_storage):
    """T23, REF-7."""
    async with protected_table(manager, s3_storage) as p:
        await take_snapshot_on_one_server(p.ks, p.server, manager, logger)
        await assert_status_stays(manager, p.server, p.ks, p.cf, lambda e: [b.name for b in e.backups] == [p.backup.name], f'only {p.backup.name} listed')


# Schema changes and truncates in the window

async def alterable_table(manager, storage):
    server = await manager.server_add(config=node_config(storage))
    return server


SCHEMA_CHANGE_IDS = ['add_column', 'drop_column', 'option', 'udt_it_uses']


async def change_schema_in_window(manager, server, storage, ks, change) -> dict:
    """Backs up, changes the schema, and waits for coverage past it. Returns the times and the DESCRIBE from before."""
    cql = manager.get_cql()
    await create_alterable_table(cql, ks)
    await apply(cql, ks, 't', simple_rows(range(4), old_ts()))
    describe = (await cql.run_async(f'DESCRIBE TABLE {ks}.t'))[0].create_statement
    backup = await protect_table(manager, server, storage, ks, 't')
    before = await wait_covered(manager, server, ks, 't')
    low = now_us()
    await cql.run_async(schema_change_statement(change, ks))
    high = now_us()
    after = await wait_covered(manager, server, ks, 't')
    return dict(backup=backup, before=before, after=after, low=low, high=high, describe=describe)


@needs_phase('1.3')
@pytest.mark.parametrize('change', SCHEMA_CHANGES, ids=SCHEMA_CHANGE_IDS)
async def test_refuse_t_after_a_schema_change(manager: ScyllaClusterManager, s3_storage, change):
    """T24, REF-9."""
    server = await alterable_table(manager, s3_storage)
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        c = await change_schema_in_window(manager, server, s3_storage, ks, change)
        result = await assert_refused(manager, server, manager.get_cql(), ks, 't', c['backup'].name, c['after'], Reason.SCHEMA_CHANGED)
        times = iso_times(result.message)
        assert any(c['low'] - CLOCK_MARGIN_US <= x <= c['high'] + CLOCK_MARGIN_US for x in times), f'expected the event time in: {result.message}'
        assert any(word in result.message.lower() for word in NEWER_BACKUP_WORDS), f'expected the next action in: {result.message}'


@needs_phase('1.3')
@pytest.mark.parametrize('change', SCHEMA_CHANGES, ids=SCHEMA_CHANGE_IDS)
async def test_t_before_a_schema_change_uses_the_old_schema(manager: ScyllaClusterManager, s3_storage, change):
    """T24, REF-9."""
    server = await alterable_table(manager, s3_storage)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        c = await change_schema_in_window(manager, server, s3_storage, ks, change)
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', c['backup'].name, c['before'], new)
        restored = (await cql.run_async(f'DESCRIBE TABLE {ks}.{new}'))[0].create_statement
        assert restored.replace(f'{ks}.{new}', f'{ks}.t') == c['describe'], f"expected:\n{c['describe']}\nsaw:\n{restored}"


@needs_phase('1.3')
@pytest.mark.parametrize('change', SCHEMA_CHANGES, ids=SCHEMA_CHANGE_IDS)
async def test_newer_backup_is_accepted_after_a_schema_change(manager: ScyllaClusterManager, s3_storage, change):
    """T24, REF-9."""
    server = await alterable_table(manager, s3_storage)
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await change_schema_in_window(manager, server, s3_storage, ks, change)
        newer = await protect_table(manager, server, s3_storage, ks, 't')
        await restore_ok(manager, server, ks, 't', newer.name, await wait_covered(manager, server, ks, 't'), unique_name('restored_'))


@needs_phase('1.3')
@pytest.mark.parametrize('change', ['ALTER TABLE {ks}.other ADD added int', 'ALTER TYPE {ks}.unused ADD extra int'], ids=['other_table', 'udt_it_doesnt_use'])
async def test_schema_change_elsewhere_is_accepted(manager: ScyllaClusterManager, s3_storage, change):
    """T33, REF-9."""
    async with protected_table(manager, s3_storage) as p:
        await p.cql.run_async(f'CREATE TABLE {p.ks}.other {TABLE_SCHEMA}')
        await p.cql.run_async(f'CREATE TYPE {p.ks}.unused (zip int)')
        await p.cql.run_async(change.format(ks=p.ks))
        await p.assert_restored_exact(p.seed, await p.covered())


@needs_phase('1.3')
async def test_refuse_after_durable_writes_is_turned_off(manager: ScyllaClusterManager, s3_storage):
    """T33, REF-1."""
    async with protected_table(manager, s3_storage) as p:
        t = await p.covered()
        await p.cql.run_async(ALTER_KEYSPACE_RF1_NONDURABLE.format(ks=p.ks))
        await p.refused(t, Reason.DURABLE)


async def truncate_in_window(p) -> dict:
    before = await p.covered()
    low = now_us()
    await p.cql.run_async(f'TRUNCATE TABLE {p.ks}.t')
    high = now_us()
    await p.write(simple_rows([50], high))
    return dict(before=before, after=await p.covered(), low=low, high=high)


@needs_phase('1.3')
async def test_refuse_t_after_a_truncate(manager: ScyllaClusterManager, s3_storage):
    """T25, REF-10."""
    async with protected_table(manager, s3_storage) as p:
        c = await truncate_in_window(p)
        result = await p.refused(c['after'], Reason.TRUNCATED)
        times = iso_times(result.message)
        assert any(c['low'] - CLOCK_MARGIN_US <= x <= c['high'] + CLOCK_MARGIN_US for x in times), f'expected the event time in: {result.message}'
        assert any(word in result.message.lower() for word in NEWER_BACKUP_WORDS), f'expected the next action in: {result.message}'


@needs_phase('1.3')
async def test_t_before_a_truncate_is_accepted(manager: ScyllaClusterManager, s3_storage):
    """T25, REF-10."""
    async with protected_table(manager, s3_storage) as p:
        c = await truncate_in_window(p)
        await p.assert_restored_exact(p.seed, c['before'])


@needs_phase('1.3')
async def test_newer_backup_is_accepted_after_a_truncate(manager: ScyllaClusterManager, s3_storage):
    """T25, REF-10."""
    async with protected_table(manager, s3_storage) as p:
        await truncate_in_window(p)
        newer = await protect_table(manager, p.server, s3_storage, p.ks, p.cf)
        _, new = await p.restore(await p.covered(), backup=newer.name)
        seen = await pks_of(p.cql, p.ks, new)
        assert seen == {50}, f'expected only the write after the truncate, saw {seen}'


# The form of T

INVALID_T = {'no_zone': '2026-11-04T14:05:00', 'no_zone_with_fraction': '2026-11-04T14:05:00.123456', 'offset': '2026-11-04T14:05:00+02:00',
             'zero_offset': '2026-11-04T14:05:00+00:00',
             'local_form': '2026-11-04 14:05:00', 'space_with_z': '2026-11-04 14:05:00Z', 'seven_digits': '2026-11-04T14:05:00.1234567Z',
             'impossible_day': '2026-02-30T14:05:00Z',
             'impossible_month': '2026-13-01T14:05:00Z', 'leap_second': '2026-12-31T23:59:60Z', 'hour_24': '2026-11-04T24:00:00Z',
             'lowercase_z': '2026-11-04T14:05:00z',
             'lowercase_t': '2026-11-04t14:05:00Z', 'leading_space': ' 2026-11-04T14:05:00Z', 'trailing_space': '2026-11-04T14:05:00Z ',
             'seconds': '1730000000',
             'microseconds': '1730000000000000', 'empty': '', 'only_a_space': ' ', 'word': 'now'}


@needs_phase('1.3')
@pytest.mark.parametrize('name', INVALID_T)
async def test_refuse_invalid_t(manager: ScyllaClusterManager, s3_storage, name):
    """T26, REF-14."""
    async with protected_table(manager, s3_storage) as p:
        result = await p.refused(INVALID_T[name], Reason.T_INVALID)
        assert ISO_RE.search(result.message), f'expected an example of the form in: {result.message}'


SHORT_FORMS = {'no_fraction': ('{base}Z', {1}), 'six_digits': ('{base}.000000Z', {1}), 'one_digit': ('{base}.5Z', {1, 2}),
               'below_the_half': ('{base}.499999Z', {1})}


@needs_phase('1.3')
@pytest.mark.parametrize('name', SHORT_FORMS)
async def test_missing_fields_below_the_second_mean_zero(manager: ScyllaClusterManager, s3_storage, name):
    """T26, REF-14. Rows sit exactly on the boundary, so a form read wrongly shows."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        whole_second = (p.backup.start_us // 1_000_000 + 2) * 1_000_000
        await p.write(simple_rows([1], whole_second) + simple_rows([2], whole_second + 500_000))
        await p.covered(whole_second + 500_000 + CLOCK_MARGIN_US)
        await p.covered()
        form, expected = SHORT_FORMS[name]
        t = form.format(base=iso(whole_second)[:-len('.000000Z')])
        _, new = await p.restore(t)
        seen = await pks_of(p.cql, p.ks, new)
        assert seen == expected, f'expected keys {expected} at {t}, saw {seen}'


# The disk reserve

async def set_reserve(manager, server, percent):
    await manager.server_update_config(server.server_id, CFG_DISK_RESERVE_PERCENT, percent)

    async def applied():
        return True if await manager.api.get_config(server.ip_addr, CFG_DISK_RESERVE_PERCENT) == percent else None
    await wait_for(applied, deadline())


@needs_phase('1.3')
async def test_disk_reserve_defaults_to_10_percent(manager: ScyllaClusterManager, s3_storage):
    """T27, REF-16."""
    server = await manager.server_add(config=node_config(s3_storage))
    seen = await manager.api.get_config(server.ip_addr, CFG_DISK_RESERVE_PERCENT)
    assert seen == 10, f'expected 10, saw {seen}'


@needs_phase('1.3')
async def test_refuse_when_the_disk_reserve_would_be_breached(manager: ScyllaClusterManager, s3_storage):
    """T27, REF-16."""
    async with protected_table(manager, s3_storage) as p:
        t = await p.covered()
        await set_reserve(manager, p.server, 100)
        result = await p.refused(t, Reason.DISK)
        assert result.needed_bytes > 0 and result.free_bytes > 0 and result.reserve_bytes > 0, f'expected all three figures: {result}'
        assert result.free_bytes - result.needed_bytes < result.reserve_bytes, f'expected the figures to explain the refusal: {result}'


@needs_phase('1.3')
async def test_lowering_the_disk_reserve_lets_a_restore_through(manager: ScyllaClusterManager, s3_storage):
    """T27, REF-16."""
    async with protected_table(manager, s3_storage) as p:
        t = await p.covered()
        await set_reserve(manager, p.server, 100)
        await p.refused(t, Reason.DISK)
        await set_reserve(manager, p.server, 1)
        await p.assert_restored_exact(p.seed, t)


# The new table's name

BAD_NAMES = {'taken_by_another_table': ('existing', Reason.NAME_TAKEN), 'taken_by_a_finished_restore': ('finished', Reason.NAME_TAKEN),
             'the_source': ('t', Reason.NAME_TAKEN),
             'too_long': ('a' * 49, Reason.NAME_INVALID), 'has_a_space': ('has space', Reason.NAME_INVALID), 'has_a_dash': ('has-dash', Reason.NAME_INVALID),
             'empty': ('', Reason.NAME_INVALID), 'dotted': ('some.name', Reason.NAME_INVALID)}


@needs_phase('1.3')
@pytest.mark.parametrize('name', BAD_NAMES)
async def test_refuse_new_table_name(manager: ScyllaClusterManager, s3_storage, name):
    """T28, REF-13."""
    async with protected_table(manager, s3_storage) as p:
        await p.cql.run_async(f'CREATE TABLE {p.ks}.existing {TABLE_SCHEMA}')
        await apply(p.cql, p.ks, 'existing', simple_rows([9], old_ts(), 'keep'))
        t = await p.covered()
        await p.restore(t, new_table='finished')
        new_table, reason = BAD_NAMES[name]
        await p.refused(t, reason, watched=['existing', 'finished'], new_table=new_table)


@needs_phase('1.3')
async def test_refuse_new_table_in_another_keyspace(manager: ScyllaClusterManager, s3_storage):
    """T28, REF-13."""
    async with protected_table(manager, s3_storage) as p, new_test_keyspace(manager, VNODES_KS) as other_ks:
        await p.refused(await p.covered(), Reason.NAME_OTHER_KEYSPACE, new_table='elsewhere', new_keyspace=other_ks)
        assert not await table_exists(p.cql, other_ks, 'elsewhere'), 'expected no table in the other keyspace'


@needs_phase('1.3')
async def test_new_table_name_of_maximum_length_is_accepted(manager: ScyllaClusterManager, s3_storage):
    """T28, REF-13. The control for the refusals above."""
    async with protected_table(manager, s3_storage) as p:
        longest = 'a' * 48
        await p.restore(await p.covered(), new_table=longest)
        assert await table_exists(p.cql, p.ks, longest), f'expected table {longest}'
