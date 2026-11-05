#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""PITR restore: the new table is exact at T, its schema is the backup's, and the result says what was done."""

import logging

import pytest

from test.cluster.object_store.pitr_util import (  # noqa: F401 (some are fixtures)
    _plain, apply, assert_every_shard, assert_exact, Batch, change_statements, collection_edge_statements, conditional_history, COPY_NOTICE_WORDS,
    create_rich_table, FLAG_EMPTY_BACKUP, needs_phase, node_config, NOT_VERIFIED_RE, now_us, old_ts, pks_of, protect_table, protected_table, read_all,
    read_exact, report_stalls, restore_ok, RESULT_LIMIT_WORDS, run_status_commands, seed_statements, simple_rows, STEP_US, STEPS, VNODES_KS, W,
    wait_covered, wait_protected, Writer, WRONG_WAY_BACK
)
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

SEED = "UPDATE {t} USING TIMESTAMP {ts} SET v = 'seed', s = 1, m = {{1: 'a'}}, st = {{1}} WHERE pk = 1 AND ck = 0"
CHANGES = {
    'insert': "INSERT INTO {t} (pk, ck, v) VALUES (1, 1, 'new') USING TIMESTAMP {ts}",
    'update': "UPDATE {t} USING TIMESTAMP {ts} SET v = 'new' WHERE pk = 1 AND ck = 0",
    'row_delete': 'DELETE FROM {t} USING TIMESTAMP {ts} WHERE pk = 1 AND ck = 0',
    'partition_delete': 'DELETE FROM {t} USING TIMESTAMP {ts} WHERE pk = 1',
    'range_delete': 'DELETE FROM {t} USING TIMESTAMP {ts} WHERE pk = 1 AND ck >= 0 AND ck < 5',
    'map_element': "UPDATE {t} USING TIMESTAMP {ts} SET m[1] = 'new' WHERE pk = 1 AND ck = 0",
    'set_add': 'UPDATE {t} USING TIMESTAMP {ts} SET st = st + {{2}} WHERE pk = 1 AND ck = 0',
    'static': 'UPDATE {t} USING TIMESTAMP {ts} SET s = 2 WHERE pk = 1',
}


async def one_write(p, pk=10) -> int:
    """One write after the backup. Returns its timestamp."""
    ts = p.backup.start_us + STEP_US
    await p.write(simple_rows([pk], ts))
    await p.covered()
    return ts


async def keys_at(p, t) -> set[int]:
    _, new = await p.restore(t)
    return await pks_of(p.cql, p.ks, new)


@needs_phase('1.3')
async def test_restore_keeps_write_at_exactly_t(manager: ScyllaClusterManager, s3_storage):
    """T11, RST-2."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        ts = await one_write(p)
        assert await keys_at(p, ts) == {10}


@needs_phase('1.3')
async def test_restore_drops_write_1us_above_t(manager: ScyllaClusterManager, s3_storage):
    """T11, RST-2."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        ts = await one_write(p)
        assert await keys_at(p, ts - 1) == set()


@needs_phase('1.3')
async def test_restore_keeps_write_1us_below_t(manager: ScyllaClusterManager, s3_storage):
    """T11, RST-2."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        ts = await one_write(p)
        assert await keys_at(p, ts + 1) == {10}


async def exact_around_a_change(manager, storage, kind, offset):
    seed = [W(old_ts(), SEED)]
    async with protected_table(manager, storage, seed=seed) as p:
        ts = p.backup.start_us + STEP_US
        history = seed + [W(ts, CHANGES[kind])]
        await p.write(history[1:])
        await p.covered()
        await p.assert_restored_exact(history, ts + offset)


@needs_phase('1.3')
@pytest.mark.parametrize('kind', CHANGES)
async def test_restore_is_exact_at_a_change(manager: ScyllaClusterManager, s3_storage, kind):
    """T11, RST-2."""
    await exact_around_a_change(manager, s3_storage, kind, 0)


@needs_phase('1.3')
@pytest.mark.parametrize('kind', CHANGES)
async def test_restore_is_exact_just_below_a_change(manager: ScyllaClusterManager, s3_storage, kind):
    """T11, RST-2."""
    await exact_around_a_change(manager, s3_storage, kind, -1)


@needs_phase('1.3')
async def test_tied_delete_and_insert_resolve_as_on_the_original(manager: ScyllaClusterManager, s3_storage):
    """T11, RST-2. The delete wins a tie."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        ts = p.backup.start_us + STEP_US
        history = [W(ts, "INSERT INTO {t} (pk, ck, v) VALUES (1, 0, 'tied') USING TIMESTAMP {ts}"),
                   W(ts, 'DELETE FROM {t} USING TIMESTAMP {ts} WHERE pk = 1 AND ck = 0')]
        await p.write(history)
        await p.covered()
        assert await p.assert_restored_exact(history, ts) == [], 'expected the delete to win the tie'


@needs_phase('1.3')
async def test_batch_touching_different_cells_is_split_by_t(manager: ScyllaClusterManager, s3_storage):
    """T11, RST-2."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        low, high = p.backup.start_us + STEP_US, p.backup.start_us + 2 * STEP_US
        await p.write([Batch([W(low, f"INSERT INTO {{t}} (pk, ck, v) VALUES (1, 0, 'low') USING TIMESTAMP {low}"),
                              W(high, f"INSERT INTO {{t}} (pk, ck, v) VALUES (2, 0, 'high') USING TIMESTAMP {high}")])])
        await p.covered()
        assert await keys_at(p, low) == {1}


@needs_phase('1.3')
@pytest.mark.parametrize('name, ck', [('own_cell', 3), ('own_collection_element', 4)])
async def test_batch_overwriting_itself_restores_only_the_merged_result(manager: ScyllaClusterManager, s3_storage, name, ck):
    """T34, RST-2. T between the batch's timestamps gives what was there before the batch, not the value it overwrote."""
    seed, _, _ = collection_edge_statements([1], old_ts(), 0)
    async with protected_table(manager, s3_storage, seed=seed) as p:
        _, changes, ts = collection_edge_statements([1], 0, p.backup.start_us)
        await p.write(changes)
        await p.covered()
        _, new = await p.restore(ts['low'])
        rows = [r for r in await read_exact(p.cql, p.ks, new) if r.startswith(f'(1, {ck},')]
        assert len(rows) == 1 and 'overwritten' not in rows[0], f'expected the value from before the batch, saw {rows}'


@needs_phase('1.3')
async def test_collection_overwrite_is_exact_1us_below_it(manager: ScyllaClusterManager, s3_storage):
    """T34, T11, RST-2."""
    seed, _, _ = collection_edge_statements([1], old_ts(), 0)
    async with protected_table(manager, s3_storage, seed=seed) as p:
        _, changes, ts = collection_edge_statements([1], 0, p.backup.start_us)
        await p.write(changes)
        await p.covered()
        await p.assert_restored_exact(seed + changes, ts['overwrite'] - 1)


@needs_phase('1.3')
@pytest.mark.parametrize('name, ck, stage, offset', [('empty_overwrite', 1, 'empty', -1), ('batched_delete', 2, 'delete', 0)])
async def test_ambiguous_collection_gives_done_and_one_of_two_values(manager: ScyllaClusterManager, s3_storage, name, ck, stage, offset):
    """T34, RST-2. Limit 3: at this exact T the collection may read as empty, or as it was."""
    seed, _, _ = collection_edge_statements([1], old_ts(), 0)
    async with protected_table(manager, s3_storage, seed=seed) as p:
        _, changes, ts = collection_edge_statements([1], 0, p.backup.start_us)
        await p.write(changes)
        await p.covered()
        _, new = await p.restore(ts[stage] + offset)
        rows = await p.cql.run_async(f'SELECT m FROM {p.ks}.{new} WHERE pk = 1 AND ck = {ck}')
        seen = _plain(rows[0][0]) if rows else 'no row'
        assert seen in (None, [(1, 'a'), (2, 'b')]), f'expected an empty collection or the seeded one, saw {seen}'


AMBIGUOUS_T = [('overwrite', 'overwrite', -1), ('empty_overwrite', 'empty', -1), ('batched_delete', 'delete', 0)]


@needs_phase('1.4')
@pytest.mark.parametrize('name, stage, offset', AMBIGUOUS_T)
async def test_result_counts_ambiguous_collections_as_not_verified(manager: ScyllaClusterManager, s3_storage, name, stage, offset):
    """T34, RST-7."""
    seed, _, _ = collection_edge_statements([1], old_ts(), 0)
    async with protected_table(manager, s3_storage, seed=seed) as p:
        _, changes, ts = collection_edge_statements([1], 0, p.backup.start_us)
        await p.write(changes)
        await p.covered()
        result, _ = await p.restore(ts[stage] + offset)
        found = NOT_VERIFIED_RE.search(result.notices)
        assert found and int(found.group(1)) == 1, f'expected "Not verified: 1 collections...", saw: {result.notices}'


@needs_phase('1.4')
@pytest.mark.parametrize('name, stage, offset', [(n, s, o + d) for n, s, o in AMBIGUOUS_T for d in (-1, 1)])
async def test_result_counts_nothing_1us_off_an_ambiguous_t(manager: ScyllaClusterManager, s3_storage, name, stage, offset):
    """T34, RST-7."""
    seed, _, _ = collection_edge_statements([1], old_ts(), 0)
    async with protected_table(manager, s3_storage, seed=seed) as p:
        _, changes, ts = collection_edge_statements([1], 0, p.backup.start_us)
        await p.write(changes)
        await p.covered()
        result, _ = await p.restore(ts[stage] + offset)
        assert not NOT_VERIFIED_RE.search(result.notices), f'expected nothing counted, saw: {result.notices}'


@needs_phase('1.3')
async def test_restore_at_interval_start_has_only_the_backup(manager: ScyllaClusterManager, s3_storage):
    """T11, T22, RST-2, REF-6."""
    async with protected_table(manager, s3_storage) as p:
        await one_write(p)
        await p.assert_restored_exact(p.seed, p.backup.start_us)


@needs_phase('1.3')
async def test_restore_at_interval_end_has_everything(manager: ScyllaClusterManager, s3_storage):
    """T11, T22, RST-2, REF-5."""
    async with protected_table(manager, s3_storage) as p:
        ts = p.backup.start_us + STEP_US
        history = p.seed + simple_rows([10], ts)
        await p.write(history[len(p.seed):])
        await p.assert_restored_exact(history, await p.covered())


@needs_phase('1.3')
async def test_no_writes_since_the_backup_restores_the_backup(manager: ScyllaClusterManager, s3_storage):
    """T12, RST-2."""
    async with protected_table(manager, s3_storage) as p:
        await p.assert_restored_exact(p.seed, await p.covered())


@needs_phase('1.3')
async def test_empty_table_at_the_backup_restores_empty(manager: ScyllaClusterManager, s3_storage):
    """T12, RST-2."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        _, new = await p.restore(await p.covered())
        seen = await read_all(p.cql, p.ks, new)
        assert seen == [], f'expected no rows, saw {seen}'


@needs_phase('1.3')
async def test_writes_after_an_empty_backup_are_restored(manager: ScyllaClusterManager, s3_storage):
    """T12, RST-2."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        later = simple_rows(range(10, 14), p.backup.start_us + STEP_US)
        await p.write(later)
        await p.assert_restored_exact(later, await p.covered())


@needs_phase('1.3')
async def test_other_table_with_the_same_keys_doesnt_leak(manager: ScyllaClusterManager, s3_storage):
    """T11, RST-2. The other table has the same key with its own value, and a key the source doesn't have."""
    async with protected_table(manager, s3_storage) as p:
        await p.cql.run_async(f'CREATE TABLE {p.ks}.other (pk int, ck int, v text, w int, s int static, m map<int, text>, st set<int>, PRIMARY KEY (pk, ck))')
        ts = p.backup.start_us + STEP_US
        await apply(p.cql, p.ks, 'other', simple_rows([1, 999], ts, 'OTHER'))
        await p.assert_restored_exact(p.seed, await p.covered())


@needs_phase('1.3')
async def test_mixed_history_on_every_shard_is_exact(manager: ScyllaClusterManager, object_storage):
    """T11, RST-2. The one end-to-end case the requirements ask for: every kind of change on keys on every shard, on each kind of store."""
    pks = list(range(8))
    seed = seed_statements(pks, old_ts())
    async with protected_table(manager, object_storage, seed=seed) as p:
        await assert_every_shard(p.cql, p.ks, p.cf, pks)
        changes, ts = change_statements(pks, p.backup.start_us)
        await p.write(changes)
        await p.covered()
        await p.assert_restored_exact(seed + changes, ts['static'])


async def restore_with_clients_writing(p):
    """Restores to T with writers on the source and another table. Returns the new table's name and the two writers."""
    await p.cql.run_async(f'CREATE TABLE {p.ks}.other (pk int, ck int, v text, PRIMARY KEY (pk, ck))')
    ts = p.backup.start_us + STEP_US
    await p.write(simple_rows([10], ts))
    await p.covered()
    source = Writer(p.cql, p.ks, p.cf, min_ts=ts + 1).start()
    other = Writer(p.cql, p.ks, 'other', min_ts=ts + 1).start()
    await source.wait_acked(10)
    await other.wait_acked(10)
    started = now_us()
    _, new = await p.restore(ts)
    done = now_us()
    await source.stop()
    await other.stop()
    for w in (source, other):
        assert w.failure is None, f'expected writes to keep working during a restore: {w.failure}'
        assert any(started <= ack <= done for _, ack in w.acked.values()), 'expected a client write acknowledged during the restore'
    return ts, new, source, other


@needs_phase('1.3')
async def test_restore_during_client_writes_is_exact(manager: ScyllaClusterManager, object_storage):
    """T10, RST-1."""
    async with protected_table(manager, object_storage) as p:
        ts, new, _, _ = await restore_with_clients_writing(p)
        await assert_exact(p.cql, p.ks, new, p.seed + simple_rows([10], ts), ts)


@needs_phase('1.3')
async def test_source_keeps_writes_made_during_a_restore(manager: ScyllaClusterManager, s3_storage):
    """T10, RST-1."""
    async with protected_table(manager, s3_storage) as p:
        _, _, source, _ = await restore_with_clients_writing(p)
        missing = set(source.acked) - await pks_of(p.cql, p.ks, p.cf)
        assert not missing, f'expected the source to keep its writes, missing {sorted(missing)[:10]}'


@needs_phase('1.3')
async def test_other_table_keeps_writes_made_during_a_restore(manager: ScyllaClusterManager, s3_storage):
    """T10, RST-1."""
    async with protected_table(manager, s3_storage) as p:
        _, _, _, other = await restore_with_clients_writing(p)
        missing = set(other.acked) - await pks_of(p.cql, p.ks, 'other')
        assert not missing, f'expected the other table to keep its writes, missing {sorted(missing)[:10]}'


@needs_phase('1.3')
@pytest.mark.parametrize('index, offset', [(0, 0), (1, -1), (1, 0)], ids=['first_write', 'just_below_second_write', 'second_write'])
async def test_table_with_conditional_writes_is_exact_at_t(manager: ScyllaClusterManager, s3_storage, index, offset):
    """T29, REF-1. LWT isn't a refused kind. Depends on PM-36, the team lead's safety check."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        history = await conditional_history(p.cql, p.ks)
        await p.covered()
        await p.assert_restored_exact(history, history[index].ts + offset)


@needs_phase('1.3')
async def test_new_table_has_the_schema_at_the_backup(manager: ScyllaClusterManager, s3_storage):
    """T15, RST-6. Non-default options, a UDT, a static and a dropped column."""
    server = await manager.server_add(config=node_config(s3_storage))
    await new_table_schema_case(manager, server, s3_storage, alter_after_t=False)


@needs_phase('1.3')
async def test_alter_after_t_leaves_the_new_table_with_the_backups_schema(manager: ScyllaClusterManager, s3_storage):
    """T15, RST-6, REF-9."""
    server = await manager.server_add(config=node_config(s3_storage))
    await new_table_schema_case(manager, server, s3_storage, alter_after_t=True)


async def new_table_schema_case(manager, server, storage, alter_after_t):
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        describe, rows = await create_rich_table(cql, ks)
        backup = await protect_table(manager, server, storage, ks, 'src')
        t = await wait_covered(manager, server, ks, 'src')
        if alter_after_t:
            await cql.run_async(f'ALTER TABLE {ks}.src ADD later int')
            await wait_covered(manager, server, ks, 'src')
        await restore_ok(manager, server, ks, 'src', backup.name, t, 'restored')
        restored = (await cql.run_async(f'DESCRIBE TABLE {ks}.restored'))[0].create_statement
        assert restored.replace(f'{ks}.restored', f'{ks}.src') == describe, f'expected:\n{describe}\nsaw:\n{restored}'
        seen = await read_all(cql, ks, 'restored')
        assert seen == rows, f'expected rows {rows}, saw {seen}'


# What the result says

async def finished_restore(p):
    """A restore with a write on each side of T, and bytes from another table in the window. Returns the result, T and the new table."""
    await p.cql.run_async(f'CREATE TABLE {p.ks}.other (pk int, ck int, v text, PRIMARY KEY (pk, ck))')
    ts = p.backup.start_us + STEP_US
    await p.write(simple_rows([10], ts) + simple_rows([11], ts + STEP_US))
    await apply(p.cql, p.ks, 'other', simple_rows([1], ts))
    await p.covered()
    result, new = await p.restore(ts)
    return result, ts, new


@needs_phase('1.4')
async def test_result_states_t_and_backup(manager: ScyllaClusterManager, s3_storage):
    """T10, RST-7."""
    async with protected_table(manager, s3_storage) as p:
        result, ts, _ = await finished_restore(p)
        assert (result.t_us, result.backup) == (ts, p.backup.name), result


@needs_phase('1.4')
async def test_result_states_objects_and_bytes_read(manager: ScyllaClusterManager, s3_storage):
    """T10, RST-7."""
    async with protected_table(manager, s3_storage) as p:
        result, _, _ = await finished_restore(p)
        assert result.objects_read > 0 and result.bytes_read > 0, result


@needs_phase('1.4')
async def test_result_counts_cells_applied_and_dropped(manager: ScyllaClusterManager, s3_storage):
    """T10, T11, RST-7."""
    async with protected_table(manager, s3_storage) as p:
        result, _, _ = await finished_restore(p)
        assert result.cells_applied > 0 and result.cells_dropped > 0, result


@needs_phase('1.4')
async def test_result_counts_no_cells_dropped_when_none_are_above_t(manager: ScyllaClusterManager, s3_storage):
    """T11, RST-7."""
    async with protected_table(manager, s3_storage) as p:
        await p.write(simple_rows([10], p.backup.start_us + STEP_US))
        result, _ = await p.restore(await p.covered())
        assert result.cells_dropped == 0, result


@needs_phase('1.4')
async def test_result_flags_an_empty_backup(manager: ScyllaClusterManager, s3_storage):
    """T12, RST-7."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        result, _ = await p.restore(await p.covered())
        assert result.cells_applied == 0 and FLAG_EMPTY_BACKUP in result.flags, result


@needs_phase('1.4')
async def test_result_doesnt_flag_a_backup_with_data(manager: ScyllaClusterManager, s3_storage):
    """T12, RST-7."""
    async with protected_table(manager, s3_storage) as p:
        result, _ = await p.restore(await p.covered())
        assert FLAG_EMPTY_BACKUP not in result.flags, result


@needs_phase('1.4')
async def test_result_doesnt_flag_an_empty_backup_with_later_writes(manager: ScyllaClusterManager, s3_storage):
    """T12, RST-7."""
    async with protected_table(manager, s3_storage, seed=[]) as p:
        await p.write(simple_rows([10], p.backup.start_us + STEP_US))
        result, _ = await p.restore(await p.covered())
        assert result.cells_applied > 0 and FLAG_EMPTY_BACKUP not in result.flags, result


@needs_phase('1.4')
@pytest.mark.parametrize('limit', RESULT_LIMIT_WORDS)
async def test_result_states_limit(manager: ScyllaClusterManager, s3_storage, limit):
    """T10, RST-1, RST-7."""
    async with protected_table(manager, s3_storage) as p:
        result, _, _ = await finished_restore(p)
        assert RESULT_LIMIT_WORDS[limit] in result.notices.lower(), f'expected {RESULT_LIMIT_WORDS[limit]!r} for {limit}, saw: {result.notices}'


@needs_phase('1.4')
async def test_result_warns_about_the_wrong_way_back(manager: ScyllaClusterManager, s3_storage):
    """T10, RTN-1."""
    async with protected_table(manager, s3_storage) as p:
        result, _, _ = await finished_restore(p)
        assert WRONG_WAY_BACK in result.notices, f'expected the warning word for word, saw: {result.notices}'


@needs_phase('1.4')
async def test_result_says_this_is_a_copy(manager: ScyllaClusterManager, s3_storage):
    """T10, RTN-1."""
    async with protected_table(manager, s3_storage) as p:
        result, _, _ = await finished_restore(p)
        missing = [word for word in COPY_NOTICE_WORDS if word not in result.notices.lower()]
        assert not missing, f'expected {missing}, saw: {result.notices}'


@needs_phase('1.4')
async def test_result_commands_protect_the_new_table(manager: ScyllaClusterManager, s3_storage):
    """T10, RTN-1."""
    async with protected_table(manager, s3_storage) as p:
        result, _, new = await finished_restore(p)
        await run_status_commands(manager, p.server, result.commands)
        await wait_protected(manager, p.server, p.ks, new)


@needs_phase('1.4')
async def test_result_reports_each_step(manager: ScyllaClusterManager, s3_storage):
    """T10, LVL-4."""
    async with protected_table(manager, s3_storage) as p:
        result, _, _ = await finished_restore(p)
        assert set(result.steps) == set(STEPS), result
        assert all(s.seconds >= 0 and s.bytes >= 0 for s in result.steps.values()), result


@needs_phase('1.4')
async def test_result_steps_sum_to_the_total(manager: ScyllaClusterManager, s3_storage):
    """T10, LVL-4."""
    async with protected_table(manager, s3_storage) as p:
        result, _, _ = await finished_restore(p)
        total = sum(s.seconds for s in result.steps.values())
        assert abs(total - result.total_seconds) <= 0.01 * result.total_seconds, f'expected {result.total_seconds} within 1%, steps sum to {total}'


@needs_phase('1.4')
async def test_result_reports_bytes_read_for_other_tables(manager: ScyllaClusterManager, s3_storage):
    """T10, LVL-4."""
    async with protected_table(manager, s3_storage) as p:
        result, _, _ = await finished_restore(p)
        assert result.other_tables_bytes > 0, result
