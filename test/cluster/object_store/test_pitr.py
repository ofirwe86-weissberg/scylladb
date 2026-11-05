#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Black-box tests of single-node PITR, written from docs-work/requirements-must.md (issue #22).

These use only what an operator has: CQL, REST, nodetool, configuration, the bucket, and starting or
killing the node. There's no error injection. The test IDs in the docstrings match docs-work/test-plan.md.

Each test is skipped until the phase it needs has landed: add the phase to LANDED_PHASES to enable it. A
skipped test proves nothing: none of these has run, so none has been seen to fail or pass.
"""

import asyncio
import json
import logging
import random
import re
import shlex
import time
import uuid

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple

import boto3
import pytest

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.object_store.test_backup import do_backup, take_snapshot_on_one_server
from test.cluster.util import new_test_keyspace, reconnect_driver, wait_for_cql_and_get_hosts
from test.pylib.rest_client import HTTPError
from test.pylib.s3mock_server import create_bucket, create_conf
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name, wait_for

logger = logging.getLogger(__name__)

# The build layers from the requirements (1.1 ship, 1.2 backup and status, 1.3 correct restore, 1.4 result and finish).
# Add a phase here when it lands, to enable the tests that need it.
LANDED_PHASES: tuple[str, ...] = ()


def landed(phase: str) -> bool:
    return phase in LANDED_PHASES


def needs_phase(phase: str):
    """Skips a test until its phase lands. A typed skip, because test.py reports a bare pytest.mark.skip as an error."""
    if landed(phase):
        return lambda test: test
    return pytest.mark.skip_bug(link="https://github.com/ofirwe86-weissberg/scylladb/issues/22",
                                reason=f"PITR phase {phase} hasn't landed; the surface in the adapter section is only proposed")


@pytest.fixture(autouse=True)
async def report_stalls(manager):
    """Report only: a stall never fails a test here, and passing says nothing about stalls on worst-case inputs."""
    yield
    for server in await manager.all_servers():
        try:
            log = await manager.server_open_log(server.server_id)
            stalls = [int(m.group(1)) for _, m in await log.grep(r'Reactor stalled for (\d+) ms')]
            logger.info(f'Server {server.server_id}: {len(stalls)} reactor stalls, longest {max(stalls, default=0)} ms')
        except Exception as e:
            logger.info(f'Could not read stalls from server {server.server_id}: {e}')


####################################################################################################
# ADAPTER: every assumption about the feature's surface is here, and nothing below names any of it.
# None of this is defined yet. It's the surface QA proposes for the design (docs-work/test-plan.md, section 5).
# A rename changes this section only.
####################################################################################################

SHIPPING_PREFIX = 'pitr-log'  # all shipped objects are assumed to be under this prefix
MUTABLE_OBJECTS = ('head',)   # last component of the shipped objects the product overwrites. All others are immutable
BACKUP_PREFIX = 'pitr-backups'  # where a backup must go to protect a table: "the shipping store" in SHP-5

CFG_ENDPOINT = 'pitr_shipping_endpoint'  # setting this one turns shipping on
CFG_BUCKET = 'pitr_shipping_bucket'
CFG_PREFIX = 'pitr_shipping_prefix'
CFG_SEAL_SECONDS = 'pitr_seal_interval_seconds'
CFG_OUTAGE_LIMIT_BYTES = 'pitr_outage_limit_bytes'
CFG_DISK_RESERVE_PERCENT = 'pitr_disk_reserve_percent'  # assumed live-updatable

STATUS_URI = '/storage_service/pitr/status'  # GET
RESTORE_URI = '/storage_service/pitr/restore'  # POST, returns a task manager id, or 400 with a refusal as JSON
RESULT_URI = '/storage_service/pitr/restore_result'  # GET, durable, survives a restart

PROTECTED = 'protected'
NOT_PROTECTED = 'not protected'
INCOMPLETE = 'incomplete'
DONE = 'done'
FAILED = 'failed'
CAUSE_RESTART = 'restart'
CAUSE_OUTAGE = 'outage'
FLAG_EMPTY_BACKUP = 'empty_backup'
STEPS = ('check', 'fetch', 'load', 'replay', 'flush')


class Reason:
    """Stable reason codes, for status (table) and for refusals (restore)."""
    # status
    BROKEN = 'protection_broken'
    RESTORED_TABLE = 'restored_table'
    # table kinds: status and refusal
    TABLETS = 'table_uses_tablets'
    STRONG = 'table_strongly_consistent'
    COUNTERS = 'table_has_counters'
    VIEW_BASE = 'table_has_view'
    INDEXED = 'table_has_index'
    VIEW_TABLE = 'table_is_view'
    INDEX_TABLE = 'table_is_index'
    CDC = 'table_uses_cdc'
    DURABLE = 'durable_writes_false'
    LOGSTOR = 'table_uses_logstor'
    SYSTEM = 'table_is_system'
    ALTERNATOR = 'table_is_alternator'
    # refusals
    NODES = 'more_than_one_node'
    RF = 'replication_factor_not_1'
    OBJECT_MISSING = 'shipped_object_missing'
    OBJECT_DAMAGED = 'shipped_object_damaged'
    RECORDS_MISSING = 'protection_records_missing'
    RECORDS_DAMAGED = 'protection_records_damaged'
    BACKUP_BEFORE_SHIPPING = 'backup_before_shipping'
    BACKUP_NOT_USABLE = 'backup_not_usable'
    T_PAST_COVERAGE = 't_past_coverage'
    T_BEFORE_COVERAGE = 't_before_coverage'
    NO_SUCH_BACKUP = 'no_such_backup'
    NO_BACKUP = 'no_backup'
    BACKUP_UNFINISHED = 'backup_upload_unfinished'
    SCHEMA_CHANGED = 'schema_changed_in_window'
    TRUNCATED = 'truncate_in_window'
    T_INVALID = 't_invalid'
    DISK = 'disk_reserve'
    NAME_TAKEN = 'new_table_name_taken'
    NAME_INVALID = 'new_table_name_invalid'
    NAME_OTHER_KEYSPACE = 'new_table_other_keyspace'


# Words that show a limit or a next action is stated. Lowercase; each is looked for in the lowercased text.
STATUS_LIMIT_WORDS = ['experimental', 'vnodes', 'tablets']
RESULT_LIMIT_WORDS = {'limit 3': 'clocks', 'limit 4': 'ttl', 'limit 5': 'newer than t', 'limit 6': "isn't checked",
                      'limit 15': 'refresh', 'limit 8': 'truncated'}
WRONG_WAY_BACK = ("Don't load this copy's files into the original unless it's been truncated: "
                  "later deletes in the original still win, and rows stay missing with no error.")
COPY_NOTICE_WORDS = ['copy', 'production', 'route back', 'own backup']
SHIPPING_OFF_WORDS = ['node']
TOPOLOGY_LIMIT_WORDS = {Reason.NODES: 'one node', Reason.RF: 'rf=1'}
PAST_COVERAGE_WORDS = ['lag']
NEWER_BACKUP_WORDS = ['newer backup', 'earlier']
UNFINISHED_BACKUP_WORDS = ['finish']
INCOMPLETE_ACTION_WORDS = ['drop', 'new name']


@dataclass
class Backup:
    name: str
    start_us: int
    end_us: int


@dataclass
class TableStatus:
    keyspace: str
    table: str
    state: str
    reason: str | None
    commands: list[str]         # exact commands that restore protection
    restart_required: bool
    break_cause: str | None
    break_time_us: int | None
    backups: list[Backup]
    latest_us: int | None       # latest restorable time, including after a break
    lag_seconds: float | None
    step: str | None            # for an incomplete restore
    text: str                   # everything the entry says, for checking a next action is stated


@dataclass
class Status:
    shipping_on: bool
    off_reason: str
    store_reachable: bool
    store_error: str
    limits_text: str
    tables: list[TableStatus]


@dataclass
class Step:
    seconds: float
    bytes: int


@dataclass
class Result:
    state: str
    reason: str | None = None
    message: str = ''
    notices: str = ''           # the result's limits and warnings as text
    commands: list[str] = field(default_factory=list)
    t_us: int | None = None
    backup: str | None = None
    objects_read: int | None = None
    bytes_read: int | None = None
    cells_applied: int | None = None
    cells_dropped: int | None = None
    flags: list[str] = field(default_factory=list)
    steps: dict[str, Step] = field(default_factory=dict)
    total_seconds: float | None = None
    other_tables_bytes: int | None = None
    needed_bytes: int | None = None
    free_bytes: int | None = None
    reserve_bytes: int | None = None
    remaining_start_us: int | None = None     # after a gap: the interval that's still restorable, or None if none remains
    remaining_end_us: int | None = None
    raw: Any = None


def backup_prefix(ks, cf, snap) -> str:
    return f'{BACKUP_PREFIX}/{ks}/{cf}/{snap}'


def shipping_options(storage, seal_seconds=1, **extra) -> dict:
    return {CFG_ENDPOINT: storage.address, CFG_BUCKET: storage.bucket_name, CFG_PREFIX: SHIPPING_PREFIX,
            CFG_SEAL_SECONDS: seal_seconds} | extra


def _parse_status(j) -> Status:
    tables = []
    for t in j.get('tables', []):
        brk = t.get('break') or {}
        tables.append(TableStatus(
            keyspace=t['keyspace'], table=t['table'], state=t['state'], reason=t.get('reason'),
            commands=t.get('backup_commands', []), restart_required=t.get('restart_required', False),
            break_cause=brk.get('cause'), break_time_us=from_iso(brk['time']) if brk.get('time') else None,
            backups=[Backup(b['name'], from_iso(b['start']), from_iso(b['end'])) for b in t.get('backups', [])],
            latest_us=from_iso(t['latest_restorable']) if t.get('latest_restorable') else None,
            lag_seconds=t.get('lag_seconds'), step=t.get('step'), text=str(t)))
    store = j.get('store', {})
    return Status(shipping_on=j['shipping'] == 'on', off_reason=j.get('off_reason', ''), store_reachable=store.get('reachable', False),
                  store_error=store.get('error', ''), limits_text=' '.join(j.get('limits', [])), tables=tables)


def _parse_result(j) -> Result:
    remaining = j.get('remaining_interval') or {}
    return Result(
        remaining_start_us=from_iso(remaining['start']) if remaining else None,
        remaining_end_us=from_iso(remaining['end']) if remaining else None,
        state=j['state'], reason=j.get('reason'), message=j.get('message', ''), notices='\n'.join(j.get('notices', [])),
        commands=j.get('backup_commands', []), t_us=from_iso(j['t']) if j.get('t') else None, backup=j.get('backup'),
        objects_read=j.get('objects_read'), bytes_read=j.get('bytes_read'), cells_applied=j.get('cells_applied'),
        cells_dropped=j.get('cells_dropped_after_t'), flags=j.get('flags', []),
        steps={k: Step(v['seconds'], v['bytes']) for k, v in j.get('steps', {}).items()}, total_seconds=j.get('total_seconds'),
        other_tables_bytes=j.get('other_tables_bytes'), needed_bytes=j.get('needed_bytes'), free_bytes=j.get('free_bytes'),
        reserve_bytes=j.get('reserve_bytes'), raw=j)


async def get_status(manager, server) -> Status:
    return _parse_status(await manager.api.client.get_json(STATUS_URI, host=server.ip_addr))


async def start_restore(manager, server, ks, cf, backup, t, new_table, new_keyspace=None) -> str | Result:
    """Returns a task id, or the Result of a refusal made at once. T is microseconds, or a string to pass as it is."""
    params = {'keyspace': ks, 'table': cf, 'backup': backup, 'time': iso(t) if isinstance(t, int) else t, 'new_table': new_table}
    if new_keyspace:
        params['new_keyspace'] = new_keyspace
    try:
        return await manager.api.client.post_json(RESTORE_URI, host=server.ip_addr, params=params)
    except HTTPError as e:
        assert e.code == 400, f'a refusal must not look like a server error: {e}'
        return _parse_result(json.loads(e.message))


async def get_result(manager, server, ks, new_table) -> Result:
    return _parse_result(await manager.api.client.get_json(RESULT_URI, host=server.ip_addr, params={'keyspace': ks, 'new_table': new_table}))

####################################################################################################
# End of the adapter.
####################################################################################################

VNODES_KS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}"
DEADLINE_SECONDS = 300
# The VM's clock steps by up to about 150 ms (docs-work/baseline.md:115-116), so a comparison between a time
# measured here and one reported by the node has this margin.
CLOCK_MARGIN_US = 1_000_000
STEP_US = 1000
# Absence has no event to poll for, so a check for it watches the bucket for this long: 3 seal intervals, at S = 1 s.
ABSENCE_WINDOW_SECONDS = 3
THROTTLE_BYTES_PER_SECOND = 128 * 1024
KILL_ATTEMPTS = 5
SHARDS = 2                      # test/pylib/scylla_server.py:153
SEAL_SECONDS = 1                # the default of shipping_options()
IDLE_WINDOW_US = 10_000_000
IGNORE_MSB_BITS = 12            # the default of murmur3_partitioner_ignore_msb_bits
TABLE_SCHEMA = '(pk int, ck int, v text, w int, s int static, m map<int, text>, st set<int>, PRIMARY KEY (pk, ck))'
EXACT_COLUMNS = ('pk, ck, v, w, s, m, st, WRITETIME(v), WRITETIME(w), WRITETIME(s), '
                 'WRITETIME(m[1]), WRITETIME(m[2]), WRITETIME(st[1]), WRITETIME(st[2])')
ISO_RE = re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z')
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def iso(us: int) -> str:
    return (EPOCH + timedelta(microseconds=us)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def from_iso(text: str) -> int:
    return (datetime.fromisoformat(text.replace('Z', '+00:00')) - EPOCH) // timedelta(microseconds=1)


def iso_times(text: str) -> list[int]:
    return [from_iso(m) for m in ISO_RE.findall(text)]


def now_us() -> int:
    return time.time_ns() // 1000


def deadline() -> float:
    return time.time() + DEADLINE_SECONDS


def node_config(storage, shipping=True, **extra) -> dict:
    cfg = {'object_storage_endpoints': storage.create_endpoint_conf(), 'task_ttl_in_seconds': 300}
    if shipping:
        cfg |= shipping_options(storage)
    return cfg | extra


def table_entry(status: Status, ks, cf) -> TableStatus:
    found = [t for t in status.tables if (t.keyspace, t.table) == (ks, cf)]
    assert len(found) == 1, f'{ks}.{cf} must have one answer, got {found}'
    return found[0]


async def table_status(manager, server, ks, cf) -> TableStatus:
    return table_entry(await get_status(manager, server), ks, cf)


async def backup_table(manager, server, storage, ks, cf) -> str:
    snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
    await do_backup(server, snap, backup_prefix(ks, cf, snap), ks, cf, storage, manager, logger)
    return snap


async def wait_protected(manager, server, ks, cf, name=None) -> Backup:
    """Waits for the table to be protected by the named backup, or by any backup. Returns the backup."""
    async def protected():
        t = await table_status(manager, server, ks, cf)
        if t.state != PROTECTED:
            return None
        found = [b for b in t.backups if name is None or b.name == name]
        return found[-1] if found else None
    return await wait_for(protected, deadline())


async def protect_table(manager, server, storage, ks, cf) -> Backup:
    return await wait_protected(manager, server, ks, cf, await backup_table(manager, server, storage, ks, cf))


async def wait_covered(manager, server, ks, cf, us=None) -> int:
    """Waits until the latest restorable time reaches a time, by default a little after now, so that
    every write acknowledged before this call is covered. Returns the latest restorable time."""
    target = us if us is not None else now_us() + CLOCK_MARGIN_US
    async def covered():
        t = await table_status(manager, server, ks, cf)
        return t.latest_us if t.latest_us is not None and t.latest_us >= target else None
    return await wait_for(covered, deadline())


async def run_restore(manager, server, ks, cf, backup, t, new_table, new_keyspace=None) -> Result:
    started = await start_restore(manager, server, ks, cf, backup, t, new_table, new_keyspace)
    if isinstance(started, Result):
        return started
    await manager.api.wait_task(server.ip_addr, started)
    return await get_result(manager, server, new_keyspace or ks, new_table)


async def restore_ok(manager, server, ks, cf, backup, t, new_table) -> Result:
    result = await run_restore(manager, server, ks, cf, backup, t, new_table)
    assert result.state == DONE, f'restore to {t} must succeed, got {result}'
    return result


async def read_all(cql, ks, table) -> list[str]:
    # CL ONE: the session's default is LOCAL_QUORUM, which a single node can't meet at RF above 1.
    statement = SimpleStatement(f'SELECT * FROM {ks}.{table}', consistency_level=ConsistencyLevel.ONE)
    return sorted(repr(tuple(r)) for r in await cql.run_async(statement))


async def table_exists(cql, ks, table) -> bool:
    rows = await cql.run_async(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{table}'")
    return len(list(rows)) > 0


async def assert_refused(manager, server, cql, ks, cf, backup, t, reason, watched=(), new_table=None, new_keyspace=None) -> Result:
    """Checks the refusal rules in the requirements' Terms. `watched` are other tables that must read the same."""
    if new_table is None:
        new_table = unique_name('restored_')
    expect_absent = new_keyspace is None and not await table_exists(cql, ks, new_table)
    tables = [(ks, cf), *[(ks, w) for w in watched]] if not ks.startswith('system') else []
    before = {w: await read_all(cql, *w) for w in tables}
    result = await run_restore(manager, server, ks, cf, backup, t, new_table, new_keyspace)
    assert result.state == FAILED, f'must be refused: {result}'
    assert result.reason in ({reason} if isinstance(reason, str) else reason), f'expected reason {reason}: {result}'
    assert result.message, f'a refusal states the reason and next action: {result}'
    assert not result.cells_applied, f'refusal happens before any data is loaded: {result}'
    for w, rows in before.items():
        assert await read_all(cql, *w) == rows, f'{w} changed in a refusal'
    if expect_absent:
        assert not await table_exists(cql, ks, new_table), f'a refusal leaves no new table: {result}'
    return result


# Statements, and the independent answer for "exact at T".

class W(NamedTuple):
    ts: int
    cql: str


class Batch(NamedTuple):
    parts: list[W]


async def apply(cql, ks, table, statements, t=None):
    """Applies statements in order. With T, it applies them minus every one with a timestamp above T,
    splitting batches, which is the requirements' definition of exact at T."""
    def text(w):
        return w.cql.format(t=f'{ks}.{table}', ts=w.ts)
    for s in statements:
        if isinstance(s, Batch):
            parts = [w for w in s.parts if t is None or w.ts <= t]
            if t is None:
                await cql.run_async('BEGIN UNLOGGED BATCH ' + '; '.join(text(w) for w in parts) + ' APPLY BATCH')
            else:
                for w in parts:
                    await cql.run_async(text(w))
        elif t is None or s.ts <= t:
            await cql.run_async(text(s))


async def read_exact(cql, ks, table) -> list[str]:
    return sorted(repr(tuple(r)) for r in await cql.run_async(f'SELECT {EXACT_COLUMNS} FROM {ks}.{table}'))


async def expected_at(cql, ks, statements, t, table_options='') -> list[str]:
    """Builds a reference table now, after the restore it's compared with, so it can't be in the history
    that restore read."""
    ref = unique_name('ref_')
    await cql.run_async(f'CREATE TABLE {ks}.{ref} {TABLE_SCHEMA} {table_options}')
    await apply(cql, ks, ref, statements, t)
    return await read_exact(cql, ks, ref)


async def assert_exact(cql, ks, restored, statements, t) -> list[str]:
    expected = await expected_at(cql, ks, statements, t)
    actual = await read_exact(cql, ks, restored)
    assert actual == expected, f'{restored} is not exact at {iso(t)}'
    return expected


def seed_statements(pks, ts) -> list[W]:
    """What's in the backup: rows for each later kind of change to act on."""
    out = []
    for pk in pks:
        for ck in (0, 2, 10, 11, 25):
            out.append(W(ts, f"INSERT INTO {{t}} (pk, ck, v, w) VALUES ({pk}, {ck}, 'seed', {ck}) USING TIMESTAMP {{ts}}"))
        out.append(W(ts, f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET s = 1, m = {{{{1: 'a', 2: 'b'}}}}, st = {{{{1}}}} WHERE pk = {pk} AND ck = 0"))
        out.append(W(ts, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk + 100}, 0, 'seed') USING TIMESTAMP {{ts}}"))
    return out


def change_statements(pks, base) -> tuple[list, dict[str, int]]:
    """Every kind of change the requirements name, on every key, one stage per timestamp."""
    stages = ['insert', 'update', 'row_delete', 'partition_delete', 'range_delete', 'map_element', 'map_overwrite', 'set_add',
              'static', 'batch_low', 'batch_high', 'tie', 'last']
    ts = {name: base + (i + 1) * STEP_US for i, name in enumerate(stages)}
    out = []
    for pk in pks:
        out.append(W(ts['insert'], f"INSERT INTO {{t}} (pk, ck, v, w) VALUES ({pk}, 1, 'new', 1) USING TIMESTAMP {{ts}}"))
        out.append(W(ts['update'], f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET v = 'updated' WHERE pk = {pk} AND ck = 0"))
        out.append(W(ts['row_delete'], f"DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {pk} AND ck = 2"))
        out.append(W(ts['partition_delete'], f"DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {pk + 100}"))
        out.append(W(ts['range_delete'], f"DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {pk} AND ck >= 10 AND ck < 20"))
        out.append(W(ts['map_element'], f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET m[1] = 'changed' WHERE pk = {pk} AND ck = 0"))
        out.append(W(ts['map_overwrite'], f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET m = {{{{2: 'over'}}}} WHERE pk = {pk} AND ck = 0"))
        out.append(W(ts['set_add'], f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET st = st + {{{{2}}}} WHERE pk = {pk} AND ck = 0"))
        out.append(W(ts['static'], f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET s = 2 WHERE pk = {pk}"))
        out.append(Batch([W(ts['batch_low'], f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 20, 'low') USING TIMESTAMP {ts['batch_low']}"),
                          W(ts['batch_high'], f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 21, 'high') USING TIMESTAMP {ts['batch_high']}")]))
        out.append(W(ts['tie'], f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 30, 'tied') USING TIMESTAMP {{ts}}"))
        out.append(W(ts['tie'], f"DELETE FROM {{t}} USING TIMESTAMP {{ts}} WHERE pk = {pk} AND ck = 30"))
        out.append(W(ts['last'], f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET w = 99 WHERE pk = {pk} AND ck = 25"))
    return out, ts


def simple_rows(pks, ts, value='x') -> list[W]:
    return [W(ts, f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, 0, '{value}') USING TIMESTAMP {{ts}}") for pk in pks]


def shard_of(token: int) -> int:
    # dht/token.cc:120-122
    t = ((token + 2**63) << IGNORE_MSB_BITS) & (2**64 - 1)
    return (t * SHARDS) >> 64


async def assert_every_shard(cql, ks, cf, pks):
    rows = await cql.run_async(f"SELECT pk, token(pk) FROM {ks}.{cf}")
    shards = {shard_of(r[1]) for r in rows if r[0] in pks}
    assert shards == set(range(SHARDS)), f'the keys must reach every shard, they reach {shards}'


class Writer:
    """Keeps writing from outside, and records what was acknowledged and when."""
    def __init__(self, cql, ks, cf, min_ts=0, first_pk=1000, payload=''):
        self.cql, self.ks, self.cf, self.min_ts, self.next_pk, self.payload = cql, ks, cf, min_ts, first_pk, payload
        self.attempted: dict[int, int] = {}
        self.acked: dict[int, tuple[int, int]] = {}  # pk -> (write timestamp, time of the acknowledgement)
        self.failure: Exception | None = None
        self.stopping = False
        self.task = None

    def start(self):
        self.task = asyncio.create_task(self._run())
        return self

    async def _run(self):
        while not self.stopping:
            pk, self.next_pk = self.next_pk, self.next_pk + 1
            ts = max(now_us(), self.min_ts)
            self.attempted[pk] = ts
            try:
                await self.cql.run_async(f"INSERT INTO {self.ks}.{self.cf} (pk, ck, v) VALUES ({pk}, 0, 'bg{self.payload}') USING TIMESTAMP {ts}")
            except Exception as e:
                self.failure = e
                return
            self.acked[pk] = (ts, now_us())

    async def stop(self):
        self.stopping = True
        await self.task

    async def wait_acked(self, count):
        async def enough():
            # wait_for() swallows an assertion, so a failure ends the wait and is checked after it.
            return True if self.failure is not None or len(self.acked) >= count else None
        await wait_for(enough, deadline())
        assert self.failure is None, f'a write failed: {self.failure}'


async def pks_of(cql, ks, table) -> set[int]:
    return {r[0] for r in await cql.run_async(f'SELECT DISTINCT pk FROM {ks}.{table}')}


async def restart(manager, server, kill=False, while_down=None):
    if kill:
        await manager.server_stop(server.server_id, False)
        if while_down:
            while_down()
        await manager.server_start(server.server_id)
    else:
        await manager.server_restart(server.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
    return cql


async def run_nodetool(manager, server, argv):
    exe = await manager.server_get_exe(server.server_id)
    proc = await asyncio.create_subprocess_exec(exe, *argv, '--host', server.ip_addr,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    assert proc.returncode == 0, f'{argv} failed: {out} {err}'


async def run_status_commands(manager, server, commands):
    """Runs the commands status gave, unchanged, with real nodetool. That's what makes them exact."""
    assert commands, 'status must give the exact commands'
    for command in commands:
        assert '<' not in command, f'not an exact command: {command}'
        argv = shlex.split(command)
        assert argv[0] == 'nodetool', command
        await run_nodetool(manager, server, argv)


async def assert_acked_writes_present(cql, ks, table, writers, t, seeded) -> dict[int, int]:
    """The promise in SHP-7: no T gives done while missing writes acknowledged before coverage. A write
    acknowledged after coverage, or never, may be present or absent. Returns the writes that had to be there."""
    rows = await cql.run_async(f'SELECT pk, WRITETIME(v) AS writetime_v FROM {ks}.{table}')
    present = {r.pk: r.writetime_v for r in rows}
    assert set(seeded) <= set(present), f'rows missing at {iso(t)}: {set(seeded) - set(present)}'
    attempted = {pk: ts for w in writers for pk, ts in w.attempted.items()}
    required = {pk: ts for w in writers for pk, (ts, ack) in w.acked.items() if ts <= t and ack + CLOCK_MARGIN_US < t}
    missing = {pk for pk, ts in required.items() if present.get(pk) != ts}
    assert not missing, f'{len(missing)} acknowledged writes are missing at {iso(t)}: {sorted(missing)[:10]}'
    for pk, ts in present.items():
        assert pk in seeded or (attempted.get(pk) == ts and ts <= t), f'unexpected row {pk} at {ts}'
    return required


async def assert_nothing_appears(storage, allowed_prefixes=('sstables/',)):
    """Absence has no event to poll for, so this watches the bucket for a bounded time, and fails at once if anything appears."""
    end = time.time() + ABSENCE_WINDOW_SECONDS
    while True:
        keys = await objects_of(storage, '')
        unexpected = [k for k in keys if not k.startswith(tuple(allowed_prefixes))]
        assert not unexpected, f'nothing else may be in the store: {unexpected}'
        if time.time() >= end:
            return
        await asyncio.sleep(0.2)


async def assert_stays_unprotected(manager, server, ks, cf) -> TableStatus:
    """As above, for status: a backup that doesn't protect mustn't start to later."""
    end = time.time() + ABSENCE_WINDOW_SECONDS
    while True:
        entry = await table_status(manager, server, ks, cf)
        assert entry.state == NOT_PROTECTED and not entry.backups, entry
        if time.time() >= end:
            return entry
        await asyncio.sleep(0.2)


def is_immutable(key: str) -> bool:
    return key.rsplit('/', 1)[-1] not in MUTABLE_OBJECTS


# The bucket, seen from outside. These are blocking calls, so they leave the event loop to a store gate.

def _objects(storage, prefix):
    return {o.key: o for o in storage.get_resource().Bucket(storage.bucket_name).objects.filter(Prefix=prefix)}


async def objects_of(storage, prefix) -> dict:
    return await asyncio.to_thread(_objects, storage, prefix)


async def shipped_bytes(storage) -> int:
    return sum(o.size for o in (await objects_of(storage, SHIPPING_PREFIX)).values())


async def get_object(storage, key) -> bytes:
    def get():
        return storage.get_resource().Object(storage.bucket_name, key).get()['Body'].read()
    return await asyncio.to_thread(get)


async def put_object(storage, key, data: bytes):
    await asyncio.to_thread(lambda: storage.get_resource().Object(storage.bucket_name, key).put(Body=data))


async def delete_object(storage, key):
    await asyncio.to_thread(lambda: storage.get_resource().Object(storage.bucket_name, key).delete())


class GatedStore:
    """The store behind a TCP pass-through which the test can cut, for store failures from outside. Nothing in
    test/pylib can make a store fail on demand: s3_proxy.py only injects random retryable errors for a whole
    run. It carries bytes unchanged, so it knows nothing about the protocol."""
    def __init__(self, storage):
        self._storage = storage
        self.port = None
        self.down = False
        self._server = None
        self._conns = set()
        self._rate = None

    def __getattr__(self, name):
        return getattr(self._storage, name)

    @property
    def address(self):
        return f'http://{self._storage.ip}:{self.port}'

    def create_endpoint_conf(self):
        return create_conf(self.address, self._storage.region)

    async def start(self):
        self._server = await asyncio.start_server(self._serve, self._storage.ip, 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self):
        self.fail()
        self._server.close()

    def fail(self):
        self.down = True
        for w in self._conns:
            w.transport.abort()
        self._conns.clear()

    def throttle(self, bytes_per_second):
        """Slows every connection, so that the test can see an upload or restore running, and act on it."""
        self._rate = bytes_per_second

    def recover(self):
        self.down = False
        self._rate = None

    async def _serve(self, reader, writer):
        if self.down:
            writer.transport.abort()
            return
        self._conns.add(writer)
        up_reader, up_writer = await asyncio.open_connection(self._storage.ip, self._storage.port)
        # A cut can land while the connection above opens. It must not survive that.
        if self.down:
            writer.transport.abort()
            up_writer.transport.abort()
            return
        self._conns.add(up_writer)

        async def pump(src, dst):
            try:
                while data := await src.read(65536):
                    if self._rate:
                        await asyncio.sleep(len(data) / self._rate)
                    if self.down:
                        return
                    dst.write(data)
                    await dst.drain()
            except ConnectionError:
                pass
            finally:
                dst.close()
                self._conns.discard(dst)
        await asyncio.gather(pump(reader, up_writer), pump(up_reader, writer))


@pytest.fixture
async def gated_store(s3_storage, scylla_cluster_teardowns) -> GatedStore:
    gate = GatedStore(s3_storage)
    await gate.start()
    scylla_cluster_teardowns.append(gate.stop)
    return gate


####################################################################################################
# Phase 1.1: shipping
####################################################################################################

@needs_phase('1.1')
async def test_shipping_off_by_default_then_on(manager: ScyllaClusterManager, s3_storage):
    """T01, SHP-1. Nothing's shipped while unset, backup and restore work as usual, and config and a restart turn it on."""
    server = await manager.server_add(config=node_config(s3_storage, shipping=False, **{CFG_SEAL_SECONDS: 1}))
    cql = manager.get_cql()
    assert not (await get_status(manager, server)).shipping_on
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t1 {TABLE_SCHEMA}')
        await apply(cql, ks, 't1', simple_rows(range(4), now_us() - 3600_000_000))
        snap, tocs = await take_snapshot_on_one_server(ks, server, manager, logger)
        prefix = f'backups/{snap}'
        await do_backup(server, snap, prefix, ks, 't1', s3_storage, manager, logger)
        assert await objects_of(s3_storage, prefix), 'the backup must be in the bucket, or this listing shows nothing'
        await assert_nothing_appears(s3_storage, (prefix, 'sstables/'))

        rows = await read_all(cql, ks, 't1')
        await cql.run_async(f'TRUNCATE TABLE {ks}.t1')
        tid = await manager.api.restore(server.ip_addr, ks, 't1', s3_storage.address, s3_storage.bucket_name, prefix, tocs)
        status = await manager.api.wait_task(server.ip_addr, tid)
        assert status['state'] == 'done'
        assert await read_all(cql, ks, 't1') == rows

        await manager.server_update_config(server.server_id, config_options=shipping_options(s3_storage))
        cql = await restart(manager, server)
        assert (await get_status(manager, server)).shipping_on
        await apply(cql, ks, 't1', simple_rows([20], now_us() - 3600_000_000))
        async def something_shipped():
            return True if await objects_of(s3_storage, SHIPPING_PREFIX) else None
        await wait_for(something_shipped, deadline())
        if not landed('1.3'):
            return      # the rest needs backup and restore

        # Every table is shipped from then on: one from before shipping began, and one created after.
        await cql.run_async(f'CREATE TABLE {ks}.t2 {TABLE_SCHEMA}')
        backups = {cf: await protect_table(manager, server, s3_storage, ks, cf) for cf in ('t1', 't2')}
        changes = {cf: simple_rows(range(10, 14), backups[cf].start_us + STEP_US, cf) for cf in ('t1', 't2')}
        for cf, statements in changes.items():
            await apply(cql, ks, cf, statements)
        latest = await wait_covered(manager, server, ks, 't1')
        await wait_covered(manager, server, ks, 't2', latest)
        assert await objects_of(s3_storage, SHIPPING_PREFIX), 'writes must reach the store'
        for cf, statements in changes.items():
            new = unique_name('restored_')
            await restore_ok(manager, server, ks, cf, backups[cf].name, latest, new)
            assert {10, 11, 12, 13} <= await pks_of(cql, ks, new)


@needs_phase('1.1')
async def test_shipping_stays_off_on_more_than_one_node(manager: ScyllaClusterManager, s3_storage):
    """T02, SHP-1. Configured on two nodes, shipping stays off and status says why, and nothing's shipped."""
    servers = await manager.servers_add(2, config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows(range(4), now_us()))
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)
            status = await get_status(manager, server)
            assert not status.shipping_on
            assert all(word in status.off_reason.lower() for word in SHIPPING_OFF_WORDS), status.off_reason
        await assert_nothing_appears(s3_storage)


@needs_phase('1.3')
async def test_small_write_costs_bytes_written(manager: ScyllaClusterManager, s3_storage):
    """T03, SHP-6. One 1 KB write costs under 1 MiB in the store, and restoring from it is exact.

    This runs at a 1 second seal interval rather than the default, to stay fast. Reserved space would
    show here just the same. The default-configuration figure is left to the developer's test."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        backup = await protect_table(manager, server, s3_storage, ks, 't')
        await wait_covered(manager, server, ks, 't')
        before = await shipped_bytes(s3_storage)
        statements = [W(backup.start_us + STEP_US, "INSERT INTO {t} (pk, ck, v) VALUES (1, 0, '" + 'x' * 1024 + "') USING TIMESTAMP {ts}")]
        await apply(cql, ks, 't', statements)
        latest = await wait_covered(manager, server, ks, 't')
        grew = await shipped_bytes(s3_storage) - before
        assert 0 < grew < 1024 * 1024, f'one 1 KB write cost {grew} bytes'

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, latest, new)
        assert len(await assert_exact(cql, ks, new, statements, latest)) == 1


@needs_phase('1.3')
async def test_brief_store_failure_needs_no_action(manager: ScyllaClusterManager, gated_store):
    """T04, OUT-1. Below the outage limit, writes keep working, status shows the failure, and recovery needs nothing."""
    server = await manager.server_add(config=node_config(gated_store))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        backup = await protect_table(manager, server, gated_store, ks, 't')
        before = simple_rows(range(4), backup.start_us + STEP_US, 'before')
        await apply(cql, ks, 't', before)
        await wait_covered(manager, server, ks, 't')

        gated_store.fail()
        first_lag = None
        unprotected = []    # wait_for() swallows an assertion, so what it sees is kept and checked afterwards

        def note(entry):
            if entry.state != PROTECTED or entry.break_cause is not None:
                unprotected.append(entry)
            return entry

        async def lag_rising():
            nonlocal first_lag
            status = await get_status(manager, server)
            entry = note(table_entry(status, ks, 't'))
            if unprotected:
                return True
            if status.store_reachable or not status.store_error:
                return None
            if first_lag is None:
                first_lag = entry.lag_seconds
                return None
            return True if entry.lag_seconds > first_lag else None
        await wait_for(lag_rising, deadline())
        assert not unprotected, f'a brief failure must not break protection: {unprotected}'
        during_ts = now_us()
        during = simple_rows(range(10, 14), during_ts, 'during')
        await apply(cql, ks, 't', during)   # a write failing here fails the test
        peak_lag = note(await table_status(manager, server, ks, 't')).lag_seconds

        gated_store.recover()
        latest = await wait_covered(manager, server, ks, 't')
        async def lag_fell():
            entry = note(await table_status(manager, server, ks, 't'))
            return True if unprotected or entry.lag_seconds < peak_lag else None
        await wait_for(lag_fell, deadline())
        note(await table_status(manager, server, ks, 't'))
        assert not unprotected, f'a brief failure must not break protection: {unprotected}'
        status = await get_status(manager, server)
        assert status.store_reachable and not status.store_error

        all_writes = before + during
        for t in (during_ts, during_ts - 1, latest):
            new = unique_name('restored_')
            await restore_ok(manager, server, ks, 't', backup.name, t, new)
            await assert_exact(cql, ks, new, all_writes, t)


@needs_phase('1.3')
async def test_long_store_outage_breaks_protection(manager: ScyllaClusterManager, gated_store):
    """T05, OUT-3. Past the limit writes still succeed, status answers with a break, and T up to coverage stays exact."""
    limit = 1024 * 1024
    server = await manager.server_add(config=node_config(gated_store, **{CFG_OUTAGE_LIMIT_BYTES: limit}))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        backup = await protect_table(manager, server, gated_store, ks, 't')
        covered_writes = simple_rows(range(4), backup.start_us + STEP_US, 'covered')
        await apply(cql, ks, 't', covered_writes)
        covered = await wait_covered(manager, server, ks, 't')

        failed_at = now_us()
        gated_store.fail()
        writers = [Writer(cql, ks, 't', min_ts=covered + 1, first_pk=1000 + 100000 * i, payload='x' * 1024).start() for i in range(3)]
        async def broke():
            status = await get_status(manager, server)
            entry = table_entry(status, ks, 't')
            return (status, entry) if entry.break_cause == CAUSE_OUTAGE else None
        status, entry = await wait_for(broke, deadline())
        for w in writers:
            await w.stop()
            assert w.failure is None, f'a write failed during the outage: {w.failure}'
            assert w.acked

        assert not status.store_reachable and status.store_error
        assert entry.state == NOT_PROTECTED and entry.reason == Reason.BROKEN, entry
        assert failed_at - CLOCK_MARGIN_US <= entry.break_time_us <= now_us() + CLOCK_MARGIN_US, entry

        gated_store.recover()
        async def store_back():
            return True if (await get_status(manager, server)).store_reachable else None
        await wait_for(store_back, deadline())
        entry = await table_status(manager, server, ks, 't')
        assert entry.state == NOT_PROTECTED, f'a break lasts until a new backup: {entry}'
        # Nothing acknowledged after the cut can be in the store, so coverage can't have moved past it.
        assert covered <= entry.latest_us <= failed_at + CLOCK_MARGIN_US, f'coverage advanced during an outage: {entry}, cut at {iso(failed_at)}'

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, covered, new)
        await assert_exact(cql, ks, new, covered_writes, covered)
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, entry.latest_us, new)
        await assert_acked_writes_present(cql, ks, new, writers, entry.latest_us, range(4))
        result = await assert_refused(manager, server, cql, ks, 't', backup.name, entry.latest_us + 1, Reason.T_PAST_COVERAGE)
        assert entry.latest_us in iso_times(result.message), result

        # What status says restores protection, and nothing else, does.
        if entry.restart_required:
            cql = await restart(manager, server)
        await run_status_commands(manager, server, entry.commands)
        await wait_protected(manager, server, ks, 't')


@pytest.mark.parametrize('kill', [False, True], ids=['clean_restart', 'kill'])
@needs_phase('1.3')
async def test_restart_or_kill_breaks_protection_visibly(manager: ScyllaClusterManager, s3_storage, kill):
    """T06, SHP-7, REF-5. A stop during writes shows a break, T on both sides of coverage behaves, and the exact commands recover."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        backup = await protect_table(manager, server, s3_storage, ks, 't')
        writes = simple_rows(range(4), backup.start_us + STEP_US, 'covered')
        await apply(cql, ks, 't', writes)
        known = await wait_covered(manager, server, ks, 't')

        # In-flight writes at the stop have timestamps above `known`, so they can't make T=known ambiguous.
        writer = Writer(cql, ks, 't', min_ts=known + 1).start()
        await writer.wait_acked(20)
        some_ack = max(ack for _, ack in writer.acked.values())
        await wait_covered(manager, server, ks, 't', some_ack + CLOCK_MARGIN_US)
        await writer.wait_acked(len(writer.acked) + 20)   # keep shipping busy up to the stop
        stopped_at = now_us()
        stopping = asyncio.create_task(restart(manager, server, kill))
        cql = await stopping
        writer.stopping = True      # a write that failed when the node went away has ended it already
        await writer.task

        entry = await table_status(manager, server, ks, 't')
        assert entry.state == NOT_PROTECTED and entry.reason == Reason.BROKEN, entry
        assert entry.break_cause == CAUSE_RESTART, entry
        assert stopped_at - CLOCK_MARGIN_US <= entry.break_time_us <= now_us() + CLOCK_MARGIN_US, entry
        assert not entry.restart_required, entry
        final = entry.latest_us
        assert final >= known, 'coverage must not go back'

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, known, new)
        await assert_exact(cql, ks, new, writes, known)

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, final, new)
        required = await assert_acked_writes_present(cql, ks, new, [writer], final, range(4))
        assert required, 'this restore proves nothing unless some writes were acknowledged before coverage'

        # Coverage no longer moves, so one microsecond past the end is deterministic here.
        result = await assert_refused(manager, server, cql, ks, 't', backup.name, final + 1, Reason.T_PAST_COVERAGE)
        assert final in iso_times(result.message), result
        assert all(word in result.message.lower() for word in PAST_COVERAGE_WORDS), result

        # A write after the restart, then the exact commands from status.
        after = simple_rows([5000], final + 1000 * STEP_US, 'after')
        await apply(cql, ks, 't', after)
        await assert_refused(manager, server, cql, ks, 't', backup.name, after[0].ts, Reason.T_PAST_COVERAGE)
        await run_status_commands(manager, server, entry.commands)
        new_backup = await wait_protected(manager, server, ks, 't')
        assert new_backup.name != backup.name
        latest = await wait_covered(manager, server, ks, 't')
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', new_backup.name, latest, new)
        assert 5000 in await pks_of(cql, ks, new)


####################################################################################################
# Phase 1.2: backup and status
####################################################################################################

@needs_phase('1.2')
async def test_normal_backup_is_all_it_takes(manager: ScyllaClusterManager, s3_storage):
    """T07, SHP-5, STA-1. Real nodetool, using the commands status gives, is the only step to protected."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        status = await get_status(manager, server)
        assert all(word in status.limits_text.lower() for word in STATUS_LIMIT_WORDS), status.limits_text
        assert status.store_reachable
        entry = table_entry(status, ks, 't')
        assert entry.state == NOT_PROTECTED and entry.reason == Reason.NO_BACKUP, entry
        assert not entry.backups
        await apply(cql, ks, 't', simple_rows(range(4), now_us() - 3600_000_000))

        await run_status_commands(manager, server, entry.commands)
        backup = await wait_protected(manager, server, ks, 't')
        latest = await wait_covered(manager, server, ks, 't', backup.start_us)
        entry = await table_status(manager, server, ks, 't')
        assert entry.reason is None, entry
        assert backup.start_us <= latest and entry.lag_seconds >= 0, entry
        if not landed('1.3'):
            return      # the rest needs restore

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, latest, new)
        assert await read_all(cql, ks, new) == await read_all(cql, ks, 't')


@needs_phase('1.2')
async def test_status_answers_for_each_table_state(manager: ScyllaClusterManager, s3_storage):
    """T08, STA-1. One answer per table, a reason code per state, and a restored table is protected only after its own backup."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks, \
               new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as tablet_ks, \
               new_test_keyspace(manager, VNODES_KS + ' AND durable_writes = false') as nondurable_ks:
        for cf in ('protected', 'unbacked'):
            await cql.run_async(f'CREATE TABLE {ks}.{cf} {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {ks}.viewed (pk int, ck int, v int, PRIMARY KEY (pk, ck))')
        await cql.run_async(f'CREATE TABLE {tablet_ks}.t {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {nondurable_ks}.t {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE MATERIALIZED VIEW {ks}.v AS SELECT * FROM {ks}.viewed '
                            'WHERE pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (ck, pk)')
        backup = await protect_table(manager, server, s3_storage, ks, 'protected')
        status = await get_status(manager, server)
        expected = {(ks, 'protected'): (PROTECTED, None),
                    (ks, 'unbacked'): (NOT_PROTECTED, Reason.NO_BACKUP),
                    (ks, 'viewed'): (NOT_PROTECTED, Reason.VIEW_BASE),
                    (tablet_ks, 't'): (NOT_PROTECTED, Reason.TABLETS),
                    (nondurable_ks, 't'): (NOT_PROTECTED, Reason.DURABLE)}
        for (k, cf), (state, reason) in expected.items():
            entry = table_entry(status, k, cf)
            assert (entry.state, entry.reason) == (state, reason), entry
        assert table_entry(status, ks, 'unbacked').commands
        latest = await wait_covered(manager, server, ks, 'protected')
        entry = await table_status(manager, server, ks, 'protected')
        assert [b.name for b in entry.backups] == [backup.name] and entry.latest_us >= latest and entry.lag_seconds is not None

        if landed('1.3'):
            new = unique_name('restored_')
            await restore_ok(manager, server, ks, 'protected', backup.name, latest, new)
            entry = await table_status(manager, server, ks, new)
            assert (entry.state, entry.reason) == (NOT_PROTECTED, Reason.RESTORED_TABLE), entry
            await run_status_commands(manager, server, entry.commands)
            await wait_protected(manager, server, ks, new)

        cql = await restart(manager, server)
        entry = await table_status(manager, server, ks, 'protected')
        assert (entry.state, entry.reason) == (NOT_PROTECTED, Reason.BROKEN), entry
        assert entry.commands


@needs_phase('1.2')
async def test_coverage_advances_with_idle_shards(manager: ScyllaClusterManager, s3_storage):
    """T09, LVL-1. Covered-through passes a single-key write, with the other shard idle, then keeps moving with no writes at all,
    while stored bytes grow by under 4 KiB per shard per seal interval.

    The time taken is logged, not gated: the S + U bound is a developer test (req:81)."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        backup = await protect_table(manager, server, s3_storage, ks, 't')
        ts = max(now_us(), backup.start_us + STEP_US)
        await apply(cql, ks, 't', simple_rows([1], ts))
        await wait_covered(manager, server, ks, 't', ts)
        logger.info(f'One write covered after {(now_us() - ts) / 1e6} seconds')
        latest = await wait_covered(manager, server, ks, 't')

        if landed('1.3'):
            new = unique_name('restored_')
            await restore_ok(manager, server, ks, 't', backup.name, ts, new)
            assert await pks_of(cql, ks, new) == {1}
            new = unique_name('restored_')
            await restore_ok(manager, server, ks, 't', backup.name, ts - 1, new)
            assert await pks_of(cql, ks, new) == set(), 'T one microsecond below the write must exclude it'
            latest = await wait_covered(manager, server, ks, 't')

        # No writes, so nothing but the idle path moves coverage, and nothing should grow but a little.
        started_at = time.time()
        bytes_before = await shipped_bytes(s3_storage)
        await wait_covered(manager, server, ks, 't', latest + IDLE_WINDOW_US)
        grew = await shipped_bytes(s3_storage) - bytes_before
        intervals = (time.time() - started_at) / SEAL_SECONDS
        assert grew < 4096 * SHARDS * intervals, f'an idle node grew by {grew} bytes in {intervals} seal intervals'
        empty = [k for k, o in (await objects_of(s3_storage, SHIPPING_PREFIX)).items() if o.size == 0]
        assert not empty, f'empty objects: {empty}'


####################################################################################################
# Phase 1.3: correct restore
####################################################################################################

@needs_phase('1.3')
async def test_restore_with_clients_writing(manager: ScyllaClusterManager, object_storage):
    """T10, RST-1, RST-7, RTN-1, LVL-4. A restore during writes is exact, the source keeps its writes, and the result is complete."""
    server = await manager.server_add(config=node_config(object_storage))
    cql = manager.get_cql()
    pks = list(range(8))
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.src {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {ks}.other {TABLE_SCHEMA}')
        seed = seed_statements(pks, now_us() - 3600_000_000)
        await apply(cql, ks, 'src', seed)
        backup = await protect_table(manager, server, object_storage, ks, 'src')
        changes, ts = change_statements(pks, backup.start_us)
        await apply(cql, ks, 'src', changes)
        await apply(cql, ks, 'other', simple_rows(range(500, 504), ts['insert'], 'other'))
        await wait_covered(manager, server, ks, 'src')
        t = ts['static']

        src_writer = Writer(cql, ks, 'src', min_ts=ts['last'] + 1).start()
        other_writer = Writer(cql, ks, 'other', min_ts=ts['last'] + 1).start()
        await src_writer.wait_acked(10)
        await other_writer.wait_acked(10)
        new = unique_name('restored_')
        restore_started = now_us()
        result = await restore_ok(manager, server, ks, 'src', backup.name, t, new)
        restore_done = now_us()
        await src_writer.stop()
        await other_writer.stop()
        for w in (src_writer, other_writer):
            assert w.failure is None, w.failure
            assert any(restore_started <= ack <= restore_done for _, ack in w.acked.values()), 'a client write must land during the restore'

        expected = await assert_exact(cql, ks, new, seed + changes, t)
        assert expected != await read_exact(cql, ks, 'src'), 'T must cut something off for this test to mean anything'
        assert set(src_writer.attempted).isdisjoint(await pks_of(cql, ks, new))
        assert set(src_writer.acked) <= await pks_of(cql, ks, 'src')
        assert await pks_of(cql, ks, 'other') >= set(other_writer.acked) | {500, 501, 502, 503}

        if not landed('1.4'):
            return      # the rest reads the result's fields
        # RST-7
        assert result.t_us == t and result.backup == backup.name, result
        assert result.objects_read > 0 and result.bytes_read > 0, result
        assert result.cells_applied > 0 and result.cells_dropped > 0, result
        assert FLAG_EMPTY_BACKUP not in result.flags
        notices = result.notices.lower()
        for limit, word in RESULT_LIMIT_WORDS.items():
            assert word in notices, f'{limit} is missing from the result: {result.notices}'
        # RTN-1
        assert WRONG_WAY_BACK in result.notices
        assert all(word in notices for word in COPY_NOTICE_WORDS), result.notices
        await run_status_commands(manager, server, result.commands)
        await wait_protected(manager, server, ks, new)
        # LVL-4
        assert set(result.steps) == set(STEPS), result
        assert abs(sum(s.seconds for s in result.steps.values()) - result.total_seconds) <= 0.01 * result.total_seconds, result
        assert all(s.seconds >= 0 and s.bytes >= 0 for s in result.steps.values()), result
        assert result.other_tables_bytes > 0, 'another table was written in the window, so its bytes were read'


@needs_phase('1.3')
async def test_exact_at_every_boundary_of_t(manager: ScyllaClusterManager, object_storage):
    """T11, RST-2. Every kind of change, on every shard, restored to T at each boundary, against a table built from the statements."""
    server = await manager.server_add(config=node_config(object_storage))
    cql = manager.get_cql()
    pks = list(range(8))
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.src {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {ks}.decoy {TABLE_SCHEMA}')
        seed = seed_statements(pks, now_us() - 3600_000_000)
        await apply(cql, ks, 'src', seed)
        await assert_every_shard(cql, ks, 'src', pks)
        backup = await protect_table(manager, server, object_storage, ks, 'src')
        changes, ts = change_statements(pks, backup.start_us)
        await apply(cql, ks, 'src', changes)

        # The decoy gets the same keys and later writes, with values of its own, and a key the source doesn't have.
        await apply(cql, ks, 'decoy', changes)
        await apply(cql, ks, 'decoy', [W(ts['insert'], f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET v = 'DECOY' WHERE pk = {pk} AND ck = 0") for pk in pks])
        await apply(cql, ks, 'decoy', simple_rows([999], ts['insert'], 'DECOY'))
        end = await wait_covered(manager, server, ks, 'src')

        history = seed + changes
        cases = {'interval_start': backup.start_us,
                 'range_delete_minus_1': ts['range_delete'] - 1,
                 'range_delete_equal': ts['range_delete'],
                 'range_delete_plus_1': ts['range_delete'] + 1,
                 'batch_split': ts['batch_low'],
                 'tie': ts['tie'],
                 'interval_end': end}
        got = {}
        for name, t in cases.items():
            new = unique_name('restored_')
            result = await restore_ok(manager, server, ks, 'src', backup.name, t, new)
            got[name] = await assert_exact(cql, ks, new, history, t)
            logger.info(f'{name}: exact at {iso(t)}, {len(got[name])} rows')
            if name == 'interval_start' and landed('1.4'):
                assert result.cells_dropped > 0, f'every later write is above T: {result}'
            if name == 'interval_end' and landed('1.4'):
                assert result.cells_dropped == 0, f'no write of this table is above T: {result}'

        # If these are equal, T isn't deciding anything and the cases above prove nothing.
        assert got['range_delete_minus_1'] != got['range_delete_equal']
        assert got['range_delete_equal'] == got['range_delete_plus_1']
        assert got['interval_start'] == sorted(await expected_at(cql, ks, seed, backup.start_us))
        assert got['batch_split'] != got['interval_end']
        assert got['interval_end'] == await read_exact(cql, ks, 'src')


@needs_phase('1.3')
async def test_exact_with_empty_inputs(manager: ScyllaClusterManager, s3_storage):
    """T12, RST-2, RST-7. An empty table at the backup, and no writes since the backup, including the empty-backup flag."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        for cf in ('empty', 'empty_written', 'quiet'):
            await cql.run_async(f'CREATE TABLE {ks}.{cf} {TABLE_SCHEMA}')
        seed = simple_rows(range(4), now_us() - 3600_000_000)
        await apply(cql, ks, 'quiet', seed)
        backup = await protect_table(manager, server, s3_storage, ks, 'empty')
        for cf in ('empty_written', 'quiet'):
            await do_backup(server, backup.name, backup_prefix(ks, cf, backup.name), ks, cf, s3_storage, manager, logger)
            await wait_protected(manager, server, ks, cf, backup.name)
        later = simple_rows(range(10, 14), backup.start_us + STEP_US)
        await apply(cql, ks, 'empty_written', later)
        latest = await wait_covered(manager, server, ks, 'empty')

        new = unique_name('restored_')
        result = await restore_ok(manager, server, ks, 'empty', backup.name, latest, new)
        assert await read_all(cql, ks, new) == []
        assert not landed('1.4') or (result.cells_applied == 0 and FLAG_EMPTY_BACKUP in result.flags), result

        new = unique_name('restored_')
        result = await restore_ok(manager, server, ks, 'empty_written', backup.name, latest, new)
        await assert_exact(cql, ks, new, later, latest)
        assert not landed('1.4') or (result.cells_applied > 0 and FLAG_EMPTY_BACKUP not in result.flags), result

        new = unique_name('restored_')
        result = await restore_ok(manager, server, ks, 'quiet', backup.name, latest, new)
        await assert_exact(cql, ks, new, seed, latest)
        assert not landed('1.4') or FLAG_EMPTY_BACKUP not in result.flags, result


@needs_phase('1.3')
async def test_deleted_row_stays_deleted(manager: ScyllaClusterManager, s3_storage):
    """T13, RST-3. Purged before the backup, grace expired by the restore, truncated before the backup, and compaction during the restore."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    opts = "WITH gc_grace_seconds = 0 AND tombstone_gc = {'mode': 'immediate'}"
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.purged {TABLE_SCHEMA} {opts}')
        await cql.run_async(f'CREATE TABLE {ks}.truncated {TABLE_SCHEMA} {opts}')

        # Shipped, then purged, then backed up: only the shipped log still knows about row 1.
        ts = now_us() - 3600_000_000
        await apply(cql, ks, 'purged', simple_rows([1, 2], ts))
        await cql.run_async(f'DELETE FROM {ks}.purged USING TIMESTAMP {ts + 1} WHERE pk = 1')
        await manager.api.keyspace_flush(server.ip_addr, ks, 'purged')
        # A tombstone can't be purged in the second it was written, so compaction is repeated until it's gone.
        # If it never goes, this case would prove nothing, and the wait fails.
        async def purged():
            await manager.api.keyspace_compaction(server.ip_addr, ks, 'purged')
            fragments = await cql.run_async(f'SELECT * FROM MUTATION_FRAGMENTS({ks}.purged)')
            return True if {f.pk for f in fragments} == {2} else None
        await wait_for(purged, deadline())

        await apply(cql, ks, 'truncated', simple_rows([1, 2], ts))
        await cql.run_async(f'TRUNCATE TABLE {ks}.truncated')
        await apply(cql, ks, 'truncated', simple_rows([3], ts + 2))

        backup = await protect_table(manager, server, s3_storage, ks, 'purged')
        await do_backup(server, backup.name, backup_prefix(ks, 'truncated', backup.name), ks, 'truncated', s3_storage, manager, logger)
        await wait_protected(manager, server, ks, 'truncated', backup.name)

        # In the backup, deleted after it with no grace period: a restore must not purge this tombstone before loading.
        delete_ts = backup.start_us + STEP_US
        await cql.run_async(f'DELETE FROM {ks}.purged USING TIMESTAMP {delete_ts} WHERE pk = 2')
        latest = await wait_covered(manager, server, ks, 'purged')

        new = unique_name('restored_')
        started = await start_restore(manager, server, ks, 'purged', backup.name, delete_ts, new)
        assert not isinstance(started, Result), started
        # Manual compaction, attempted throughout. The table may not exist yet, so a failed attempt is expected.
        waiting = asyncio.create_task(manager.api.wait_task(server.ip_addr, started))
        attempts = 0
        while not waiting.done():
            attempts += 1
            try:
                await manager.api.keyspace_compaction(server.ip_addr, ks, new)
            except HTTPError as e:
                logger.info(f'Compaction attempt {attempts}: {e}')
        await waiting
        logger.info(f'Attempted compaction {attempts} times during the restore')
        assert (await get_result(manager, server, ks, new)).state == DONE
        assert await pks_of(cql, ks, new) == set(), 'rows 1 and 2 were deleted at or before T'

        # The same table with the delete above T: the row is present, so the absence above isn't an empty restore.
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 'purged', backup.name, delete_ts - 1, new)
        assert await pks_of(cql, ks, new) == {2}

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 'truncated', backup.name, latest, new)
        assert await pks_of(cql, ks, new) == {3}, 'rows from before a truncate that precedes the backup stay gone'


@needs_phase('1.3')
async def test_recover_from_truncate(manager: ScyllaClusterManager, s3_storage):
    """T14, RST-4, REF-10. T before an accidental TRUNCATE brings the data back, with no restart, and the source stays empty and in use."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    pks = list(range(8))
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.src {TABLE_SCHEMA}')
        seed = seed_statements(pks, now_us() - 3600_000_000)
        await apply(cql, ks, 'src', seed)
        backup = await protect_table(manager, server, s3_storage, ks, 'src')
        changes, ts = change_statements(pks, backup.start_us)
        await apply(cql, ks, 'src', changes)
        t = await wait_covered(manager, server, ks, 'src')
        await cql.run_async(f'TRUNCATE TABLE {ks}.src')
        await wait_covered(manager, server, ks, 'src')

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 'src', backup.name, t, new)
        assert await assert_exact(cql, ks, new, seed + changes, t), 'the truncated data must be back'
        assert await read_all(cql, ks, 'src') == []
        await apply(cql, ks, 'src', simple_rows([1], now_us()))
        assert await pks_of(cql, ks, 'src') == {1}


@needs_phase('1.3')
async def test_new_table_has_schema_of_the_backup(manager: ScyllaClusterManager, s3_storage):
    """T15, RST-6, REF-9. The new table's DESCRIBE matches the source's at the backup, including after a later ALTER, and non-default options."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TYPE {ks}.addr (street text, zip int)')
        await cql.run_async(f'''CREATE TABLE {ks}.src (pk int, ck int, v text, a frozen<addr>, gone int, s int static, PRIMARY KEY (pk, ck))
            WITH CLUSTERING ORDER BY (ck DESC) AND comment = 'pitr' AND gc_grace_seconds = 1234
            AND compaction = {{'class': 'LeveledCompactionStrategy'}} AND compression = {{'sstable_compression': 'DeflateCompressor'}}''')
        old = now_us() - 3600_000_000
        await cql.run_async(f"INSERT INTO {ks}.src (pk, ck, v, a, gone, s) VALUES (1, 1, 'x', {{street: 'y', zip: 2}}, 3, 4) USING TIMESTAMP {old}")
        await cql.run_async(f'ALTER TABLE {ks}.src DROP gone')
        describe = (await cql.run_async(f'DESCRIBE TABLE {ks}.src'))[0].create_statement
        rows = await read_all(cql, ks, 'src')
        backup = await protect_table(manager, server, s3_storage, ks, 'src')
        t = await wait_covered(manager, server, ks, 'src')

        await cql.run_async(f'ALTER TABLE {ks}.src ADD later int')
        await wait_covered(manager, server, ks, 'src')
        assert not await table_exists(cql, ks, 'restored')
        result = await restore_ok(manager, server, ks, 'src', backup.name, t, 'restored')
        restored = (await cql.run_async(f'DESCRIBE TABLE {ks}.restored'))[0].create_statement
        assert restored.replace(f'{ks}.restored', f'{ks}.src') == describe
        assert await read_all(cql, ks, 'restored') == rows
        logger.info(f'Grants notice, in the result or the README: {"grant" in result.notices.lower()}')


@needs_phase('1.4')
async def test_finished_restore_survives_kill(manager: ScyllaClusterManager, s3_storage):
    """T16, RST-12, RST-7. Killed right after done: the table and the result are unchanged."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    pks = list(range(8))
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.src {TABLE_SCHEMA}')
        seed = seed_statements(pks, now_us() - 3600_000_000)
        await apply(cql, ks, 'src', seed)
        backup = await protect_table(manager, server, s3_storage, ks, 'src')
        changes, ts = change_statements(pks, backup.start_us)
        await apply(cql, ks, 'src', changes)
        await wait_covered(manager, server, ks, 'src')

        new = unique_name('restored_')
        result = await restore_ok(manager, server, ks, 'src', backup.name, ts['static'], new)
        cql = await restart(manager, server, kill=True)
        assert (await get_result(manager, server, ks, new)).raw == result.raw
        await assert_exact(cql, ks, new, seed + changes, ts['static'])
        entry = await table_status(manager, server, ks, new)
        assert entry.state != INCOMPLETE, entry


@needs_phase('1.3')
async def test_killed_restore_is_incomplete_and_harmless(manager: ScyllaClusterManager, gated_store):
    """T17, RST-12, STA-1, REF-13. Killed while running: status says incomplete, other tables are unchanged, the name stays taken, and a new name works.

    The kill lands on whichever step is seen running. The store is slowed so that a restore stays running, and a restore that
    still ends before the kill is tried again. Covering each step needs a pause no black-box test can make: see the plan."""
    server = await manager.server_add(config=node_config(gated_store))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.src {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {ks}.other {TABLE_SCHEMA}')
        seed = seed_statements(range(50), now_us() - 3600_000_000)
        await apply(cql, ks, 'src', seed)
        backup = await protect_table(manager, server, gated_store, ks, 'src')
        changes, _ = change_statements(range(50), backup.start_us)
        await apply(cql, ks, 'src', changes)
        await apply(cql, ks, 'other', simple_rows(range(4), backup.start_us))
        t = await wait_covered(manager, server, ks, 'src')
        before = {cf: await read_all(cql, ks, cf) for cf in ('src', 'other')}

        entry = None
        for _ in range(KILL_ATTEMPTS):
            new = unique_name('restored_')
            gated_store.throttle(THROTTLE_BYTES_PER_SECOND)
            started = await start_restore(manager, server, ks, 'src', backup.name, t, new)
            assert not isinstance(started, Result), started
            async def seen():
                found = [e for e in (await get_status(manager, server)).tables if (e.keyspace, e.table) == (ks, new)]
                return found[0] if found else None
            await wait_for(seen, deadline(), period=0.001, backoff_factor=1, max_period=0.001)
            cql = await restart(manager, server, kill=True, while_down=gated_store.recover)
            entry = await table_status(manager, server, ks, new)
            if entry.state == INCOMPLETE:
                break
            logger.info(f'The restore ended before the kill landed: {entry}')
        assert entry.state == INCOMPLETE, f'{KILL_ATTEMPTS} restores ended before the kill landed, so nothing was killed: {entry}'
        assert entry.step in STEPS, entry
        assert all(word in entry.text.lower() for word in INCOMPLETE_ACTION_WORDS), entry
        for cf, rows in before.items():
            assert await read_all(cql, ks, cf) == rows, f'{cf} changed'
        await assert_refused(manager, server, cql, ks, 'src', backup.name, t, Reason.NAME_TAKEN, new_table=new)

        await cql.run_async(f'DROP TABLE {ks}.{new}')
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 'src', backup.name, t, new)
        await assert_exact(cql, ks, new, seed + changes, t)


####################################################################################################
# Phase 1.3: refusals
####################################################################################################

@needs_phase('1.3')
async def test_refuse_unsupported_tables(manager: ScyllaClusterManager, s3_storage):
    """T18, REF-1. One node, one refusal per kind, all codes different, and the supported control is accepted.

    Folded into one node for cost. Failures are collected so that one doesn't hide the rest."""
    server = await manager.server_add(config=node_config(s3_storage, alternator_port=8000, alternator_write_isolation='only_rmw_uses_lwt',
                                                         experimental_features=['strongly-consistent-tables', 'logstor']))
    cql = manager.get_cql()
    alternator = boto3.resource('dynamodb', endpoint_url=f'http://{server.ip_addr}:8000', region_name='us-east-1',
                                              aws_access_key_id='alternator', aws_secret_access_key='secret_pass')
    async with new_test_keyspace(manager, VNODES_KS) as ks, \
               new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as tablet_ks, \
               new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                                          "AND tablets = {'initial': 1} AND consistency = 'global'") as strong_ks, \
               new_test_keyspace(manager, VNODES_KS + ' AND durable_writes = false') as nondurable_ks:
        await cql.run_async(f'CREATE TABLE {ks}.supported {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {tablet_ks}.t (pk int PRIMARY KEY, v int)')
        await cql.run_async(f'CREATE TABLE {strong_ks}.t (pk int PRIMARY KEY, v int)')
        await cql.run_async(f'CREATE TABLE {nondurable_ks}.t (pk int PRIMARY KEY, v int)')
        await cql.run_async(f'CREATE TABLE {ks}.counters (pk int PRIMARY KEY, c counter)')
        await cql.run_async(f'CREATE TABLE {ks}.viewed (pk int, ck int, v int, PRIMARY KEY (pk, ck))')
        await cql.run_async(f'CREATE MATERIALIZED VIEW {ks}.a_view AS SELECT * FROM {ks}.viewed WHERE pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (ck, pk)')
        await cql.run_async(f'CREATE TABLE {ks}.indexed (pk int PRIMARY KEY, v int)')
        await cql.run_async(f'CREATE INDEX an_index ON {ks}.indexed (v)')
        await cql.run_async(f"CREATE TABLE {ks}.cdc (pk int PRIMARY KEY, v int) WITH cdc = {{'enabled': true}}")
        # In a tablet keyspace, as in test_logstor.py. The refusal must still name logstor.
        await cql.run_async(f"CREATE TABLE {tablet_ks}.logstor (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")
        alternator.create_table(TableName='alt', BillingMode='PAY_PER_REQUEST', KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                                Tags=[{'Key': 'system:initial_tablets', 'Value': 'none'}])

        cases = [(tablet_ks, 't', Reason.TABLETS), (strong_ks, 't', Reason.STRONG), (nondurable_ks, 't', Reason.DURABLE),
                 (ks, 'counters', Reason.COUNTERS), (ks, 'viewed', Reason.VIEW_BASE), (ks, 'a_view', Reason.VIEW_TABLE),
                 (ks, 'indexed', Reason.INDEXED), (ks, 'an_index_index', Reason.INDEX_TABLE),
                 (ks, 'cdc', Reason.CDC), (ks, 'cdc_scylla_cdc_log', Reason.CDC), (tablet_ks, 'logstor', Reason.LOGSTOR),
                 ('system', 'local', Reason.SYSTEM), ('system_schema', 'tables', Reason.SYSTEM), ('alternator_alt', 'alt', Reason.ALTERNATOR)]
        assert len({c[2] for c in cases}) == len(cases) - 2, 'each kind of table has its own code'
        failures = []
        for k, cf, reason in cases:
            try:
                await assert_refused(manager, server, cql, k, cf, 'any_backup', now_us(), reason)
            except AssertionError as e:
                failures.append(f'{k}.{cf}: {e}')
        assert not failures, '\n'.join(failures)

        backup = await protect_table(manager, server, s3_storage, ks, 'supported')
        await restore_ok(manager, server, ks, 'supported', backup.name, backup.start_us, unique_name('restored_'))


@pytest.mark.parametrize('case', ['rf_at_creation', 'rf_altered', 'node_added'])
@needs_phase('1.3')
async def test_refuse_more_than_one_node_or_rf(manager: ScyllaClusterManager, s3_storage, case):
    """T19, REF-2. RF above 1 from the start or altered later, or a node added after protection began."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    rf = 2 if case == 'rf_at_creation' else 1
    opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} AND tablets = {{'enabled': false}}"
    async with new_test_keyspace(manager, opts) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        if rf == 1:     # writes use LOCAL_QUORUM, which a single node can't meet at RF 2
            await apply(cql, ks, 't', simple_rows(range(4), now_us() - 3600_000_000))
        if rf == 1:     # protected first, and with T covered, so that only the later change can cause the refusal
            snap = (await protect_table(manager, server, s3_storage, ks, 't')).name
            t = await wait_covered(manager, server, ks, 't')
        else:
            snap = await backup_table(manager, server, s3_storage, ks, 't')
            t = now_us()
        reason = Reason.RF
        if case == 'rf_altered':
            await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}}")
        elif case == 'node_added':
            await manager.server_add(config=node_config(s3_storage))
            reason = Reason.NODES
        result = await assert_refused(manager, server, cql, ks, 't', snap, t, reason)
        assert TOPOLOGY_LIMIT_WORDS[reason] in result.message.lower(), result


@pytest.mark.parametrize('written', ['this_table', 'other_table'])
@needs_phase('1.3')
async def test_refuse_tampered_history(manager: ScyllaClusterManager, s3_storage, written):
    """T20, REF-4. Each shipped object in the window is removed, cut short and changed by one byte, from outside, and restore refuses each.

    This needs no knowledge of the object format. An object is in the window if it was uploaded before T, because
    it can't hold anything newer than its upload. The control after the tampering shows the tampering, and
    nothing else, caused the refusals."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.src {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {ks}.other {TABLE_SCHEMA}')
        seed = simple_rows(range(8), now_us() - 3600_000_000)
        await apply(cql, ks, 'src', seed)
        backup = await protect_table(manager, server, s3_storage, ks, 'src')
        await wait_covered(manager, server, ks, 'src')
        known = await objects_of(s3_storage, SHIPPING_PREFIX)

        writes = simple_rows(range(8, 16), backup.start_us + STEP_US)
        await apply(cql, ks, 'src' if written == 'this_table' else 'other', writes)
        await wait_covered(manager, server, ks, 'src')
        objects = await objects_of(s3_storage, SHIPPING_PREFIX)
        # S3 reports seconds, so an upload can be up to a second after the time reported.
        newest = max(o.last_modified for o in objects.values()).timestamp()
        t = await wait_covered(manager, server, ks, 'src', int(newest * 1e6) + 2 * CLOCK_MARGIN_US)
        window = {k: o for k, o in objects.items() if k not in known and o.size >= 2 and is_immutable(k)}
        assert window, 'no objects to tamper with, so this test would prove nothing'
        rng = random.Random(22)
        chosen = sorted(window)
        rng.shuffle(chosen)
        chosen = chosen[:3]
        logger.info(f'Tampering with {chosen} of {len(window)} objects, T={iso(t)}')

        failures = []
        for key in chosen:
            original = await get_object(s3_storage, key)
            flipped = bytearray(original)
            flipped[len(flipped) // 2] ^= 0x01
            for kind, reasons, tamper in (('removed', {Reason.OBJECT_MISSING, Reason.RECORDS_MISSING}, None),
                                          ('cut short', {Reason.OBJECT_DAMAGED, Reason.RECORDS_DAMAGED}, original[:len(original) // 2]),
                                          ('one byte changed', {Reason.OBJECT_DAMAGED, Reason.RECORDS_DAMAGED}, bytes(flipped))):
                if tamper is None:
                    await delete_object(s3_storage, key)
                else:
                    await put_object(s3_storage, key, tamper)
                try:
                    probe_name = unique_name('tampered_')
                    probe = await run_restore(manager, server, ks, 'src', backup.name, t, probe_name)
                    assert probe.state == FAILED and probe.reason in reasons, f'{key} {kind}: {probe}'
                    assert key.rsplit('/', 1)[-1] in probe.message, f'the refusal must name the object: {probe}'
                    assert not probe.cells_applied, f'the refusal must come before any data is loaded: {probe}'
                    assert not await table_exists(cql, ks, probe_name), f'a refusal leaves no new table: {probe}'
                    assert probe.remaining_start_us in (None, backup.start_us), f'a remaining interval starts at the backup: {probe}'
                except AssertionError as e:
                    failures.append(str(e))
                await put_object(s3_storage, key, original)
        assert not failures, '\n'.join(failures)

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 'src', backup.name, t, new)
        await assert_exact(cql, ks, new, seed + (writes if written == 'this_table' else []), t)


@pytest.mark.parametrize('upload', ['before_shipping', 'after_shipping'])
@needs_phase('1.3')
async def test_refuse_backup_from_before_shipping(manager: ScyllaClusterManager, s3_storage, upload):
    """T21, REF-4, SHP-5. A snapshot from before shipping isn't usable, even if it's uploaded after shipping began: writes between are in neither."""
    server = await manager.server_add(config=node_config(s3_storage, shipping=False))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows(range(4), now_us() - 3600_000_000))
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        prefix = backup_prefix(ks, 't', snap)
        if upload == 'before_shipping':
            await do_backup(server, snap, prefix, ks, 't', s3_storage, manager, logger)
        await apply(cql, ks, 't', simple_rows([100], now_us() - 3600_000_000))    # in no backup, and never shipped
        await manager.server_update_config(server.server_id, config_options=shipping_options(s3_storage))
        cql = await restart(manager, server)
        if upload == 'after_shipping':
            await do_backup(server, snap, prefix, ks, 't', s3_storage, manager, logger)

        entry = await table_status(manager, server, ks, 't')
        assert entry.state == NOT_PROTECTED and not entry.backups, entry
        result = await assert_refused(manager, server, cql, ks, 't', snap, now_us() - CLOCK_MARGIN_US, Reason.BACKUP_BEFORE_SHIPPING)
        assert 'nodetool backup' in result.message, result


@needs_phase('1.3')
async def test_refuse_t_outside_coverage(manager: ScyllaClusterManager, s3_storage):
    """T22, REF-5, REF-6. T in the future, T above coverage with data there, T below the interval, no such backup, and no backup.
    Both ends of the interval are accepted.

    One microsecond past the end is in T06, where coverage has stopped moving."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {ks}.unbacked {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows(range(4), now_us() - 3600_000_000))
        backup = await protect_table(manager, server, s3_storage, ks, 't')

        # A single key, so the other shard is idle, with a timestamp far ahead: data exists there, but T isn't covered.
        future = now_us() + 86400_000_000
        await apply(cql, ks, 't', simple_rows([50], future))
        await wait_covered(manager, server, ks, 't')
        watched = ['unbacked']
        for t in (future, future - 1):
            result = await assert_refused(manager, server, cql, ks, 't', backup.name, t, Reason.T_PAST_COVERAGE, watched)
            assert all(word in result.message.lower() for word in PAST_COVERAGE_WORDS), result
            assert any(x < t for x in iso_times(result.message)), f'the refusal must give the latest restorable time: {result}'

        result = await assert_refused(manager, server, cql, ks, 't', backup.name, backup.start_us - 1, Reason.T_BEFORE_COVERAGE, watched)
        assert backup.start_us in iso_times(result.message), result

        result = await assert_refused(manager, server, cql, ks, 't', 'no_such_backup', backup.start_us, Reason.NO_SUCH_BACKUP, watched)
        assert backup.name in result.message or backup.start_us in iso_times(result.message), result

        result = await assert_refused(manager, server, cql, ks, 'unbacked', 'no_such_backup', now_us(), Reason.NO_BACKUP, ['t'])
        assert 'nodetool snapshot' in result.message and 'nodetool backup' in result.message, result

        for t in (backup.start_us, await wait_covered(manager, server, ks, 't')):
            await restore_ok(manager, server, ks, 't', backup.name, t, unique_name('restored_'))


@needs_phase('1.3')
async def test_refuse_backup_with_unfinished_upload(manager: ScyllaClusterManager, gated_store):
    """T23, REF-7. An upload cut off from outside, and a snapshot never uploaded, are refused and not listed. Finishing the upload makes it usable."""
    server = await manager.server_add(config=node_config(gated_store))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t {TABLE_SCHEMA} WITH compression = {{'sstable_compression': ''}}")
        for first in range(0, 1200, 300):   # several uncompressed sstables, so that the upload has more than one object
            await apply(cql, ks, 't', simple_rows(range(first, first + 300), now_us() - 3600_000_000, 'x' * 1024))
            await manager.api.keyspace_flush(server.ip_addr, ks, 't')
        usable = await protect_table(manager, server, gated_store, ks, 't')
        cut, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        never, _ = await take_snapshot_on_one_server(ks, server, manager, logger)

        # Slowed so that the upload is still running when its first object appears, and cut then.
        size = await manager.api.get_table_disk_space_used(server.ip_addr, ks, 't')
        gated_store.throttle(THROTTLE_BYTES_PER_SECOND)
        prefix = backup_prefix(ks, 't', cut)
        tid = await manager.api.backup(server.ip_addr, ks, 't', cut, gated_store.address, gated_store.bucket_name, prefix)
        async def some_uploaded():
            return True if await objects_of(gated_store, f'{prefix}/') else None
        await wait_for(some_uploaded, deadline(), period=0.05, backoff_factor=1)
        gated_store.fail()
        try:
            await manager.api.abort_task(server.ip_addr, tid)
        except HTTPError as e:
            logger.info(f'The task had already ended: {e}')
        state = (await manager.api.wait_task(server.ip_addr, tid))['state']
        assert state == 'failed', 'the upload must really have been cut, or this case proves nothing'
        uploaded = sum(o.size for o in (await objects_of(gated_store, f'{prefix}/')).values())
        assert 0 < uploaded < size, f'the upload must be partial, or this case proves nothing: {uploaded} of {size} bytes'
        gated_store.recover()

        latest = await wait_covered(manager, server, ks, 't')
        for name in (cut, never):
            result = await assert_refused(manager, server, cql, ks, 't', name, latest, Reason.BACKUP_UNFINISHED)
            assert usable.name in result.message, f'the refusal lists the usable backups: {result}'
            assert any(word in result.message.lower() for word in UNFINISHED_BACKUP_WORDS), result
        entry = await table_status(manager, server, ks, 't')
        assert [b.name for b in entry.backups] == [usable.name], entry

        await do_backup(server, cut, prefix, ks, 't', gated_store, manager, logger)
        await wait_protected(manager, server, ks, 't', cut)
        await restore_ok(manager, server, ks, 't', cut, await wait_covered(manager, server, ks, 't'), unique_name('restored_'))


@pytest.mark.parametrize('change', ['TABLE {ks}.t ADD added int', 'TABLE {ks}.t DROP w', "TABLE {ks}.t WITH comment = 'changed'",
                                    'TYPE {ks}.addr ADD extra int'],
                         ids=['add_column', 'drop_column', 'option', 'udt_it_uses'])
@needs_phase('1.3')
async def test_refuse_schema_change_in_window(manager: ScyllaClusterManager, s3_storage, change):
    """T24, REF-9. T after the ALTER is refused, naming it and its time. T before it is accepted, and a newer backup fixes it."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TYPE {ks}.addr (zip int)')
        await cql.run_async(f'CREATE TABLE {ks}.t (pk int, ck int, v text, w int, a frozen<addr>, PRIMARY KEY (pk, ck))')
        await apply(cql, ks, 't', simple_rows(range(4), now_us() - 3600_000_000))
        describe = (await cql.run_async(f'DESCRIBE TABLE {ks}.t'))[0].create_statement
        backup = await protect_table(manager, server, s3_storage, ks, 't')
        before_change = await wait_covered(manager, server, ks, 't')

        low = now_us()
        await cql.run_async('ALTER ' + change.format(ks=ks))
        high = now_us()
        after_change = await wait_covered(manager, server, ks, 't')

        result = await assert_refused(manager, server, cql, ks, 't', backup.name, after_change, Reason.SCHEMA_CHANGED)
        assert any(low - CLOCK_MARGIN_US <= x <= high + CLOCK_MARGIN_US for x in iso_times(result.message)), f'the event time: {result}'
        assert any(word in result.message.lower() for word in NEWER_BACKUP_WORDS), result

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, before_change, new)
        restored = (await cql.run_async(f'DESCRIBE TABLE {ks}.{new}'))[0].create_statement
        assert restored.replace(f'{ks}.{new}', f'{ks}.t') == describe

        newer = await protect_table(manager, server, s3_storage, ks, 't')
        await restore_ok(manager, server, ks, 't', newer.name, await wait_covered(manager, server, ks, 't'), unique_name('restored_'))


@needs_phase('1.3')
async def test_refuse_truncate_in_window(manager: ScyllaClusterManager, s3_storage):
    """T25, REF-10. T after the TRUNCATE is refused, naming it and its time. T before it, and T with a newer backup, are accepted."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        seed = simple_rows(range(4), now_us() - 3600_000_000)
        await apply(cql, ks, 't', seed)
        backup = await protect_table(manager, server, s3_storage, ks, 't')
        before_truncate = await wait_covered(manager, server, ks, 't')

        low = now_us()
        await cql.run_async(f'TRUNCATE TABLE {ks}.t')
        high = now_us()
        await apply(cql, ks, 't', simple_rows([50], high))
        after_truncate = await wait_covered(manager, server, ks, 't')

        result = await assert_refused(manager, server, cql, ks, 't', backup.name, after_truncate, Reason.TRUNCATED)
        assert any(low - CLOCK_MARGIN_US <= x <= high + CLOCK_MARGIN_US for x in iso_times(result.message)), f'the event time: {result}'
        assert any(word in result.message.lower() for word in NEWER_BACKUP_WORDS), result

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, before_truncate, new)
        await assert_exact(cql, ks, new, seed, before_truncate)

        newer = await protect_table(manager, server, s3_storage, ks, 't')
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', newer.name, await wait_covered(manager, server, ks, 't'), new)
        assert await pks_of(cql, ks, new) == {50}


@needs_phase('1.3')
async def test_refuse_invalid_t_and_accept_short_forms(manager: ScyllaClusterManager, s3_storage):
    """T26, REF-14. Each invalid form is refused and shows the form. Fields left out below the second mean zero, shown by a write at exactly that time."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        backup = await protect_table(manager, server, s3_storage, ks, 't')
        whole_second = (backup.start_us // 1_000_000 + 2) * 1_000_000
        at_second, at_half = whole_second, whole_second + 500_000
        await apply(cql, ks, 't', simple_rows([1], at_second) + simple_rows([2], at_half))
        await wait_covered(manager, server, ks, 't', at_half + CLOCK_MARGIN_US)
        await wait_covered(manager, server, ks, 't')

        invalid = ['2026-11-04T24:00:00Z', '2026-11-04T14:05:00z', '2026-11-04t14:05:00Z', ' 2026-11-04T14:05:00Z', '2026-11-04T14:05:00Z ',
                   '2026-11-04T14:05:00', '2026-11-04T14:05:00.123456', '2026-11-04T14:05:00+02:00', '2026-11-04T14:05:00+00:00',
                   '2026-11-04 14:05:00', '2026-11-04 14:05:00Z', '2026-11-04T14:05:00.1234567Z', '2026-02-30T14:05:00Z',
                   '2026-13-01T14:05:00Z', '2026-12-31T23:59:60Z', '1730000000', '1730000000000000', '', ' ', 'now']
        failures = []
        for form in invalid:
            try:
                result = await assert_refused(manager, server, cql, ks, 't', backup.name, form, Reason.T_INVALID)
                assert ISO_RE.search(result.message), f'{form!r}: the refusal must show an example: {result}'
            except AssertionError as e:
                failures.append(f'{form!r}: {e}')
        assert not failures, '\n'.join(failures)

        base = iso(whole_second)[:-len('.000000Z')]
        for form, expected in ((f'{base}Z', {1}), (f'{base}.000000Z', {1}), (f'{base}.5Z', {1, 2}), (f'{base}.499999Z', {1})):
            new = unique_name('restored_')
            await restore_ok(manager, server, ks, 't', backup.name, form, new)
            assert await pks_of(cql, ks, new) == expected, form


@needs_phase('1.3')
async def test_refuse_when_disk_reserve_would_be_breached(manager: ScyllaClusterManager, s3_storage):
    """T27, REF-16. A reserve set to force a refusal is refused with all three figures, and a lowered reserve is accepted. The default is 10%."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    assert await manager.api.get_config(server.ip_addr, CFG_DISK_RESERVE_PERCENT) == 10

    async def set_reserve(percent):
        await manager.server_update_config(server.server_id, CFG_DISK_RESERVE_PERCENT, percent)
        async def applied():
            return True if await manager.api.get_config(server.ip_addr, CFG_DISK_RESERVE_PERCENT) == percent else None
        await wait_for(applied, deadline())

    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows(range(4), now_us() - 3600_000_000))
        backup = await protect_table(manager, server, s3_storage, ks, 't')
        t = await wait_covered(manager, server, ks, 't')

        await set_reserve(100)
        result = await assert_refused(manager, server, cql, ks, 't', backup.name, t, Reason.DISK)
        assert result.needed_bytes > 0 and result.free_bytes > 0 and result.reserve_bytes > 0, result
        assert result.free_bytes - result.needed_bytes < result.reserve_bytes, f'the figures must explain the refusal: {result}'

        await set_reserve(1)
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, t, new)
        assert await read_all(cql, ks, new) == await read_all(cql, ks, 't')


@needs_phase('1.3')
async def test_refuse_new_table_name(manager: ScyllaClusterManager, s3_storage):
    """T28, REF-13. Taken by a table or an earlier restore, equal to the source, invalid, or in another keyspace. A name at the maximum length is accepted."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks, new_test_keyspace(manager, VNODES_KS) as other_ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {ks}.existing {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows(range(4), now_us() - 3600_000_000))
        await apply(cql, ks, 'existing', simple_rows([9], now_us() - 3600_000_000, 'keep'))
        backup = await protect_table(manager, server, s3_storage, ks, 't')
        t = await wait_covered(manager, server, ks, 't')
        await restore_ok(manager, server, ks, 't', backup.name, t, 'finished')
        watched = ['existing', 'finished']

        failures = []
        cases = [('existing', Reason.NAME_TAKEN), ('finished', Reason.NAME_TAKEN), ('t', Reason.NAME_TAKEN),
                 ('a' * 49, Reason.NAME_INVALID), ('has space', Reason.NAME_INVALID), ('has-dash', Reason.NAME_INVALID),
                 ('', Reason.NAME_INVALID), (f'{ks}.dotted', Reason.NAME_INVALID)]
        for name, reason in cases:
            try:
                await assert_refused(manager, server, cql, ks, 't', backup.name, t, reason, watched, new_table=name)
            except AssertionError as e:
                failures.append(f'{name!r}: {e}')
        assert not failures, '\n'.join(failures)

        await assert_refused(manager, server, cql, ks, 't', backup.name, t, Reason.NAME_OTHER_KEYSPACE, watched,
                             new_table='elsewhere', new_keyspace=other_ks)
        assert not await table_exists(cql, other_ks, 'elsewhere')

        longest = 'a' * 48
        await restore_ok(manager, server, ks, 't', backup.name, t, longest)
        assert await table_exists(cql, ks, longest)


@needs_phase('1.3')
async def test_table_with_conditional_writes_is_accepted(manager: ScyllaClusterManager, s3_storage):
    """T29, REF-1. LWT isn't a refused kind: a table with conditional writes restores exact at T. Depends on PM-36, the team lead's safety check.

    A conditional write can't choose its timestamp, so the expected table is built from the timestamps the source reports."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        backup = await protect_table(manager, server, s3_storage, ks, 't')

        async def conditional(statement, row, value, applied=True):
            rows = await cql.run_async(statement)
            assert rows[0][0] is applied, f'{statement}: {rows}'
            pk, ck = row
            written = await cql.run_async(f'SELECT WRITETIME(v) FROM {ks}.t WHERE pk = {pk} AND ck = {ck}')
            return W(written[0][0], f"INSERT INTO {{t}} (pk, ck, v) VALUES ({pk}, {ck}, '{value}') USING TIMESTAMP {{ts}}")

        first = await conditional(f"INSERT INTO {ks}.t (pk, ck, v) VALUES (1, 0, 'first') IF NOT EXISTS", (1, 0), 'first')
        await conditional(f"INSERT INTO {ks}.t (pk, ck, v) VALUES (1, 0, 'lost') IF NOT EXISTS", (1, 0), 'first', applied=False)
        second = await conditional(f"UPDATE {ks}.t SET v = 'second' WHERE pk = 1 AND ck = 0 IF v = 'first'", (1, 0), 'second')
        other = await conditional(f"INSERT INTO {ks}.t (pk, ck, v) VALUES (2, 0, 'other') IF NOT EXISTS", (2, 0), 'other')
        history = [first, second, other]
        assert first.ts < second.ts < other.ts, history
        latest = await wait_covered(manager, server, ks, 't')

        for t in (first.ts, second.ts - 1, second.ts, latest):
            new = unique_name('restored_')
            await restore_ok(manager, server, ks, 't', backup.name, t, new)
            await assert_exact(cql, ks, new, history, t)


@pytest.mark.parametrize('route', ['other_bucket', 'other_prefix', 'skip_flush'])
@needs_phase('1.3')
async def test_other_backup_routes_dont_protect(manager: ScyllaClusterManager, s3_storage, scylla_cluster_teardowns, route):
    """T30, SHP-5. A backup to another bucket or prefix, or from a --skip-flush snapshot, leaves the table not protected, and restore
    naming it is refused. The commands status gives then protect the table, so nothing else stood in the way."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await apply(cql, ks, 't', simple_rows(range(4), now_us() - 3600_000_000))

        if route == 'skip_flush':
            snap = unique_name('skipflush_')
            await run_nodetool(manager, server, ['nodetool', 'snapshot', '--skip-flush', '--tag', snap, ks])
        else:
            snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        bucket = s3_storage.bucket_name
        prefix = backup_prefix(ks, 't', snap)
        if route == 'other_prefix':
            prefix = f'elsewhere/{snap}'
        elif route == 'other_bucket':
            bucket = f'pitr-other-{uuid.uuid4().hex[:12]}'
            await asyncio.to_thread(create_bucket, s3_storage.get_resource(), bucket)
            def destroy_bucket():
                other = s3_storage.get_resource().Bucket(bucket)
                other.objects.all().delete()
                other.delete()
            scylla_cluster_teardowns.append(destroy_bucket)
        tid = await manager.api.backup(server.ip_addr, ks, 't', snap, s3_storage.address, bucket, prefix)
        status = await manager.api.wait_task(server.ip_addr, tid)
        assert status['state'] == 'done', 'the backup itself must work, or this case proves nothing'

        entry = await assert_stays_unprotected(manager, server, ks, 't')
        assert entry.reason == Reason.NO_BACKUP, entry
        await assert_refused(manager, server, cql, ks, 't', snap, now_us() - CLOCK_MARGIN_US, Reason.BACKUP_NOT_USABLE)

        await run_status_commands(manager, server, entry.commands)
        await wait_protected(manager, server, ks, 't')


@needs_phase('1.3')
async def test_gap_leaves_a_remaining_interval(manager: ScyllaClusterManager, s3_storage):
    """T31, REF-4. With a shipped object gone, T past it is refused, naming the object and the remaining interval. T inside that interval is
    exact with the gap still there. A gap with none remaining can't be set up from outside: that's a developer test."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.src {TABLE_SCHEMA}')
        seed = simple_rows(range(8), now_us() - 3600_000_000)
        await apply(cql, ks, 'src', seed)
        backup = await protect_table(manager, server, s3_storage, ks, 'src')
        early = simple_rows(range(8, 12), backup.start_us + STEP_US)
        await apply(cql, ks, 'src', early)
        before_gap = await wait_covered(manager, server, ks, 'src')
        known = await objects_of(s3_storage, SHIPPING_PREFIX)   # everything up to before_gap is in these

        await apply(cql, ks, 'src', simple_rows(range(12, 16), max(now_us(), before_gap + 1)))
        await wait_covered(manager, server, ks, 'src')
        objects = await objects_of(s3_storage, SHIPPING_PREFIX)
        newest = max(o.last_modified for o in objects.values()).timestamp()
        end = await wait_covered(manager, server, ks, 'src', int(newest * 1e6) + 2 * CLOCK_MARGIN_US)
        window = {k: o for k, o in objects.items() if k not in known and o.size >= 2 and is_immutable(k)}
        assert window, 'no object to remove, so this test would prove nothing'
        key = max(window, key=lambda k: (window[k].last_modified, k))
        await delete_object(s3_storage, key)

        reasons = {Reason.OBJECT_MISSING, Reason.RECORDS_MISSING}
        result = await assert_refused(manager, server, cql, ks, 'src', backup.name, end, reasons)
        assert key.rsplit('/', 1)[-1] in result.message, result
        assert result.remaining_start_us == backup.start_us, result
        assert before_gap <= result.remaining_end_us < end, f'the gap is after {iso(before_gap)}: {result}'
        times = iso_times(result.message)
        assert result.remaining_start_us in times and result.remaining_end_us in times, f'the message must give the interval: {result}'

        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 'src', backup.name, before_gap, new)
        await assert_exact(cql, ks, new, seed + early, before_gap)
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 'src', backup.name, result.remaining_end_us, new)
        assert set(range(12)) <= await pks_of(cql, ks, new)
        await assert_refused(manager, server, cql, ks, 'src', backup.name, result.remaining_end_us + 1, reasons)
        assert key not in await objects_of(s3_storage, SHIPPING_PREFIX), 'the gap must still be there for this to mean anything'


@needs_phase('1.3')
async def test_interval_starts_at_the_snapshot(manager: ScyllaClusterManager, s3_storage):
    """T32, REF-6, RST-2. The interval starts at the snapshot, not the end of the upload: a write between the two is restorable, on either side of T."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        seed = simple_rows(range(4), now_us() - 3600_000_000)
        await apply(cql, ks, 't', seed)

        before_snapshot = now_us()
        snap, _ = await take_snapshot_on_one_server(ks, server, manager, logger)
        after_snapshot = now_us()

        # Above any start the margin allows, and before the upload begins, so T here is between the snapshot and the end of the upload.
        between = simple_rows([10], after_snapshot + 2 * CLOCK_MARGIN_US)
        await apply(cql, ks, 't', between)
        async def time_passed():
            return True if now_us() > between[0].ts else None
        await wait_for(time_passed, deadline())
        upload_started = now_us()

        await do_backup(server, snap, backup_prefix(ks, 't', snap), ks, 't', s3_storage, manager, logger)
        backup = await wait_protected(manager, server, ks, 't', snap)
        assert before_snapshot - CLOCK_MARGIN_US <= backup.start_us <= after_snapshot + CLOCK_MARGIN_US, f'{backup} is not the snapshot time'
        assert backup.start_us < between[0].ts < upload_started
        await wait_covered(manager, server, ks, 't')

        history = seed + between
        for t in (backup.start_us, between[0].ts - 1, between[0].ts):
            new = unique_name('restored_')
            await restore_ok(manager, server, ks, 't', backup.name, t, new)
            await assert_exact(cql, ks, new, history, t)
        await assert_refused(manager, server, cql, ks, 't', backup.name, backup.start_us - 1, Reason.T_BEFORE_COVERAGE)


@needs_phase('1.3')
async def test_schema_change_elsewhere_isnt_a_change_to_this_table(manager: ScyllaClusterManager, s3_storage):
    """T33, REF-9, REF-1. Altering another table, or a UDT this table doesn't use, doesn't refuse. Turning durable_writes off in the
    keyspace falls under REF-1, with its own code."""
    server = await manager.server_add(config=node_config(s3_storage))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TYPE {ks}.unused (zip int)')
        await cql.run_async(f'CREATE TABLE {ks}.t {TABLE_SCHEMA}')
        await cql.run_async(f'CREATE TABLE {ks}.other {TABLE_SCHEMA}')
        seed = simple_rows(range(4), now_us() - 3600_000_000)
        await apply(cql, ks, 't', seed)
        backup = await protect_table(manager, server, s3_storage, ks, 't')

        await cql.run_async(f'ALTER TABLE {ks}.other ADD added int')
        await cql.run_async(f'ALTER TYPE {ks}.unused ADD extra int')
        latest = await wait_covered(manager, server, ks, 't')
        new = unique_name('restored_')
        await restore_ok(manager, server, ks, 't', backup.name, latest, new)
        await assert_exact(cql, ks, new, seed, latest)

        await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
                            "AND durable_writes = false")
        await assert_refused(manager, server, cql, ks, 't', backup.name, latest, Reason.DURABLE)
