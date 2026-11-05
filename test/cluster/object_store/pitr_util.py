#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""What the test_pitr_*.py tests share: the adapter to the feature's surface, setup, statements and a store that can fail.

The tests are black box, written from docs-work/requirements-must.md (issue #22). They use only what an operator has: CQL,
REST, nodetool, configuration, the bucket, and starting or killing the node. The IDs in their docstrings match docs-work/test-plan.md.

Each test is skipped until the phase it needs has landed: add the phase to LANDED_PHASES to enable it. A skipped test proves nothing.
"""

import asyncio
import json
import logging
import os
import random
import re
import shlex
import threading
import time
import uuid

from collections.abc import Mapping, Set as AbstractSet
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple

import boto3
import pytest

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.util import SortedSet

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

# Each test must fail on a build without the change, so PITR_RUN_SKIPPED=1 runs every test as if its phase had landed. With
# PITR_DEADLINE_SECONDS=10 as well, a poll of an endpoint that doesn't exist gives up quickly. Neither is for normal runs.
RUN_SKIPPED = os.environ.get('PITR_RUN_SKIPPED') == '1'


def landed(phase: str) -> bool:
    return RUN_SKIPPED or phase in LANDED_PHASES


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

CFG_ENDPOINT = 'pitr_endpoint'  # real, from #5: setting this and the bucket turns shipping on
CFG_BUCKET = 'pitr_bucket'  # real, from #5
CFG_PREFIX = 'pitr_prefix'  # real, from #5
CFG_SEAL_SECONDS = 'pitr_seal_interval_in_seconds'  # real, from #5
CFG_UNSHIPPED_LIMIT_MB = 'pitr_unshipped_limit_in_mb'  # real, from #5
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
STATUS_LIMIT_WORDS = ['experimental', 'vnodes', 'tablets', 'object lock']
RESULT_LIMIT_WORDS = {'limit 3': 'clocks', 'limit 3, batches': 'batch', 'limit 4': 'ttl', 'limit 5': 'newer than t', 'limit 6': "isn't checked",
                      'limit 15': 'refresh', 'limit 8': 'truncated'}
WRONG_WAY_BACK = ("Don't load this copy's files into the original unless it's been truncated: "
                  "later deletes in the original still win, and rows stay missing with no error.")
NOT_VERIFIED_RE = re.compile(r'Not verified: (\d+) collections may differ at this exact T; retry with T one microsecond off\.')
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
TABLETS_KS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
STRONG_KS = TABLETS_KS + " AND tablets = {'initial': 1} AND consistency = 'global'"
NONDURABLE_KS = VNODES_KS + ' AND durable_writes = false'
RF2_KS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'enabled': false}"
ALTER_KEYSPACE_RF2 = "ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}}"
ALTER_KEYSPACE_RF1_NONDURABLE = ("ALTER KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
                                 "AND durable_writes = false")
PURGEABLE_OPTIONS = "WITH gc_grace_seconds = 0 AND tombstone_gc = {'mode': 'immediate'}"
DEADLINE_SECONDS = int(os.environ.get('PITR_DEADLINE_SECONDS', 300))
# The VM's clock steps by up to about 150 ms (docs-work/baseline.md:115-116), so a comparison between a time
# measured here and one reported by the node has this margin.
CLOCK_MARGIN_US = 1_000_000
STEP_US = 1000
# Absence has no event to poll for, so a check for it watches the bucket for this long: 5 seal intervals, at S = 1 s.
# T01 requires objects to appear within this time once shipping is on, which is what shows that the window can catch shipping.
ABSENCE_WINDOW_SECONDS = 5
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


async def assert_refused(manager, server, cql, ks, cf, backup, t, reason, watched=(), new_table=None, new_keyspace=None, read_source=True) -> Result:
    """Checks the refusal rules in the requirements' Terms. `watched` are other tables that must read the same."""
    if new_table is None:
        new_table = unique_name('restored_')
    expect_absent = new_keyspace is None and not await table_exists(cql, ks, new_table)
    tables = [(ks, cf), *[(ks, w) for w in watched]] if read_source and not ks.startswith('system') else []
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


# Scenario setup, shared with test_pitr_helpers.py, which checks it on a build with no PITR.

def unsupported_tables_config(cfg: dict) -> dict:
    return cfg | {'alternator_port': 8000, 'alternator_write_isolation': 'only_rmw_uses_lwt',
                  'experimental_features': ['strongly-consistent-tables', 'logstor']}


async def create_unsupported_tables(server, cql, ks, tablet_ks, strong_ks, nondurable_ks) -> list[tuple[str, str, str]]:
    """One table of each kind REF-1 refuses. Returns its keyspace, name and reason code."""
    alternator = boto3.resource('dynamodb', endpoint_url=f'http://{server.ip_addr}:8000', region_name='us-east-1',
                                aws_access_key_id='alternator', aws_secret_access_key='secret_pass')
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
    await asyncio.to_thread(alternator.create_table, TableName='alt', BillingMode='PAY_PER_REQUEST', KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                            Tags=[{'Key': 'system:initial_tablets', 'Value': 'none'}])
    return [(tablet_ks, 't', Reason.TABLETS), (strong_ks, 't', Reason.STRONG), (nondurable_ks, 't', Reason.DURABLE),
            (ks, 'counters', Reason.COUNTERS), (ks, 'viewed', Reason.VIEW_BASE), (ks, 'a_view', Reason.VIEW_TABLE),
            (ks, 'indexed', Reason.INDEXED), (ks, 'an_index_index', Reason.INDEX_TABLE),
            (ks, 'cdc', Reason.CDC), (ks, 'cdc_scylla_cdc_log', Reason.CDC), (tablet_ks, 'logstor', Reason.LOGSTOR),
            ('system', 'local', Reason.SYSTEM), ('system_schema', 'tables', Reason.SYSTEM), ('alternator_alt', 'alt', Reason.ALTERNATOR)]


async def create_rich_table(cql, ks) -> tuple[str, list[str]]:
    """Table src with non-default options, a UDT, a static and a dropped column, and one row. Returns its DESCRIBE and its rows."""
    await cql.run_async(f'CREATE TYPE {ks}.addr (street text, zip int)')
    await cql.run_async(f'''CREATE TABLE {ks}.src (pk int, ck int, v text, a frozen<addr>, gone int, s int static, PRIMARY KEY (pk, ck))
        WITH CLUSTERING ORDER BY (ck DESC) AND comment = 'pitr' AND gc_grace_seconds = 1234
        AND compaction = {{'class': 'LeveledCompactionStrategy'}} AND compression = {{'sstable_compression': 'DeflateCompressor'}}''')
    old = now_us() - 3600_000_000
    await cql.run_async(f"INSERT INTO {ks}.src (pk, ck, v, a, gone, s) VALUES (1, 1, 'x', {{street: 'y', zip: 2}}, 3, 4) USING TIMESTAMP {old}")
    await cql.run_async(f'ALTER TABLE {ks}.src DROP gone')
    return (await cql.run_async(f'DESCRIBE TABLE {ks}.src'))[0].create_statement, await read_all(cql, ks, 'src')


SCHEMA_CHANGES = ['TABLE {ks}.t ADD added int', 'TABLE {ks}.t DROP w', "TABLE {ks}.t WITH comment = 'changed'", 'TYPE {ks}.addr ADD extra int']


def schema_change_statement(change: str, ks: str) -> str:
    return 'ALTER ' + change.format(ks=ks)


async def create_alterable_table(cql, ks):
    await cql.run_async(f'CREATE TYPE IF NOT EXISTS {ks}.addr (zip int)')
    await cql.run_async(f'CREATE TABLE {ks}.t (pk int, ck int, v text, w int, a frozen<addr>, PRIMARY KEY (pk, ck))')


async def write_and_purge(manager, server, cql, ks):
    """Row 1 written and deleted, row 2 written, in table purged. Returns once row 1 and its tombstone are out of the data."""
    ts = now_us() - 3600_000_000
    await apply(cql, ks, 'purged', simple_rows([1, 2], ts))
    await cql.run_async(f'DELETE FROM {ks}.purged USING TIMESTAMP {ts + 1} WHERE pk = 1')
    await manager.api.keyspace_flush(server.ip_addr, ks, 'purged')
    # A tombstone can't be purged in the second it was written, so compaction is repeated until it's gone.
    # If it never goes, the case would prove nothing, and the wait fails.
    async def purged():
        await manager.api.keyspace_compaction(server.ip_addr, ks, 'purged')
        fragments = await cql.run_async(f'SELECT * FROM MUTATION_FRAGMENTS({ks}.purged)')
        return True if {f.pk for f in fragments} == {2} else None
    await wait_for(purged, deadline())
    return ts


async def write_row_flush_then_delete_it(manager, server, cql, ks):
    """Rows 0 and 1 in table t, flushed, then row 0's delete, so that the row and its tombstone are in different sstables. Grace is 0 and nothing compacts,
    so a snapshot taken now holds both. Returns once the data shows row 0 still there, or this would be the purged case again."""
    ts = old_ts()
    await apply(cql, ks, 't', simple_rows([0, 1], ts))
    await manager.api.keyspace_flush(server.ip_addr, ks, 't')
    await apply(cql, ks, 't', [W(ts + 1, 'DELETE FROM {t} USING TIMESTAMP {ts} WHERE pk = 0')])
    await manager.api.keyspace_flush(server.ip_addr, ks, 't')


async def row_is_in_the_data(cql, ks, pk) -> bool:
    fragments = await cql.run_async(f'SELECT * FROM MUTATION_FRAGMENTS({ks}.t)')
    return any(f.pk == pk and f.mutation_fragment_kind == 'clustering row' for f in fragments)


async def write_several_sstables(manager, server, cql, ks):
    """Uncompressed, so that an upload has real bytes, and several sstables, so that an upload cut part-way has finished some objects."""
    for first in range(0, 1200, 300):
        await apply(cql, ks, 't', simple_rows(range(first, first + 300), old_ts(), 'x' * 1024))
        await manager.api.keyspace_flush(server.ip_addr, ks, 't')


UNCOMPRESSED_SCHEMA = f"{TABLE_SCHEMA} WITH compression = {{'sstable_compression': ''}}"


async def conditional_history(cql, ks) -> list:
    """Conditional writes on table t, one not applied. A conditional write can't choose its timestamp, so this returns
    plain statements with the timestamps the table reports."""
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
    # Only writes to one partition are ordered. The other partition's write has a timestamp of its own, from its own shard's
    # clock, which steps on this VM. A repeat saw it below the second write.
    assert first.ts < second.ts, [first, second]
    return [first, second, other]


# Statements, and the independent answer for "exact at T".

class W(NamedTuple):
    ts: int
    cql: str


class Batch(NamedTuple):
    """Parts with different timestamps. The requirements merge a batch first, so a value the batch itself overwrote doesn't count as
    written. `merged` says by hand which parts survive that, when it isn't all of them."""
    parts: list[W]
    merged: list[W] | None = None


async def apply(cql, ks, table, statements, t=None):
    """Applies statements in order. With T, it applies them minus every one with a timestamp above T, merging
    each batch and then splitting it, which is the requirements' definition of exact at T."""
    def text(w):
        return w.cql.format(t=f'{ks}.{table}', ts=w.ts)
    for s in statements:
        if isinstance(s, Batch):
            if t is None:
                await cql.run_async('BEGIN UNLOGGED BATCH ' + '; '.join(text(w) for w in s.parts) + ' APPLY BATCH')
            else:
                for w in s.parts if s.merged is None else s.merged:
                    if w.ts <= t:
                        await cql.run_async(text(w))
        elif t is None or s.ts <= t:
            await cql.run_async(text(s))


def _plain(value):
    """A collection as a sorted list, so that an answer doesn't depend on the driver's types."""
    if isinstance(value, Mapping):
        return sorted(value.items())
    if isinstance(value, (AbstractSet, SortedSet)):
        return sorted(value)
    return value


async def read_exact(cql, ks, table) -> list[str]:
    return sorted(repr(tuple(_plain(v) for v in r)) for r in await cql.run_async(f'SELECT {EXACT_COLUMNS} FROM {ks}.{table}'))


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
    assert actual == expected, (f'{restored} is not exact at {iso(t)}. Expected but missing: {sorted(set(expected) - set(actual))}. '
                                f'Present but not expected: {sorted(set(actual) - set(expected))}')
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


def collection_edge_statements(pks, seed_ts, base) -> tuple[list, list, dict[str, int]]:
    """The cases limit 3 names, one row each. ck 0: a collection overwrite. ck 1: an empty overwrite. ck 2: a delete batched with an
    element 1 microsecond later. ck 3 and ck 4: a batch that overwrites its own cell, and its own collection element."""
    ts = {'overwrite': base + 1000, 'empty': base + 2000, 'delete': base + 3000, 'added': base + 3001, 'low': base + 4000, 'high': base + 5000}
    seed, changes = [], []
    for pk in pks:
        for ck in range(5):
            seed.append(W(seed_ts, f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET v = 'seed', m = {{{{1: 'a', 2: 'b'}}}} WHERE pk = {pk} AND ck = {ck}"))
        changes.append(W(ts['overwrite'], f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET m = {{{{2: 'over'}}}} WHERE pk = {pk} AND ck = 0"))
        changes.append(W(ts['empty'], f"UPDATE {{t}} USING TIMESTAMP {{ts}} SET m = {{{{}}}} WHERE pk = {pk} AND ck = 1"))
        changes.append(Batch([W(ts['delete'], f"DELETE m FROM {{t}} USING TIMESTAMP {ts['delete']} WHERE pk = {pk} AND ck = 2"),
                              W(ts['added'], f"UPDATE {{t}} USING TIMESTAMP {ts['added']} SET m[2] = 'added' WHERE pk = {pk} AND ck = 2")]))
        cell_kept = W(ts['high'], f"UPDATE {{t}} USING TIMESTAMP {ts['high']} SET v = 'kept' WHERE pk = {pk} AND ck = 3")
        element_kept = W(ts['high'], f"UPDATE {{t}} USING TIMESTAMP {ts['high']} SET m[1] = 'kept' WHERE pk = {pk} AND ck = 4")
        changes.append(Batch([W(ts['low'], f"UPDATE {{t}} USING TIMESTAMP {ts['low']} SET v = 'overwritten' WHERE pk = {pk} AND ck = 3"), cell_kept,
                              W(ts['low'], f"UPDATE {{t}} USING TIMESTAMP {ts['low']} SET m[1] = 'overwritten' WHERE pk = {pk} AND ck = 4"), element_kept],
                             merged=[cell_kept, element_kept]))
    return seed, changes, ts


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


async def assert_status_stays(manager, server, ks, cf, holds, expected: str) -> TableStatus:
    """As above, for status: what must not change has to stay that way for the whole window. Fails at once if it doesn't."""
    end = time.time() + ABSENCE_WINDOW_SECONDS
    while True:
        entry = await table_status(manager, server, ks, cf)
        assert holds(entry), f'expected {expected}, saw: {entry}'
        if time.time() >= end:
            return entry
        await asyncio.sleep(0.2)


async def assert_stays_unprotected(manager, server, ks, cf) -> TableStatus:
    return await assert_status_stays(manager, server, ks, cf, lambda e: e.state == NOT_PROTECTED and not e.backups, 'not protected, with no usable backup')


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
    """The store behind a TCP pass-through which the test can cut or slow, for store failures from outside. Nothing in
    test/pylib can make a store fail on demand: s3_proxy.py only injects random retryable errors for a whole run. It
    carries bytes unchanged, so it knows nothing about the protocol.

    It has a thread and an event loop of its own. It has to outlive the test's event loop, because the node keeps using
    the store until the cluster is torn down, and a blocking call in a test mustn't stall the store."""
    def __init__(self, storage):
        self._storage = storage
        self.port = None
        self.down = False
        self.connections = 0
        self._server = None
        self._conns = set()
        self._budget = None
        self._held = False
        self._loop = None
        self._thread = None

    def __getattr__(self, name):
        return getattr(self._storage, name)

    @property
    def address(self):
        return f'http://{self._storage.ip}:{self.port}'

    def create_endpoint_conf(self):
        return create_conf(self.address, self._storage.region)

    def start(self):
        listening = threading.Event()

        def run():
            self._loop = asyncio.new_event_loop()

            async def listen():
                self._server = await asyncio.start_server(self._serve, self._storage.ip, 0)
                self.port = self._server.sockets[0].getsockname()[1]
                listening.set()
            self._loop.run_until_complete(listen())
            self._loop.run_forever()
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.close()
        self._thread = threading.Thread(target=run, daemon=True, name='GatedStore')
        self._thread.start()
        assert listening.wait(30), 'the gate did not start'

    def stop(self):
        self.fail()

        def close():
            self._server.close()
            self._loop.stop()
        self._loop.call_soon_threadsafe(close)
        self._thread.join(30)

    def _abort_connections(self):
        for w in self._conns:
            w.transport.abort()
        self._conns.clear()

    def fail(self):
        self.down = True
        self._loop.call_soon_threadsafe(self._abort_connections)

    def fail_after_bytes(self, count):
        """Cuts the store once this many more bytes have gone towards it, whatever they are. No moment to hit."""
        self._budget = count

    def hold(self):
        """Stops carrying bytes, without cutting anything. Whatever needs the store waits. No moment to hit."""
        self._held = True

    def recover(self):
        self.down = False
        self._budget = None
        self._held = False

    async def _serve(self, reader, writer):
        if self.down:
            writer.transport.abort()
            return
        self.connections += 1
        self._conns.add(writer)
        up_reader, up_writer = await asyncio.open_connection(self._storage.ip, self._storage.port)
        # A cut can land while the connection above opens. It must not survive that.
        if self.down:
            writer.transport.abort()
            up_writer.transport.abort()
            return
        self._conns.add(up_writer)

        async def pump(src, dst, towards_store):
            try:
                while data := await src.read(65536):
                    while self._held and not self.down:
                        await asyncio.sleep(0.01)
                    if towards_store and self._budget is not None:
                        self._budget -= len(data)
                        if self._budget <= 0:
                            self.fail()
                    if self.down:
                        return
                    dst.write(data)
                    await dst.drain()
            except ConnectionError:
                pass
            finally:
                dst.close()
                self._conns.discard(dst)
        await asyncio.gather(pump(reader, up_writer, True), pump(up_reader, writer, False))


@pytest.fixture
def gated_store(s3_storage, scylla_cluster_teardowns) -> GatedStore:
    gate = GatedStore(s3_storage)
    gate.start()
    scylla_cluster_teardowns.append(gate.stop)
    return gate


OLD_TS_US = 3600_000_000    # how far back seed data is written, so that it's always older than the backup


def old_ts() -> int:
    return now_us() - OLD_TS_US


@dataclass
class ProtectedTable:
    """A node, and table t in a vnode keyspace, seeded and protected by one backup. Nothing else."""
    manager: Any
    server: Any
    cql: Any
    storage: Any
    ks: str
    backup: Backup
    seed: list
    cf: str = 't'

    async def covered(self, us=None) -> int:
        return await wait_covered(self.manager, self.server, self.ks, self.cf, us)

    async def write(self, statements):
        await apply(self.cql, self.ks, self.cf, statements)

    async def status(self) -> TableStatus:
        return await table_status(self.manager, self.server, self.ks, self.cf)

    async def restore(self, t, new_table=None, backup=None) -> tuple[Result, str]:
        """A restore that has to succeed. Returns its result and the new table's name."""
        new_table = new_table or unique_name('restored_')
        result = await restore_ok(self.manager, self.server, self.ks, self.cf, backup or self.backup.name, t, new_table)
        return result, new_table

    async def refused(self, t, reason, backup=None, **kwargs) -> Result:
        return await assert_refused(self.manager, self.server, self.cql, self.ks, self.cf, backup or self.backup.name, t, reason, **kwargs)

    async def assert_restored_exact(self, history, t) -> list[str]:
        _, new = await self.restore(t)
        return await assert_exact(self.cql, self.ks, new, history, t)

    async def restart(self, kill=False):
        self.cql = await restart(self.manager, self.server, kill)


@asynccontextmanager
async def protected_table(manager, storage, seed=None, schema=TABLE_SCHEMA, **config):
    server = await manager.server_add(config=node_config(storage, **config))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {schema}')
        seed = simple_rows(range(4), old_ts()) if seed is None else seed
        await apply(cql, ks, 't', seed)
        backup = await protect_table(manager, server, storage, ks, 't')
        yield ProtectedTable(manager, server, cql, storage, ks, backup, seed)
