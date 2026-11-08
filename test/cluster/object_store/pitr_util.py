#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""What the test_pitr_*.py tests share: the adapter to the feature's surface, setup, statements and a store that can fail.

The tests are black box, written from docs-work/requirements-must.md and the slim plan, docs-work/test-plan-slim.md (issue #22).
They use only what an operator has: CQL, REST, nodetool, configuration, the bucket, and starting or killing the node.

Each test is skipped until the product pieces it needs have landed: add them to LANDED_PHASES to enable it. A skipped test proves nothing.
"""

import asyncio
import logging
import os
import re
import shlex
import threading
import time

from collections.abc import Mapping, Set as AbstractSet
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, NamedTuple

import aiohttp
import botocore.exceptions
import pytest

from cassandra.metadata import Murmur3Token
from cassandra.util import SortedSet

from test.cluster.object_store.test_backup import do_backup, take_snapshot_on_one_server
from test.cluster.util import new_test_keyspace, reconnect_driver, wait_for_cql_and_get_hosts
from test.pylib.rest_client import HTTPError
from test.pylib.s3mock_server import create_conf
from test.pylib.util import unique_name, wait_for

logger = logging.getLogger(__name__)

# The product pieces the tests wait for (docs-work/test-plan-slim.md, section 2):
#   ep.3    the replay_to endpoint fetches and replays the history and ends in a minimal `done` (with ep.1 and ep.2 before it)
#   st.1    the status GET (#14)
#   15-bc   the refusals 15-b and 15-c, with the P68 messages
#   12-2.2  the upload-finished marker of a backup
# Add a piece here when it lands, to enable the tests that need it.
LANDED_PHASES: tuple[str, ...] = ('ep.3',)

# Each test must fail on a build without the change, so PITR_RUN_SKIPPED=1 runs every test as if its pieces had landed. With
# PITR_DEADLINE_SECONDS=10 as well, a poll of an endpoint that doesn't exist gives up quickly. Neither is for normal runs.
RUN_SKIPPED = os.environ.get('PITR_RUN_SKIPPED') == '1'


def landed(phase: str) -> bool:
    return RUN_SKIPPED or phase in LANDED_PHASES


def needs_phase(*phases: str):
    """Skips a test until every piece it needs lands. A typed skip, because test.py reports a bare pytest.mark.skip as an error."""
    missing = [p for p in phases if not landed(p)]
    if not missing:
        return lambda test: test
    return pytest.mark.skip_bug(link="https://github.com/ofirwe86-weissberg/scylladb/issues/22",
                                reason=f"PITR pieces {', '.join(missing)} haven't landed")


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
# A rename changes this section only.
####################################################################################################

SHIPPING_PREFIX = 'pitr-log'    # the value the tests give pitr_prefix: every shipped object is under it
BACKUP_PREFIX = 'backups'       # where the tests put a backup, as the README tells the user

CFG_ENDPOINT = 'pitr_endpoint'  # db/config.cc: setting this and the bucket turns shipping on
CFG_BUCKET = 'pitr_bucket'
CFG_PREFIX = 'pitr_prefix'
CFG_SEAL_SECONDS = 'pitr_seal_interval_in_seconds'
CFG_UNSHIPPED_LIMIT_MB = 'pitr_unshipped_limit_in_mb'

# POST, api/api-doc/storage_service.json on pitr/18-ep1b-endpoint. Returns a task manager id. The outcome is the task's
# state, and a refusal is its failure text.
REPLAY_URI = '/storage_service/pitr/replay_to'
# GET. Proposed: st.1 hasn't defined it yet. Until it lands, the coverage wait falls back to the probe below.
STATUS_URI = '/storage_service/pitr/status'

DONE = 'done'
FAILED = 'failed'
SEAL_SECONDS = 1


class Reason:
    """Reason codes in a refusal's text. All but the last three are real: db/pitr/refusal.cc on pitr/18-ep3c-splits."""
    T_INVALID = 't_invalid'
    T_PAST_COVERAGE = 't_past_coverage'
    RECORDS_MISSING = 'protection_records_missing'
    RECORDS_DAMAGED = 'protection_records_damaged'
    BACKUP_BEFORE_SHIPPING = 'backup_before_shipping'
    STORE_LOCATION_MISSING = 'store_location_missing'
    TARGET_MISSING = 'target_missing'
    TARGET_NOT_EMPTY = 'target_not_empty'
    TARGET_SCHEMA_DIFFERS = 'target_schema_differs'
    TARGET_AUTO_COMPACTION = 'target_auto_compaction_enabled'
    TARGET_ALREADY_REPLAYED = 'target_already_replayed'
    BACKUP_FILE_MISSING = 'backup_file_missing'
    BOUND_FILE_DAMAGED = 'bound_file_damaged'
    BACKUP_NAMES_ANOTHER_TABLE = 'backup_names_another_table'
    REPLAY_ALREADY_RUNNING = 'replay_already_running'
    BACKUP_MANIFEST_DAMAGED = 'backup_manifest_damaged'
    BACKUP_FILE_TOO_LARGE = 'backup_file_too_large'
    TARGET_USES_TABLETS = 'target_uses_tablets'
    SEGMENT_MISSING = 'shipped_object_missing'
    SEGMENT_DAMAGED = 'shipped_object_damaged'
    # Proposed: no real code exists yet for these. They need 15-b/c (TRUNCATE, schema change) and 12-2.2 (upload unfinished),
    # which aren't built (their tests are untouched). Change the spelling here when the piece lands and names its code. A compaction during a replay has no
    # code either: T3 accepts any real one.
    TRUNCATED = 'source_truncated'
    SCHEMA_CHANGED = 'source_schema_changed'
    BACKUP_UNFINISHED = 'backup_upload_unfinished'


KNOWN_REASONS = tuple(v for k, v in vars(Reason).items() if not k.startswith('_'))
# The history between the backup and T is damaged or has a gap (REF-4).
HISTORY_DAMAGED = {Reason.RECORDS_MISSING, Reason.RECORDS_DAMAGED, Reason.SEGMENT_MISSING, Reason.SEGMENT_DAMAGED}

# RTN-1, verbatim: requirements-must.md.
WRONG_WAY_BACK = ("Don't load this copy's files into the original unless it's been truncated: "
                  "later deletes in the original still win, and rows stay missing with no error.")
# The line the node logs when a replay ends in `done` (db/pitr/replay_to.cc, format_replay_result). Tasks carry no success text
# (#100), so the log is where the result is.
RESULT_MARK = 'is done: restored from backup'
PAST_COVERAGE_WORDS = ['lag']
# P68 (PM-67): the refusal on a done table says where the result was written.
RESULT_LOGGED_WORDS = ['log']
# A shipped segment's object key is <prefix>/<host id>/<shard>/<chain>/segments/<file name>.
SEGMENT_KEY_RE = re.compile(r'/(\d+)/([^/]+)/segments/(\S+)$')


@dataclass
class Outcome:
    """How a replay ended, read from its task."""
    state: str
    reason: str | None
    message: str            # the failure text: the refusal's reason and next action
    result_text: str        # every string the finished task carries, where a `done` result is looked for
    raw: Any = None


@dataclass
class Status:
    """The node's PITR status (STA-1 as changed by P68): shipping state, covered-through, usable backups, `incomplete` tables."""
    state: str
    break_cause: str | None
    covered_through_us: int | None
    backups: list[tuple[str, int]] = field(default_factory=list)       # (snapshot, its time)
    incomplete: list[tuple[str, str]] = field(default_factory=list)    # (keyspace, table)
    raw: Any = None


def _strings(j):
    if isinstance(j, str):
        yield j
    elif isinstance(j, Mapping):
        for v in j.values():
            yield from _strings(v)
    elif isinstance(j, list):
        for v in j:
            yield from _strings(v)


def reason_in(text: str) -> str | None:
    """The reason code a refusal's text carries: a leading `<code>:` if there is one, else the first known code in it as a
    whole word. Matching words is the fallback, because the product doesn't yet promise where the code goes."""
    m = re.match(r'\s*([a-z][a-z0-9_]*)\s*:', text)
    if m and m[1] in KNOWN_REASONS:
        return m[1]
    found = [(m.start(), code) for code in KNOWN_REASONS for m in [re.search(rf'\b{code}\b', text)] if m]
    return min(found)[1] if found else None


def parse_outcome(task) -> Outcome:
    error = task.get('error') or ''
    return Outcome(state=task['state'], reason=reason_in(error) if task['state'] == FAILED else None, message=error,
                   result_text='\n'.join(_strings(task)), raw=task)


def parse_status(j) -> Status:
    return Status(state=j['state'], break_cause=j.get('break_cause'),
                  covered_through_us=from_iso(j['covered_through']) if j.get('covered_through') else None,
                  backups=[(b['snapshot'], from_iso(b['time'])) for b in j.get('backups', [])],
                  incomplete=[(x['keyspace'], x['table']) for x in j.get('incomplete', [])], raw=j)


async def start_replay(manager, server, *, endpoint, bucket, prefix, ks, target, snapshot, t) -> str:
    """Returns the task id. T is microseconds, or a string to pass as it is."""
    params = {'endpoint': endpoint, 'bucket': bucket, 'prefix': prefix, 'keyspace': ks, 'table': target, 'snapshot': snapshot,
              't': iso(t) if isinstance(t, int) else t}
    return await manager.api.client.post_json(REPLAY_URI, host=server.ip_addr, params=params)


async def wait_replay(manager, server, task_id) -> Outcome:
    """Polls the task with a deadline: wait_task has none, and a hung replay must fail the test rather than the run."""
    async def ended():
        task = await manager.api.get_task_status(server.ip_addr, task_id)
        return task if task['state'] in (DONE, FAILED) else None
    return parse_outcome(await wait_for(ended, deadline(), label=f'replay task {task_id}'))


async def result_line(pitr) -> str | None:
    """The `done` result line of a replay into this node, from its log, or None if there is none."""
    log = await pitr.manager.server_open_log(server_id=pitr.server.server_id)
    found = await log.grep(re.escape(RESULT_MARK))
    return found[-1][0] if found else None


async def get_status(manager, server) -> Status | None:
    """None while st.1 hasn't landed and the URI doesn't exist. Once it has, a missing status is an error."""
    try:
        return parse_status(await manager.api.client.get_json(STATUS_URI, host=server.ip_addr))
    except HTTPError as e:
        # RUN_SKIPPED must not claim the endpoint exists: the status GET (#14) isn't built, so a 404 is the answer.
        if e.code == 404 and 'st.1' not in LANDED_PHASES:
            return None
        raise


def target_statement(schema_cql: str, ks: str, target: str) -> str:
    """The CREATE TABLE the README tells the user to run: the backup's own schema.cql, renamed, without its ID, with
    compaction disabled."""
    text = re.sub(r'/\*.*?\*/', '', schema_cql, flags=re.S)
    text = '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))
    creates = [s.strip() for s in text.split(';') if re.match(r'\s*CREATE\s+TABLE\b', s, re.I)]
    assert len(creates) == 1, f'expected one CREATE TABLE in schema.cql: {schema_cql}'
    stmt, renamed = re.subn(r'^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?("?\w+"?)\.("?\w+"?)', f'CREATE TABLE {ks}.{target}', creates[0], flags=re.I)
    assert renamed == 1, f'no table name in {creates[0]}'
    stmt = re.sub(r'\bID\s*=\s*[0-9a-fA-F-]{36}\s*', '', stmt)
    stmt = re.sub(r'\bWITH\s+AND\b', 'WITH', stmt, flags=re.I)
    stmt = re.sub(r'\bAND\s+AND\b', 'AND', stmt, flags=re.I)
    stmt = re.sub(r'\s+(AND|WITH)\s*$', '', stmt, flags=re.I)
    disabled = "compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}"
    stmt, replaced = re.subn(r"\bcompaction\s*=\s*\{[^}]*\}", disabled, stmt, flags=re.I)
    if not replaced:
        stmt += f' AND {disabled}' if re.search(r'\)\s*WITH\b', stmt, re.I) else f' WITH {disabled}'
    return stmt

####################################################################################################
# End of the adapter.
####################################################################################################

VNODES_KS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}"
PURGEABLE_OPTIONS = "WITH gc_grace_seconds = 0 AND tombstone_gc = {'mode': 'immediate'}"
DEADLINE_SECONDS = int(os.environ.get('PITR_DEADLINE_SECONDS', 300))
# The VM's clock steps by up to about 150 ms (docs-work/baseline.md:115-116), so a comparison between a time
# measured here and one reported by the node has this margin.
CLOCK_MARGIN_US = 1_000_000
STEP_US = 1000
SHARDS = 2                      # test/pylib/scylla_server.py:153
IGNORE_MSB_BITS = 12            # the default of murmur3_partitioner_ignore_msb_bits
FAR_FUTURE_US = 86400_000_000   # how far past now the coverage probe asks for, so that it's always refused
TABLE_SCHEMA = '(pk int, ck int, v text, w int, s int static, m map<int, text>, st set<int>, PRIMARY KEY (pk, ck))'
EXACT_COLUMNS = ('pk, ck, v, w, s, m, st, WRITETIME(v), WRITETIME(w), WRITETIME(s), '
                 'WRITETIME(m[1]), WRITETIME(m[2]), WRITETIME(st[1]), WRITETIME(st[2])')
UNCOMPRESSED_SCHEMA = f"{TABLE_SCHEMA} WITH compression = {{'sstable_compression': ''}}"
ISO_RE = re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z')
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def iso(us: int) -> str:
    return (EPOCH + timedelta(microseconds=us)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def from_iso(text: str) -> int:
    return (datetime.fromisoformat(text.replace('Z', '+00:00')) - EPOCH) // timedelta(microseconds=1)


def iso_times(text: str) -> list[int]:
    return [from_iso(m) for m in ISO_RE.findall(text)]


def latest_restorable(outcome: Outcome, t: int) -> int:
    """The latest restorable time a REF-5 refusal of T states: the latest time in its message below T. The message may
    repeat T, which is never restorable, so T and anything after it don't count."""
    below = [x for x in iso_times(outcome.message) if x < t]
    assert below, f'a refusal past coverage must state the latest restorable time: {outcome}'
    return max(below)


def now_us() -> int:
    return time.time_ns() // 1000


def later_than(previous_us: int) -> int:
    """A timestamp after previous_us even if the clock was stepped back since it was taken: the host's time sync does that by
    about 150 ms, which put a later now_us() before an earlier one."""
    return max(now_us(), previous_us + 1)


def deadline() -> float:
    return time.time() + DEADLINE_SECONDS


LAG_RE = re.compile(r'a lag of (\d+)\s?(?:us|\u00b5s|\u03bcs)')


def lag_us(text: str) -> int:
    """The `a lag of <n>us` a refusal past coverage states, in microseconds. The unit may print as `us` or `\u00b5s`; the
    number is exact, whatever the precision of the ISO times beside it."""
    m = LAG_RE.search(text)
    assert m, f'no `a lag of <n>us` in: {text}'
    return int(m[1])


async def wait_log(log, pattern: str, from_mark: int | None = None) -> re.Match:
    """Waits until the node's log has a line matching pattern, from a mark if given, and returns the match. It fails at the
    test's deadline instead of hanging, which ScyllaLogFile.wait_for would for 10 minutes."""
    try:
        _, found = await log.wait_for(pattern, from_mark=from_mark, timeout=max(deadline() - time.time(), 0.001))
    except TimeoutError:
        raise AssertionError(f'the log has no line matching {pattern!r}') from None
    return found[0][1]


FETCH_DIR_RE = r'fetching the history into (\S+)\n'


async def fetch_dir(log, from_mark: int | None = None) -> str:
    """The directory a replay fetches the shipped segments into, from the node's INFO line `fetching the history into <dir>`."""
    return (await wait_log(log, FETCH_DIR_RE, from_mark)).group(1)


def count_files(directory: str) -> int:
    """Every file under directory, whatever its size; 0 if it doesn't exist (yet, or any more)."""
    return sum(len(files) for _, _, files in os.walk(directory))


async def abort_task(manager, server, task_id: str):
    """Aborts a task through the task manager's REST API. A refused abort raises HTTPError: the call returned 200 or it fails."""
    await manager.api.abort_task(server.ip_addr, task_id)


async def post_raw(manager, server, uri: str, params: Mapping[str, str]) -> tuple[int, str]:
    """POSTs to the node's REST API and returns the status and body whatever they are, where start_replay raises on a non-200,
    for a test of how a call is rejected. A parameter is left out by leaving it out of params."""
    url = f'http://{server.ip_addr}:{manager.api.client.default_port}{uri}'
    async with aiohttp.request('POST', url, params=params, timeout=aiohttp.ClientTimeout(total=DEADLINE_SECONDS)) as resp:
        return resp.status, await resp.text()


def backup_prefix(ks, cf, snap) -> str:
    return f'{BACKUP_PREFIX}/{ks}/{cf}/{snap}'


def node_config(storage, shipping=True, ship_to=None, also=(), **extra) -> dict:
    """`storage` is the endpoint backups and replays name. Shipping goes to `ship_to`, by default the same one. `also` are
    other endpoints to configure, so that one test can ship through a gate and back up around it, or the other way round."""
    ship_to = ship_to or storage
    endpoints = storage.create_endpoint_conf()
    for other in (ship_to, *also):
        endpoints += [e for e in other.create_endpoint_conf() if e['name'] not in {x['name'] for x in endpoints}]
    cfg = {'object_storage_endpoints': endpoints, 'task_ttl_in_seconds': 300}
    if shipping:
        cfg |= {CFG_ENDPOINT: ship_to.address, CFG_BUCKET: ship_to.bucket_name, CFG_PREFIX: SHIPPING_PREFIX, CFG_SEAL_SECONDS: SEAL_SECONDS}
    return cfg | extra


async def read_all(cql, ks, table) -> list[str]:
    return sorted(repr(tuple(r)) for r in await cql.run_async(f'SELECT * FROM {ks}.{table}'))


async def create_target(cql, storage, ks, target, prefix):
    """Creates the target the way the README tells the user: from the backup's schema.cql in the bucket."""
    schema = (await get_object(storage, f'{prefix}/schema.cql')).decode()
    await cql.run_async(target_statement(schema, ks, target))


# Scenario setup, shared with test_pitr_helpers.py, which checks it on a build with no PITR.

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


async def write_and_purge(manager, server, cql, ks, table='purged', purged_pk=1, kept_pks=(2,)):
    """Row purged_pk written and deleted, rows kept_pks written. Returns once purged_pk and its tombstone are out of the data."""
    ts = now_us() - 3600_000_000
    await apply(cql, ks, table, simple_rows([purged_pk, *kept_pks], ts))
    await cql.run_async(f'DELETE FROM {ks}.{table} USING TIMESTAMP {ts + 1} WHERE pk = {purged_pk}')
    await manager.api.keyspace_flush(server.ip_addr, ks, table)
    # A tombstone can't be purged in the second it was written, so compaction is repeated until it's gone.
    # If it never goes, the case would prove nothing, and the wait fails.
    async def purged():
        await manager.api.keyspace_compaction(server.ip_addr, ks, table)
        fragments = await cql.run_async(f'SELECT * FROM MUTATION_FRAGMENTS({ks}.{table})')
        return True if {f.pk for f in fragments} == set(kept_pks) else None
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


async def write_several_sstables(manager, server, cql, ks, table='t'):
    """Uncompressed, so that an upload has real bytes, and several sstables, so that an upload cut part-way has finished some objects."""
    for first in range(0, 1200, 300):
        await apply(cql, ks, table, simple_rows(range(first, first + 300), old_ts(), 'x' * 1024))
        await manager.api.keyspace_flush(server.ip_addr, ks, table)


# Statements, and the independent answer for "exact at T".

class W(NamedTuple):
    """A statement and its timestamp. With values, it's prepared and bound, so that a large value isn't parsed as CQL text."""
    ts: int
    cql: str
    values: tuple = ()


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

    async def run(w):
        if w.values:
            await cql.run_async(cql.prepare(text(w)), w.values)
        else:
            await cql.run_async(text(w))
    for s in statements:
        if isinstance(s, Batch):
            if t is None:
                await cql.run_async('BEGIN UNLOGGED BATCH ' + '; '.join(text(w) for w in s.parts) + ' APPLY BATCH')
            else:
                for w in s.parts if s.merged is None else s.merged:
                    if w.ts <= t:
                        await run(w)
        elif t is None or s.ts <= t:
            await run(s)


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
    """Builds a reference table now, after the replay it's compared with, so it can't be in the history
    that replay read."""
    ref = unique_name('ref_')
    await cql.run_async(f'CREATE TABLE {ks}.{ref} {TABLE_SCHEMA} {table_options}')
    await apply(cql, ks, ref, statements, t)
    return await read_exact(cql, ks, ref)


def _short(rows) -> list[str]:
    return [r if len(r) <= 200 else r[:200] + f'... ({len(r)} chars)' for r in rows]


async def assert_exact(cql, ks, restored, statements, t) -> list[str]:
    expected = await expected_at(cql, ks, statements, t)
    actual = await read_exact(cql, ks, restored)
    assert actual == expected, (f'{restored} is not exact at {iso(t)}. Expected but missing: {_short(sorted(set(expected) - set(actual)))}. '
                                f'Present but not expected: {_short(sorted(set(actual) - set(expected)))}')
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


def token_of(pk: int) -> int:
    """The murmur3 token of an int partition key, as the driver computes it for routing."""
    return Murmur3Token.hash_fn(pk.to_bytes(4, 'big', signed=True))


def keys_on_shard(shard: int, count: int, start: int = 0) -> list[int]:
    """The first `count` int keys from `start` that land on the shard. test_pitr_helpers.py checks this against the node."""
    out, pk = [], start
    while len(out) < count:
        if shard_of(token_of(pk)) == shard:
            out.append(pk)
        pk += 1
    return out


def keys_on_every_shard(per_shard: int, start: int = 0) -> list[int]:
    return [pk for shard in range(SHARDS) for pk in keys_on_shard(shard, per_shard, start)]


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
    try:
        out, err = await proc.communicate()
    except asyncio.CancelledError:
        proc.kill()  # a caller's timeout must not leave nodetool running
        raise
    assert proc.returncode == 0, f'{argv} failed: {out} {err}'


async def run_status_commands(manager, server, commands):
    """Runs commands unchanged, with real nodetool. That's what makes them exact."""
    assert commands, 'expected exact commands'
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


RETRY_DELAYS = (0.2, 0.5, 1, 2)     # seconds between attempts after the connection was dropped under a tamper call; the last repeats


def _retrying(storage, what: str, call):
    """Runs call(resource) with a fresh resource each time, again if the mock store dropped the connection under it
    (#111: a keep-alive connection the mock has closed). Only a closed or reset connection is retried, with a growing pause,
    until the test's deadline, and each retry is logged: any other error, and a failure that outlasts the deadline, reaches the
    test as it is."""
    end = deadline()
    for attempt in range(1 << 30):
        try:
            return call(storage.get_resource())
        except (botocore.exceptions.ConnectionClosedError, ConnectionResetError) as e:
            delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
            if time.time() + delay > end:
                raise
            logger.warning(f'{what}: connection dropped ({type(e).__name__}), retry {attempt + 1} after {delay} s')
            time.sleep(delay)


async def put_object(storage, key, data: bytes):
    await asyncio.to_thread(_retrying, storage, f'put {key}', lambda r: r.Object(storage.bucket_name, key).put(Body=data))


async def delete_object(storage, key):
    await asyncio.to_thread(_retrying, storage, f'delete {key}', lambda r: r.Object(storage.bucket_name, key).delete())


async def segment_keys(storage, host, shard) -> set[str]:
    """The keys of the segments shipped by one shard of one node: <prefix>/<host id>/<shard>/<chain>/segments/<file>. A chain
    directory's ledger entries are not segments and are left out."""
    keys = await objects_of(storage, f'{SHIPPING_PREFIX}/{host}/{int(shard):010d}/')
    return {k for k in keys if SEGMENT_KEY_RE.search(k)}


REQUEST_LINE_RE = re.compile(rb'([A-Z]+) (\S+) HTTP/1\.[01]\r\n')


class RequestStarts:
    """Finds the HTTP/1.1 requests in the bytes going towards the store, so that a hold rule can pick one. It skips a body by
    its Content-Length, so that bytes in a body never look like a request. A chunked body has no length up front: inside one,
    only a read that starts with a request line counts, which holds because the client waits for a response before it sends
    its next request on a connection."""
    def __init__(self):
        self._head = None
        self._body_left = 0
        self._chunked = False

    def feed(self, data: bytes) -> list[tuple[str, str]]:
        """The (method, path) of each request whose head ends in these bytes."""
        found = []
        while data:
            if self._body_left:
                n = min(self._body_left, len(data))
                self._body_left -= n
                data = data[n:]
                continue
            if self._head is None:
                if self._chunked and not REQUEST_LINE_RE.match(data):
                    return found
                self._chunked = False
                self._head = b''
            self._head += data
            end = self._head.find(b'\r\n\r\n')
            if end < 0:
                return found
            head, data = self._head[:end + 2], self._head[end + 4:]
            self._head = None
            if m := REQUEST_LINE_RE.match(head):
                found.append((m[1].decode(), m[2].decode()))
            length = re.search(rb'(?im)^content-length:\s*(\d+)\r$', head)
            self._chunked = re.search(rb'(?im)^transfer-encoding:\s*chunked\r$', head) is not None
            self._body_left = int(length[1]) if length and not self._chunked else 0
        return found


class GatedStore:
    """The store behind a TCP pass-through which the test can cut or slow, for store failures from outside. Nothing in
    test/pylib can make a store fail on demand: s3_proxy.py only injects random retryable errors for a whole run. It
    carries bytes unchanged. Only the hold rule looks at them, to find where each request starts.

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
        self._rule = None
        self._after = None
        self.holding = threading.Event()
        self.requests: list[tuple[str, str]] = []
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
            if pending:
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

    def hold_when(self, rule: Callable[[str, str], bool]):
        """Holds each connection on which a request matches rule(method, path), from that request until release(), and
        carries every other connection as before. The rule sees every request in order, on the gate's thread, so it may
        keep state, such as "the first read of the history after the backup's Data.db"."""
        self.holding.clear()
        self._after = None
        self._rule = rule

    def hold_after_response_bytes(self, rule: Callable[[str, str], bool], count: int):
        """For a request that matches rule(method, path): carries its response up to count bytes (headers included), then holds
        that connection, with the rest of the response unsent, until release(). The request itself goes through at once, so
        the node is mid-read of an object, for a test that looks at what it has on disk then. A response shorter than count
        is never held, nor is one of exactly count bytes. Every other connection is carried as before. Clears any hold_when() rule."""
        assert count > 0, 'a hold after no bytes is hold_when'
        self.holding.clear()
        self._rule = None
        self._after = (rule, count)

    def release(self):
        self._rule = None
        self._after = None
        self.holding.clear()

    def recover(self):
        self.down = False
        self._budget = None
        self._held = False
        self._rule = None
        self._after = None
        self.holding.clear()

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
        starts = RequestStarts()
        response_left = None    # bytes of the matching request's response still to carry before the hold

        async def pump(src, dst, towards_store):
            nonlocal response_left
            held_here = False
            try:
                while data := await src.read(65536):
                    if towards_store:
                        for method, path in starts.feed(data):
                            self.requests.append((method, path))
                            rule = self._rule
                            if not held_here and rule is not None and rule(method, path):
                                held_here = True
                                self.holding.set()
                            # Decided at every request, so that a short response doesn't leave its budget to the next request.
                            after = self._after
                            response_left = after[1] if after is not None and after[0](method, path) else None
                    elif response_left is not None and not self.down:
                        if self._after is None:
                            response_left = None
                        elif len(data) <= response_left:
                            response_left -= len(data)
                        else:
                            head, data = data[:response_left], data[response_left:]
                            response_left = None
                            if head:
                                dst.write(head)
                                await dst.drain()
                            self.holding.set()
                            while self._after is not None and not self.down:
                                await asyncio.sleep(0.01)
                            if not data:
                                continue
                    while (self._held or (held_here and self._rule is not None)) and not self.down:
                        await asyncio.sleep(0.01)
                    held_here = False
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


def is_history_read(method, path) -> bool:
    return method in ('GET', 'HEAD') and f'/{SHIPPING_PREFIX}/' in path


def is_shipping_upload(method, path) -> bool:
    return method in ('PUT', 'POST') and f'/{SHIPPING_PREFIX}/' in path


def is_entry_upload(method, path) -> bool:
    """A hold rule: the upload of a ledger entry (<sequence>.entry). A segment's own upload is not one, so a rule that holds
    this lets the segment through and stops coverage from passing it."""
    return method == 'PUT' and f'/{SHIPPING_PREFIX}/' in path and path.split('?', 1)[0].endswith('.entry')


def first_history_read_after_load():
    """A hold rule: the first read under the shipping prefix after the backup's Data.db was read. That is, the backup is
    loaded and the history isn't read yet. A shipping upload is never held, so shipping goes on."""
    loaded = False

    def rule(method, path):
        nonlocal loaded
        if method == 'GET' and f'/{BACKUP_PREFIX}/' in path and path.endswith('Data.db'):
            loaded = True
            return False
        return loaded and is_history_read(method, path)
    return rule


OLD_TS_US = 3600_000_000    # how far back seed data is written, so that it's always older than the backup


def old_ts() -> int:
    return now_us() - OLD_TS_US


@dataclass
class Pitr:
    """A node with shipping on, and table t in a vnode keyspace, with what a test does to it. `storage` is the endpoint
    backups and replays name."""
    manager: Any
    server: Any
    cql: Any
    storage: Any
    ks: str
    cf: str = 't'
    snap: str | None = None
    prefix: str | None = None
    backup_keys: set = field(default_factory=set)
    _probe: str | None = None
    _pulse_pks: list = field(default_factory=list)
    _pulse_checked: bool = False

    async def write(self, statements, table=None):
        await apply(self.cql, self.ks, table or self.cf, statements)

    async def backup(self, storage=None) -> str:
        """Snapshot and backup of t. Records what was under the shipping prefix when it finished."""
        storage = storage or self.storage
        self.snap, _ = await take_snapshot_on_one_server(self.ks, self.server, self.manager, logger)
        self.prefix = backup_prefix(self.ks, self.cf, self.snap)
        await do_backup(self.server, self.snap, self.prefix, self.ks, self.cf, storage, self.manager, logger)
        self.backup_keys = set(await objects_of(self.storage, SHIPPING_PREFIX))
        self._probe = None
        return self.snap

    async def new_target(self) -> str:
        target = unique_name('target_')
        await create_target(self.cql, self.storage, self.ks, target, self.prefix)
        return target

    async def replay(self, t, target=None, storage=None) -> tuple[Outcome, str]:
        storage = storage or self.storage
        target = target or await self.new_target()
        tid = await start_replay(self.manager, self.server, endpoint=storage.address, bucket=storage.bucket_name, prefix=self.prefix,
                                 ks=self.ks, target=target, snapshot=self.snap, t=t)
        outcome = await wait_replay(self.manager, self.server, tid)
        logger.info(f'Replay of {self.snap} into {target} to {iso(t) if isinstance(t, int) else t}: {outcome}')
        return outcome, target

    async def replay_done(self, t, target=None) -> tuple[Outcome, str]:
        outcome, target = await self.replay(t, target)
        assert outcome.state == DONE, f'replay to {iso(t)} must be done, got {outcome}'
        return outcome, target

    async def status(self) -> Status | None:
        return await get_status(self.manager, self.server)

    async def incomplete(self) -> list[tuple[str, str]] | None:
        """The `incomplete` tables, or None while status hasn't landed: a check of it is then left out, and logged."""
        status = await self.status()
        if status is None:
            logger.warning('status has not landed: the `incomplete` check is left out')
            return None
        return status.incomplete

    async def assert_nothing_loaded(self, target):
        """P52: replay loads the backup itself, so a refusal after the load leaves rows. Nothing loaded means the target
        reads empty, and status doesn't list it `incomplete`."""
        assert await read_all(self.cql, self.ks, target) == [], f'a refusal must leave {target} empty'
        incomplete = await self.incomplete()
        assert incomplete is None or (self.ks, target) not in incomplete, f'a refusal before load must not list {target} incomplete'

    async def assert_incomplete(self, target, required=False):
        incomplete = await self.incomplete()
        assert incomplete is not None or not required, 'status must exist to show `incomplete`'
        assert incomplete is None or (self.ks, target) in incomplete, f'{target} must be listed incomplete: {incomplete}'

    async def refused(self, t, reasons, watched=(), target=None, loaded_ok=False, storage=None) -> tuple[Outcome, str]:
        """Checks the refusal rules: failed, with one of the reasons and a message, the source and `watched` unchanged, and
        nothing loaded unless the refusal may come after the load (`loaded_ok`), in which case the target is `incomplete`."""
        reasons = {reasons} if isinstance(reasons, str) else set(reasons)
        tables = [self.cf, *watched]
        before = {w: await read_all(self.cql, self.ks, w) for w in tables}
        outcome, target = await self.replay(t, target, storage)
        assert outcome.state == FAILED, f'replay to {t if isinstance(t, str) else iso(t)} must be refused: {outcome}'
        assert outcome.reason in reasons, f'expected a reason in {reasons}: {outcome}'
        assert outcome.message.strip() not in ('', outcome.reason), f'a refusal states the reason and next action: {outcome}'
        for w, rows in before.items():
            assert await read_all(self.cql, self.ks, w) == rows, f'{w} changed in a refusal'
        if loaded_ok and await read_all(self.cql, self.ks, target):
            await self.assert_incomplete(target)
        else:
            await self.assert_nothing_loaded(target)
        return outcome, target

    async def pulse(self):
        """One write on every shard, to a table of its own, so that every shard's coverage moves. P62: an idle shard stops it."""
        if not self._pulse_pks:
            await self.cql.run_async(f'CREATE TABLE IF NOT EXISTS {self.ks}.pulse (pk int PRIMARY KEY, v bigint)')
            self._pulse_pks = keys_on_every_shard(1, 10_000)
        for pk in self._pulse_pks:
            await self.cql.run_async(f'INSERT INTO {self.ks}.pulse (pk, v) VALUES ({pk}, {now_us()})')
        if not self._pulse_checked:
            # Ask the node where the keys went: an idle shard holds coverage at 0, and the test would only time out.
            rows = await self.cql.run_async(f'SELECT pk, token(pk) FROM {self.ks}.pulse')
            reached = {shard_of(r[1]) for r in rows if r[0] in self._pulse_pks}
            assert reached == set(range(SHARDS)), f'the pulse must reach every shard, it reaches {sorted(reached)}'
            self._pulse_checked = True

    async def covered_through(self) -> int | None:
        """The node's covered-through time, from status. Until status lands, from the probe: a replay to a far-future T on one
        throwaway target, whose refusal states the latest restorable time and leaves the target empty to be used again."""
        status = await self.status()
        if status is not None:
            return status.covered_through_us
        if self._probe is None:
            self._probe = await self.new_target()
        t = now_us() + FAR_FUTURE_US
        outcome, _ = await self.replay(t, self._probe)
        assert outcome.reason != Reason.TARGET_ALREADY_REPLAYED, f'a refusal before load must leave the probe usable: {outcome}'
        if outcome.state != FAILED or outcome.reason != Reason.T_PAST_COVERAGE:
            logger.info(f'The coverage probe got {outcome}')
            return None
        return latest_restorable(outcome, t)

    async def covered(self, us=None, pulse=True) -> int:
        """Waits until the covered-through time reaches a time, by default a little after now, so that every write acknowledged
        before this call is covered. With pulse, it writes on every shard while it waits. Returns the covered-through time."""
        target = us if us is not None else now_us() + CLOCK_MARGIN_US

        async def reached():
            if pulse:
                await self.pulse()
            latest = await self.covered_through()
            return latest if latest is not None and latest >= target else None
        return await wait_for(reached, deadline(), label=f'covered-through {iso(target)}')

    async def snapshot_time(self) -> int:
        """The backup's snapshot time, from the usable backups in status."""
        status = await self.status()
        assert status is not None, 'the snapshot time is read from status'
        found = [ts for name, ts in status.backups if name == self.snap]
        assert len(found) == 1, f'status must list backup {self.snap} once: {status}'
        return found[0]

    async def restart(self, kill=False, while_down=None):
        self.cql = await restart(self.manager, self.server, kill, while_down)
        self._probe = None


@asynccontextmanager
async def pitr_node(manager, storage, schema=TABLE_SCHEMA, ship_to=None, also=(), **config):
    server = await manager.server_add(config=node_config(storage, ship_to=ship_to, also=also, **config))
    cql = manager.get_cql()
    async with new_test_keyspace(manager, VNODES_KS) as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t {schema}')
        yield Pitr(manager, server, cql, storage, ks)
