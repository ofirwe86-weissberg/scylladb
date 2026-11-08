#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""The measured RPO/RTO run (issue #21): what a user actually gets, not what the README derives.

This file measures; it does not test behaviour. It is skipped unless PITR_MEASURE=1, so it never
runs in CI, and it asserts only what would make a number meaningless (a load that wrote nothing, a
lag far above the seal interval, a restore missing a write it had acknowledged below T). Every size
is an environment variable with a tiny default, so the same three runs serve the smoke and the
timed stages.

Each run prints one line beginning with PITR_MEASURE_JSON and writes the same JSON to a file whose
path it prints. Sleeps are what a measurement is made of here (a lag is sampled over time, a load
is paced by the clock), so the no-sleep rule of the functional tests does not apply."""

import asyncio
import json
import logging
import os
import random
import re
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from test.cluster.object_store.pitr_util import CLOCK_MARGIN_US, SHARDS, assert_every_shard, iso, now_us, objects_of, restart, shipped_bytes
from test.cluster.object_store.test_pitr_replay_endpoint import (LOG_LEVELS, RF_1, backup_with_a_pitr_bound, coverage_reached, make_pulse,
                                                                 one_key_per_shard, one_server, start_replay, succeeded, wait_for_coverage)
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.skipif(os.environ.get('PITR_MEASURE') != '1', reason='a measurement run, not a test: set PITR_MEASURE=1')


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def env_ints(name: str, default: str) -> list[int]:
    """A list, so one invocation can measure the same thing at several sizes (the seal interval)."""
    return [int(x) for x in os.environ.get(name, default).split(',')]


BACKUP_MB = env_int('PITR_MEASURE_BACKUP_MB', 5)       # the backup a restore starts from; stage 1 uses ~200
LOAD_S = env_int('PITR_MEASURE_LOAD_S', 30)            # how long the write load runs; stage 1 uses 300
SEAL_S = env_ints('PITR_MEASURE_SEAL_S', '60')         # the seal interval(s); the lag run takes '60,10'
RATE_KBPS = env_int('PITR_MEASURE_RATE_KBPS', 256)     # the write load, in KB/s of row payload
REPS = env_int('PITR_MEASURE_REPS', 1)                 # restores timed per RTO run; stage 1 uses 3
MEMORY = os.environ.get('PITR_MEASURE_MEMORY', '2G')   # per node; see node_memory() on why
LAG_SLACK_S = env_int('PITR_MEASURE_LAG_SLACK_S', 60)  # how far above the seal interval a lag may go before the run is called broken
KILLS = env_int('PITR_MEASURE_KILLS', 1)               # kills, restarts and restores the loss run takes in a row
SEED = env_int('PITR_MEASURE_SEED', 20261108)          # the delay before each kill is drawn from this, so a number can be reproduced

ROW_BYTES = 1024                 # one row of payload, the size the plan measures at
TICK_SECONDS = 0.1               # the load's pacing tick
LAG_SAMPLE_SECONDS = 5           # the plan's sampling interval
SEED_BATCH = 200                 # rows per concurrent batch while seeding the backup
VALUE = 'x' * ROW_BYTES
# The node log's timestamp, wherever in the line it sits. Only differences within one log are used,
# so the node's timezone does not matter.
LOG_TS_RE = re.compile(r'(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3})')


def log_time(line: str) -> float | None:
    """Seconds, from a node-log line, or None if the line carries no timestamp."""
    match = LOG_TS_RE.search(line)
    if not match:
        return None
    return time.mktime(time.strptime(match.group(1)[:19], '%Y-%m-%d %H:%M:%S')) + int(match.group(1)[20:]) / 1000


def node_memory() -> str:
    """Why this much: the outage latch (#88) releases a hold once the unshipped bytes pass
    pitr_unshipped_limit_in_mb (256 by default, four commitlog segments of 64 MB), and a node that
    cannot keep a segment's worth of writes in memory flushes and seals far more often than the
    seal interval, which would measure the VM and not the product. 2 GB over 2 shards holds a
    segment per shard with room to spare, and the limit stays at its default, well above one
    segment."""
    return MEMORY


def hardware() -> dict:
    """What the numbers are of. Recorded with every run, because none of them transfer to another
    machine."""
    def command(argv):
        try:
            return subprocess.run(argv, capture_output=True, text=True, timeout=30).stdout.strip()
        except Exception as e:                   # a measurement must not fail on a missing tool
            return f'unavailable: {e}'

    cpu = {}
    for line in command(['lscpu']).splitlines():
        key, _, value = line.partition(':')
        if key.strip() in ('Model name', 'Architecture', 'CPU(s)', 'Thread(s) per core', 'CPU max MHz'):
            cpu[key.strip()] = value.strip()
    mem_total = ''
    try:
        with open('/proc/meminfo') as f:
            mem_total = next(line for line in f if line.startswith('MemTotal'))
    except Exception as e:
        mem_total = f'unavailable: {e}'
    usage = shutil.disk_usage(os.getcwd())
    return {
        'cpu': cpu,
        'ram': mem_total.strip(),
        'disk_total_gb': round(usage.total / 2**30, 1),
        'disk_free_gb': round(usage.free / 2**30, 1),
        'scylla_smp': SHARDS,
        'scylla_memory': node_memory(),
        'store': 'local mock (no WAN latency or bandwidth limit, so upload and fetch are optimistic)',
        'git_head': os.environ.get('PITR_MEASURE_GIT_HEAD') or command(['git', 'rev-parse', 'HEAD']),
    }


async def measure_server(manager, object_storage, seal_s):
    """The node every run measures: shipping on, the seal interval under test, the shipper at debug
    so the upload of each segment can be timed, and the memory node_memory() explains."""
    return await one_server(manager, object_storage, shipping=True, seal_seconds=seal_s,
                            loggers=f'{LOG_LEVELS}:pitr=debug:pitr_chain_check=debug', cmdline_extra=['-m', node_memory()])


def build_mode(exe: str) -> str:
    """Which build the numbers are of, read off the binary under test: dev is unoptimized, so its
    RTO is pessimistic, and only the release column is the one a user would see."""
    return next((mode for mode in ('debug', 'dev', 'release', 'sanitize') if f'/{mode}/' in exe), 'unknown')


def output_dir(tmp_path) -> Path:
    """Where the summaries go. Under the worktree's build/ by default, because test.py keeps the
    stdout of a passing test nowhere and empties testlog/ when it starts; PITR_MEASURE_DIR moves
    it, and the test's own tmpdir is the fallback."""
    path = Path(os.environ.get('PITR_MEASURE_DIR') or 'build/pitr-logs/measure')
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.warning('cannot write the summary to %s (%s), using %s', path, e, tmp_path)
        return tmp_path
    return path


def report(name: str, tmp_path, body: dict) -> dict:
    """One JSON summary per run: on stdout behind PITR_MEASURE_JSON, and in a file of its own."""
    summary = {'run': name, 'build_mode': build_mode(str(body.get('scylla_exe', ''))),
               'params': {'backup_mb': BACKUP_MB, 'load_s': LOAD_S, 'seal_s': SEAL_S, 'rate_kbps': RATE_KBPS,
                          'reps': REPS, 'row_bytes': ROW_BYTES},
               'hardware': hardware()} | body
    line = json.dumps(summary, sort_keys=True)
    path = output_dir(tmp_path) / f'pitr_measure_{name}_{time.strftime("%m%d-%H%M%S")}.json'
    path.write_text(line + '\n')
    print(f'PITR_MEASURE_JSON {line}')
    print(f'PITR_MEASURE_FILE {path}')
    logger.info('PITR_MEASURE_JSON %s', line)
    return summary


def percentile(values: list[float], p: float) -> float | None:
    """Nearest-rank, on a handful of samples: no interpolation to argue about."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


class steady_load:
    """A paced write load of 1 KB rows into one table, recording what was acknowledged and when.

    A tick that runs late is not made up for, so the load never piles up on a node that has slowed
    down; the rate it achieved is reported next to the rate it was asked for. The write timestamp is
    set by the client, which is what lets a restore's rows be matched against the writes that were
    acknowledged before T."""

    def __init__(self, cql, ks, table, rate_kbps=RATE_KBPS, first_pk=0):
        self._cql, self._ks, self._table, self._rate_kbps = cql, ks, table, rate_kbps
        self._next_pk = first_pk
        self.attempted: dict[str, int] = {}             # pk -> write timestamp (us)
        self.acked: dict[str, tuple[int, int]] = {}     # pk -> (write timestamp, time of the acknowledgement)
        self.failure: Exception | None = None
        self.started_us = self.stopped_us = 0
        self._stopping = False
        self._task = None

    def start(self):
        self.started_us = now_us()
        self._task = asyncio.create_task(self._run())
        return self

    async def _run(self):
        rows_per_tick = max(1, round(self._rate_kbps * 1024 * TICK_SECONDS / ROW_BYTES))
        begin, tick = time.monotonic(), 0
        while not self._stopping:
            pks = [f'r{pk}' for pk in range(self._next_pk, self._next_pk + rows_per_tick)]
            self._next_pk += rows_per_tick
            try:
                await asyncio.gather(*(self._write(pk) for pk in pks))
            except Exception as e:
                # A kill ends the load this way, which is the point of the loss run; every other
                # run checks this afterwards.
                self.failure = e
                return
            tick += 1
            delay = begin + tick * TICK_SECONDS - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)

    async def _write(self, pk):
        ts = now_us()
        self.attempted[pk] = ts
        await self._cql.run_async(f"INSERT INTO {self._ks}.{self._table} (pk, value) VALUES ('{pk}', '{VALUE}') USING TIMESTAMP {ts}")
        self.acked[pk] = (ts, now_us())

    async def stop(self, expect_failure=False, timeout=120):
        self._stopping = True
        try:
            await asyncio.wait_for(asyncio.shield(self._task), timeout)
        except asyncio.TimeoutError:
            # A write left pending on a dead connection would otherwise outlive the test and write
            # into a keyspace the next run has dropped.
            logger.warning('the load did not stop within %ss, cancelling its last writes', timeout)
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception) as e:
                logger.info('the cancelled load ended with %r', e)
        self.stopped_us = now_us()
        if not expect_failure:
            assert self.failure is None, f'the load failed: {self.failure}'
        assert self.acked, 'the load acknowledged no write at all, so there is nothing to measure'
        return self

    def stats(self) -> dict:
        seconds = (self.stopped_us - self.started_us) / 1e6
        return {'acked_rows': len(self.acked), 'attempted_rows': len(self.attempted), 'seconds': round(seconds, 1),
                'asked_kbps': self._rate_kbps,
                'achieved_kbps': round(len(self.acked) * ROW_BYTES / 1024 / seconds, 1) if seconds else None,
                'failed': repr(self.failure) if self.failure else None}


async def seed_backup_sized(cql, ks, table, megabytes):
    """The data a restore starts from, written in bounded batches so one gather never holds the
    whole backup: 200 MB of 1 KB rows is 200k statements."""
    rows = max(1, megabytes * 1024 * 1024 // ROW_BYTES)
    for base in range(0, rows, SEED_BATCH):
        batch = range(base, min(base + SEED_BATCH, rows))
        await asyncio.gather(*(cql.run_async(f"INSERT INTO {ks}.{table} (pk, value) VALUES ('seed{pk}', '{VALUE}')") for pk in batch))
    logger.info('seeded %s rows (about %s MB) into %s.%s', rows, megabytes, ks, table)
    return rows


async def fresh_target(cql, ks, name):
    """A target per restore: a replay refuses a target it has already marked, so each repetition
    needs its own. Compaction off in the schema, the way the README tells a user to."""
    await cql.run_async(f"CREATE TABLE {ks}.{name} (pk text primary key, value text) "
                        "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};")
    return name


class pulser:
    """A write on every shard, through whichever connection is current.

    make_pulse() holds the connection it was made with, and the loss run kills the node and
    reconnects, so its pulse has to be re-bound after every restart. Coverage is the lowest of the
    shards', so one idle shard would hold the whole node at 1970 (P62)."""

    def __init__(self, cql, ks, keys):
        self.cql, self._ks, self._keys = cql, ks, keys

    async def __call__(self):
        await asyncio.gather(*(self.cql.run_async(f'INSERT INTO {self._ks}.pulse (pk) VALUES ({pk});') for pk in self._keys))


async def make_pulser(cql, ks) -> pulser:
    await cql.run_async(f'CREATE TABLE {ks}.pulse (pk int primary key);')
    keys = sorted(await one_key_per_shard(cql, f'{ks}.pulse', [str(pk) for pk in range(32)]))
    logger.info('the pulse writes %s, one key per shard', keys)
    return pulser(cql, ks, keys)


async def shipping_timeline(log, from_mark) -> dict:
    """What the shipper's own log says about the uploads in a window.

    The seal instant is not logged, so seal -> entry cannot be split: what is here is the upload
    rate over the window the stored segments span, and the wait from a segment being stored to its
    ledger entry, which is what releases its hold and moves coverage."""
    stored = [(log_time(line), int(m.group(1)), int(m.group(2)))
              for line, m in await log.grep(r'Stored segment (\d+) \((\d+) bytes\)', from_mark=from_mark)]
    recorded = {int(m.group(1)): log_time(line)
                for line, m in await log.grep(r'Recorded segment (\d+) as entry', from_mark=from_mark)}
    waits = [round(recorded[sid] - at, 3) for at, sid, _ in stored if sid in recorded and at is not None]
    total_bytes = sum(size for _, _, size in stored)
    span = (stored[-1][0] - stored[0][0]) if len(stored) > 1 else None
    return {'segments_stored': len(stored), 'segments_recorded': len(recorded), 'stored_bytes': total_bytes,
            'stored_span_seconds': round(span, 3) if span else None,
            'upload_kbps_over_span': round(total_bytes / 1024 / span, 1) if span else None,
            'stored_to_entry_seconds': {'p50': percentile(waits, 0.5), 'max': max(waits) if waits else None},
            'note': 'the seal instant is not logged, so seal -> entry is not split; a span of two or three segments '
                    'stored back to back is a queue drain, not a rate, so read it only over many segments; see issue #21'}


@pytest.mark.parametrize('seal_s', SEAL_S)
async def test_measure_lag(manager: ScyllaClusterManager, s3_storage, tmp_path, seal_s):
    """How far the shipped history trails the present under a steady load, at one seal interval:
    the lag an RPO is made of. One node, one table, 1 KB rows on every shard. One run per seal
    interval, each on a cluster of its own, so PITR_MEASURE_SEAL_S=60,10 measures both."""
    server = await measure_server(manager, s3_storage, seal_s)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        snap, prefix = await backup_with_a_pitr_bound(manager, server, s3_storage, ks, {'0': 'zero'})
        pulse = await make_pulse(cql, ks)
        # Nothing can be sampled before the history covers anything at all: coverage starts at the
        # epoch and the first entry takes up to one seal interval.
        await wait_for_coverage(manager, server, s3_storage, ks, 'target', snap, prefix, pulse, now_us())
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        load = steady_load(cql, ks, 'source').start()
        lags, end = [], time.monotonic() + LOAD_S
        while time.monotonic() < end:
            await asyncio.sleep(LAG_SAMPLE_SECONDS)
            # The pulse guards the P62 limit: one idle shard holds the node's coverage down, and a
            # lag measured against it would be the idle shard's, not the load's.
            await pulse()
            covered = await coverage_reached(manager, server, s3_storage, ks, 'target', snap, prefix)
            lags.append(round((now_us() - covered) / 1e6, 3))
            logger.info('seal=%ss lag=%ss covered through %s', seal_s, lags[-1], iso(covered))
        await load.stop()
        await assert_every_shard(cql, ks, 'source', set(load.acked))
        result = report(f'lag_s{seal_s}', tmp_path, {
            'seal_s': seal_s, 'scylla_exe': await manager.server_get_exe(server.server_id),
            'lag_seconds': {'p50': percentile(lags, 0.5), 'p95': percentile(lags, 0.95), 'max': max(lags), 'count': len(lags)},
            'load': load.stats(), 'shipping': await shipping_timeline(log, mark),
            'shipped_bytes_total': await shipped_bytes(s3_storage),
            'shipped_objects': len(await objects_of(s3_storage, 'pitr-log'))})

    assert result['lag_seconds']['count'] >= 2, f'{result}: too few samples to say anything'
    # Sanity, not a bar: a lag far above the seal interval means the load outran the uploads or the
    # node stalled, and the numbers of such a run describe neither.
    assert result['lag_seconds']['max'] <= seal_s + LAG_SLACK_S, \
        f"the lag at seal={seal_s}s reached {result['lag_seconds']['max']}s, above the seal interval plus {LAG_SLACK_S}s: {result}"


async def measure_one_kill(manager, server, s3_storage, seal_s, index, rng, cql):
    """One kill -9 and the restore after it: the load runs, the history catches up with part of it,
    the kill lands at a random point of the seal cycle, and what the restore gives back is counted.
    Returns what was measured and the connection the restart left.

    Each kill takes a backup of its own, in a keyspace of its own: a restart starts a new chain,
    and coverage for a backup taken before the restart stops at the chain that was live then, so a
    second kill measured against the first kill's backup would wait for a coverage advance that
    never comes (seen at KILLS=3; see the report on #21).

    The delay before the kill is what makes the figure an RPO rather than its minimum. The seal
    timer is periodic (pitr_shipper.cc, arm_periodic), and the load starts right after a coverage
    advance, so a kill taken without a delay always lands just after an entry and reports about the
    upload time alone; the real range is up to one seal interval above that. The delay is drawn
    from the seed the run records, so a surprising number can be reproduced."""
    async with new_test_keyspace(manager, RF_1) as ks:
        return await _one_kill_in(manager, server, s3_storage, ks, seal_s, index, rng, cql)


async def _one_kill_in(manager, server, s3_storage, ks, seal_s, index, rng, cql):
    """The body of one kill, inside the keyspace it measures in. ks.target is only ever the probe
    the coverage reading is refused on, and the restore goes into a table of its own; both turn
    compaction off through their schema, because the API's disable_autocompaction does not survive
    the restart."""
    snap, prefix = await backup_with_a_pitr_bound(manager, server, s3_storage, ks, {'0': 'zero'}, schema_flag_only=True)
    pulse = await make_pulser(cql, ks)
    await wait_for_coverage(manager, server, s3_storage, ks, 'target', snap, prefix, pulse, now_us())
    load = steady_load(cql, ks, 'source').start()
    end = time.monotonic() + LOAD_S
    while time.monotonic() < end:
        await asyncio.sleep(LAG_SAMPLE_SECONDS)
        await pulse()
    # The history has to cover part of the load, or every acknowledged write would sit above L and
    # the restore would be measured against nothing.
    first_ack = min(ts for ts, _ in load.acked.values())
    last_covered = await wait_for_coverage(manager, server, s3_storage, ks, 'target', snap, prefix, pulse, first_ack + CLOCK_MARGIN_US)
    advanced_at = now_us()

    delay = rng.uniform(0, seal_s)
    logger.info('kill %s waits %.1fs into the seal cycle before the kill', index, delay)
    deadline = time.monotonic() + delay
    while True:
        left = deadline - time.monotonic()
        if left <= 0:
            break
        await asyncio.sleep(min(1.0, left))
        await pulse()
        covered = await coverage_reached(manager, server, s3_storage, ks, 'target', snap, prefix)
        if covered > last_covered:
            last_covered, advanced_at = covered, now_us()
    kill_us = now_us()
    cql = await restart(manager, server, kill=True)
    pulse.cql = cql
    # The load dies with the node, which is the event being measured: its failure is expected.
    await load.stop(expect_failure=True)

    covered = await coverage_reached(manager, server, s3_storage, ks, 'target', snap, prefix)
    assert covered <= kill_us, f'the history covers {iso(covered)}, past the kill at {iso(kill_us)}'
    target = await fresh_target(cql, ks, f'loss_{index}')
    tid = await start_replay(manager, server, s3_storage, ks, target, snap, prefix, iso(covered))
    succeeded(await manager.api.wait_task(server.ip_addr, tid))

    rows = await cql.run_async(f'SELECT pk, WRITETIME(value) AS wt FROM {ks}.{target}')
    present = {r.pk: r.wt for r in rows}
    # Mirrors assert_acked_writes_present(): a write acknowledged within the clock margin of L may
    # be either present or absent, so only the ones clearly below L are required.
    required = {pk: ts for pk, (ts, ack) in load.acked.items() if ts <= covered and ack + CLOCK_MARGIN_US < covered}
    missing = sorted(pk for pk, ts in required.items() if present.get(pk) != ts)
    # What the user loses: acknowledged before the kill, written above L, so the T filter leaves it
    # out of every restore. Bounded by the kill, so a write the driver got through afterwards is
    # not counted as lost.
    after_l = sorted(pk for pk, (ts, ack) in load.acked.items() if ts > covered and ack <= kill_us)
    above_l = sorted(pk for pk, wt in present.items() if wt > covered)
    return {'kill': index, 'delay_before_kill_seconds': round(delay, 3),
            'kill_phase_seconds': round((kill_us - advanced_at) / 1e6, 3),
            'kill_at': iso(kill_us), 'restorable_at': iso(covered),
            'effective_rpo_seconds': round((kill_us - covered) / 1e6, 3),
            'acked_rows': len(load.acked), 'restored_rows': len(present),
            'required_more_than_margin_below_l': len(required),
            'missing_more_than_margin_below_l': len(missing), 'missing_sample': missing[:10],
            'acked_after_l': len(after_l), 'rows_above_l_in_the_restore': len(above_l),
            'above_l_sample': above_l[:10], 'load': load.stats()}, cql


async def test_measure_loss(manager: ScyllaClusterManager, s3_storage, tmp_path):
    """What a kill -9 costs: the writes acknowledged before the kill but above the latest
    restorable point L, and the promise that every write acknowledged more than the clock margin
    below L comes back. Effective RPO = kill time - L.

    PITR_MEASURE_KILLS kills in a row, each with its own delay into the seal cycle, its own backup
    and its own restore target, because one draw from a cycle says little about the range."""
    seal_s = SEAL_S[0]
    rng = random.Random(SEED)
    server = await measure_server(manager, s3_storage, seal_s)
    cql = manager.get_cql()
    kills = []
    for index in range(KILLS):
        one, cql = await measure_one_kill(manager, server, s3_storage, seal_s, index, rng, cql)
        kills.append(one)
        logger.info('kill %s measured %s', index, one)
    rpos = [k['effective_rpo_seconds'] for k in kills]
    result = report('loss', tmp_path, {
        'seal_s': seal_s, 'scylla_exe': await manager.server_get_exe(server.server_id), 'seed': SEED,
        'effective_rpo_seconds': {'min': min(rpos), 'p50': percentile(rpos, 0.5), 'max': max(rpos), 'count': len(rpos)},
        'kills': kills,
        'note': 'one node, killed and restarted once per kill, with a backup of its own per kill. The delay before '
                'each kill is uniform over [0, S) from the recorded seed, so the RPO samples the seal cycle instead '
                'of its minimum; the kill phase is the time since the last coverage advance a probe saw, polled about '
                'once a second, so it is accurate to about that'})

    for k in result['kills']:
        assert k['required_more_than_margin_below_l'], \
            f'no write was acknowledged more than the clock margin before L, so kill {k["kill"]} measured nothing: {k}'
        # The one real assert of this file: a restore that lost a write it had acknowledged well
        # below L is wrong data, not a slow number.
        assert not k['missing_more_than_margin_below_l'], \
            f'{k["missing_more_than_margin_below_l"]} writes acknowledged more than the clock margin before L are missing: ' \
            f'{k["missing_sample"]} (L={k["restorable_at"]})'
        # Free, and the other half of the promise: T filters out everything above L.
        assert not k['rows_above_l_in_the_restore'], \
            f'{k["rows_above_l_in_the_restore"]} restored rows were written above L: {k["above_l_sample"]} (L={k["restorable_at"]})'


async def test_measure_rto(manager: ScyllaClusterManager, s3_storage, tmp_path):
    """How long a restore takes: a backup of B MB plus a load of LOAD_S seconds of history,
    replayed into a fresh table, timed from the call to `done` and split by the phases the node log
    marks. One repetition per target, REPS of them."""
    seal_s = SEAL_S[0]
    server = await measure_server(manager, s3_storage, seal_s)
    cql = manager.get_cql()
    async with new_test_keyspace(manager, RF_1) as ks:
        backup_rows = 0

        async def seed():
            nonlocal backup_rows
            backup_rows = await seed_backup_sized(cql, ks, 'source', BACKUP_MB)

        snap, prefix = await backup_with_a_pitr_bound(manager, server, s3_storage, ks, {}, seed=seed)
        pulse = await make_pulse(cql, ks)
        await wait_for_coverage(manager, server, s3_storage, ks, 'target', snap, prefix, pulse, now_us())
        log = await manager.server_open_log(server.server_id)
        shipping_mark = await log.mark()
        load = steady_load(cql, ks, 'source').start()
        end = time.monotonic() + LOAD_S
        while time.monotonic() < end:
            await asyncio.sleep(LAG_SAMPLE_SECONDS)
            await pulse()
        await load.stop()
        t_us = now_us()
        # Every write of the load has to be below T and shipped, or the restore would not be
        # replaying the history that was just written.
        await wait_for_coverage(manager, server, s3_storage, ks, 'target', snap, prefix, pulse, t_us)

        reps = []
        for rep in range(REPS):
            target = await fresh_target(cql, ks, f'rto_{rep}')
            log = await manager.server_open_log(server.server_id)
            mark = await log.mark()
            began = time.monotonic()
            tid = await start_replay(manager, server, s3_storage, ks, target, snap, prefix, iso(t_us))
            succeeded(await manager.api.wait_task(server.ip_addr, tid))
            total = time.monotonic() - began
            reps.append({'total_seconds': round(total, 3)} | await _phases(log, mark, ks, target, total))
            logger.info('rep %s of the restore took %ss: %s', rep, round(total, 1), reps[-1])

        restored = (await cql.run_async(f'SELECT count(*) AS n FROM {ks}.{target}'))[0].n
        result = report('rto', tmp_path, {'seal_s': seal_s, 'scylla_exe': await manager.server_get_exe(server.server_id), 'reps': reps, 'load': load.stats(),
                                          'rows_in_the_last_target': restored, 'backup_rows': backup_rows,
                                          'shipping': await shipping_timeline(log, shipping_mark),
                                          'shipped_bytes_total': await shipped_bytes(s3_storage)})
        assert all(r['total_seconds'] > 0 for r in result['reps']), result
        # A restore of nothing would give a fine RTO and mean nothing. The seed keys and the load's
        # keys are disjoint and nothing is deleted, so the count is exact rather than a lower bound.
        assert restored == backup_rows + len(load.acked), \
            f'the restore holds {restored} rows, not the {backup_rows} of the backup plus the {len(load.acked)} the load acknowledged'


# A shipped segment's object key is <prefix>/<host id>/<shard>/<chain>/segments/<file name>
# (ledger_entry.cc, segment_key() over chain_key_prefix()), so the fetch lines say which shard's
# history the bytes came from without the product having to log it.
FETCHED_RE = re.compile(r'fetched (\S+) to \S+, (\d+) bytes')
SEGMENT_KEY_RE = re.compile(r'/(\d+)/([^/]+)/segments/(\S+)$')


async def _phases(log, mark, ks, target, total) -> dict:
    """The restore split by phase, from the node log's own marks.

    The phases the log actually marks are: the checks (the call until the run reports it started),
    the load of the backup's sstables, and the replay of the history. The fetch of each segment is
    interleaved with the replay of the one before it, so it is reported per segment and as a byte
    total, not as a phase of its own; the flush and the durable mark sit between the last segment
    and the result line. The chain lines and the result line are copied in as they are: they are
    what says how much of the fetched history predates the backup's bound."""
    async def at(expr, last=False):
        hits = await log.grep(expr, from_mark=mark)
        if not hits:
            return None
        return log_time(hits[-1 if last else 0][0])

    started = await at(r'replay of backup .* started:')
    loaded = await at(r"backup '.*' loaded")
    replayed = await at(r'replayed \d+ segment\(s\) of shard', last=True)
    done = await at(rf'Point-in-time replay of {ks}\.{target} to t=.* is done:')

    fetched = []
    for line, _ in await log.grep(r'fetched \S+ to \S+, \d+ bytes', from_mark=mark):
        hit = FETCHED_RE.search(line)
        key = SEGMENT_KEY_RE.search(hit.group(1))
        fetched.append({'shard': int(key.group(1)) if key else None,
                        'chain': key.group(2) if key else None,
                        'segment': key.group(3) if key else hit.group(1),
                        'bytes': int(hit.group(2))})
    chains = [line[line.index('chain '):].strip() for line, _ in await log.grep(r'chain .* holds \d+ entries and \d+ segments', from_mark=mark)]
    result_lines = [line[line.index('Point-in-time replay'):].strip()
                    for line, _ in await log.grep(rf'Point-in-time replay of {ks}\.{target} to t=.* is done:', from_mark=mark)]
    below_bound = re.search(r'(\d+) entry/entries below the backup\'s bound', result_lines[0]) if result_lines else None

    def gap(a, b):
        return round(b - a, 3) if a is not None and b is not None else None

    start_to_done = gap(started, done)
    return {'phase_seconds': {# The REST call, the checks and the task-manager wait all sit outside
                              # the node's own start-to-done, so what is left of the client total
                              # is the three together.
                              'checks_and_rest_roundtrip': None if start_to_done is None else round(total - start_to_done, 3),
                              'load': gap(started, loaded), 'replay_with_fetch': gap(loaded, replayed),
                              'flush_and_mark': gap(replayed, done), 'start_to_done': start_to_done},
            'segments_fetched': len(fetched), 'fetched_bytes': sum(f['bytes'] for f in fetched),
            'fetched_segments': fetched, 'chains': chains,
            'entries_below_the_backup_bound': int(below_bound.group(1)) if below_bound else None,
            'result_line': result_lines[0] if result_lines else None,
            'missing_marks': [name for name, value in (('started', started), ('loaded', loaded),
                                                       ('replayed', replayed), ('done', done)) if value is None]}
