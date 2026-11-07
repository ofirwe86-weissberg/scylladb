# Single-node point-in-time recovery (experimental)

## What it is

Point-in-time recovery (PITR) restores one table to how it was at a chosen
time T, into a new table, without touching the original. It is meant for
undoing logical damage: a bad delete, a bad application release, a wrong
update. It works by shipping the node's sealed commitlog segments to object
storage (S3 or GCS) as they close, next to the sstable backups that
`nodetool backup` already takes. A restore loads a backup into an empty table
and then replays the shipped commitlog on top of it, up to T. The feature is
experimental and limited to one node, replication factor 1, and vnode
keyspaces. It does not protect against node or disk loss.

**Status: not usable yet.** Shipping and the bound file work, but the replay
of the shipped history (#18, fetch and replay, not built yet) is missing, so
no restore can reach `done` today. See [Implementation status](#implementation-status-2026-11-07).

## How it works

```
  writes ──► commitlog (per shard)
                │  seal every pitr_seal_interval_in_seconds
                ▼
        sealed segment ──upload──► <prefix>/<host>/<shard>/<chain>/segments/<file>
                │                  then ledger entry <sequence>.entry, in id order
                ▼
        hold released after the entry is stored

  nodetool snapshot + backup ──► backup prefix: sstables, manifest.json,
                                 schema.cql, pitr-bound.txt

  restore: CREATE target ──► POST replay_to ──► check ──► mark ──► load backup
           ──► fetch + replay segments to T ──► flush ──► durable `done`
```

### Ship path

When shipping is on, each shard seals its active commitlog segment every
`pitr_seal_interval_in_seconds`, without asking for a memtable flush. A
sealed segment is held on local disk: the commitlog may not delete or
recycle it until it is shipped. The shard uploads the segment's used bytes
to the store, and then writes an immutable ledger entry for it.

Objects are laid out per shard and per chain:

```
<prefix>/<host_id>/<shard>/<chain>/<sequence>.entry
<prefix>/<host_id>/<shard>/<chain>/segments/<file_name>
```

where `<chain>` is `<boot_epoch_us>-<chain_start_us>-<chain_uuid>`. A chain
starts when shipping starts, at each node start. Segments already on disk at
that moment are below the chain's floor and are released, not shipped.

Ledger entries are written one at a time, in segment-id order, with dense
sequence numbers starting at 0. Each entry names its segment, the previous
entry's segment, the used size, a checksum of the uploaded bytes, and the
time through which every acknowledged write is covered ("covered-through").
A missing sequence number ends the chain. A restore reads the entries by
their predictable keys, not by listing the bucket. The checksum is
`utils::crc32` (Castagnoli, initial value 0, no final XOR), so it doesn't
match the CRC32C a store reports for the object.

Covered-through is captured at the start of each seal cycle, minus a fixed
1 s margin, and is carried only by an entry whose segment holds every write
acknowledged before the capture. A segment's hold is released only after its
ledger entry is in the store. If a segment fails to close, an upload fails in
a way no retry fixes, or an entry can't be written, the chain ends there and
shipping stops until the next restart.

### Backup

Take backups as usual, with `nodetool snapshot` and then `nodetool backup`.
With shipping on, the snapshot also gets a `pitr-bound.txt` file, written
before `manifest.json`. It records, per shard, the commitlog position where
replay must start for this snapshot, and the chain that was shipping when the
snapshot was taken. `nodetool backup` uploads it with the rest of the
snapshot. A backup without this file can't be used for PITR.

### Restore

1. Create the target table from the backup's `schema.cql`: give it a new
   name in the same keyspace, drop the `ID` option, and disable compaction
   with `'enabled': 'false'` in its compaction option (or, after the CREATE,
   with `nodetool disableautocompaction`).
2. Call `POST /storage_service/pitr/replay_to` with the parameters
   `endpoint`, `bucket`, `prefix` (the backup's location, as given to
   `nodetool backup`), `keyspace` and `table` (the target), `snapshot` (the
   snapshot tag) and `t`.
3. The call returns a task id at once. Every refusal and failure is the
   task's outcome, not an HTTP error. Wait for it with the task manager
   (`nodetool tasks wait <id>`, or `GET /task_manager/wait_task/<id>`).
4. The task runs on shard 0:
   - It checks the call before touching anything: T parses, the endpoint is
     configured, the target exists, no replay is running into it or has run
     into it, its auto-compaction is off, it is empty, the backup's
     `manifest.json` names the given snapshot, `pitr-bound.txt` is present
     and valid, and the target's schema matches the backup's apart from
     the name, the ID and compaction. The chain checks (a dense chain from the
     backup's bound to T on every shard, and T not past covered-through) and
     the TRUNCATE and schema refusals are to run here too, before the mark;
     they come with #18's fetch and replay and with #15. A refusal leaves
     the node as it was.
   - It writes a durable `started` mark for the target, under the node's
     work directory. From then on the target is `incomplete`.
   - It holds every compaction of the target off, on every shard, for the
     whole run, and loads the backup's sstables listed in its
     `manifest.json` through the existing restore path.
   - It fetches the shipped segments, replays them strictly, keeps only
     writes with a timestamp at or below T, and applies them to the target.
   - It flushes the target and replaces the mark with a durable `done`.
   If a compaction could have run on the target during the run, the task
   fails and never reports `done`.

The last two steps are #18's fetch and replay, not built yet.

### Status

A status GET is planned. It will show the shipping state (off with its
reason, active, or broken with its time and cause), the covered-through time
of the current chain, the usable backups with their snapshot times, and the
targets that are `incomplete`. It will not show limits, per-table
protection, or restorable intervals. Its URI and format are not decided.

## Configuration

All options are in `scylla.yaml`. Turning shipping on or changing it takes a
restart.

| Option | Default | Meaning |
|---|---|---|
| `pitr_endpoint` | empty | Name of an `object_storage_endpoints` entry, which supplies the credentials |
| `pitr_bucket` | empty | Bucket shipping writes to |
| `pitr_prefix` | `pitr` | Prefix of every shipped object, so one bucket can hold more than one node |
| `pitr_seal_interval_in_seconds` | 60 | How often each shard seals its active segment. 1 to 3600 |
| `pitr_unshipped_limit_in_mb` | 256 | Unshipped data held, in MiB, before shipping gives up on protection so writes keep succeeding. Must be above 0 |

Shipping is off while `pitr_endpoint` and `pitr_bucket` are both unset. With
only one of them set, a value out of range, an unknown endpoint, more than
one node, or the commitlog disabled, shipping stays off and the node logs
why. The commitlog clamps the limit it holds segments against to 3/4 of the
lower of the shard's flush threshold and its share of
`commitlog_total_space_in_mb`, so a store outage reaches the limit before it
triggers memtable flushes or blocks writes. Nothing acts on the limit yet:
that is the outage latch, which is not built (limit 22).

The endpoint is configured as for backup and restore (`docs/dev/object_storage.md`):

```yaml
object_storage_endpoints:
  - name: https://s3.us-east-1.amazonaws.com:443
    aws_region: us-east-1
pitr_endpoint: https://s3.us-east-1.amazonaws.com:443
pitr_bucket: my-pitr-bucket
pitr_prefix: pitr/node1
pitr_seal_interval_in_seconds: 60
```

**Store permissions.** The node's credentials need write access
(`s3:PutObject`) to the shipping prefix and the backup prefix, and read
access (`s3:GetObject`) to both for a restore. A restore also needs
`s3:ListBucket` on the bucket: without it, S3 reports a missing key as
access denied, so restores are expected to fail with a store error (the
restore path that probes for missing keys isn't built yet). The full list of
actions hasn't been checked against each store. Buckets with object lock
aren't supported (limit 37).

## Quick start

```sh
# 1. Turn shipping on (scylla.yaml as above) and restart the node.

# 2. Back up the table, to the same endpoint and bucket.
nodetool snapshot --tag snap1 --table t ks
nodetool backup --endpoint https://s3.us-east-1.amazonaws.com:443 \
    --bucket my-pitr-bucket --prefix backups/ks/t/snap1 \
    --keyspace ks --table t --snapshot snap1

# 3. Damage happens. Fetch the backup's schema and create the target.
aws s3 cp s3://my-pitr-bucket/backups/ks/t/snap1/schema.cql .
#    Edit it: rename ks.t to ks.t_restored, remove the ID option, and set
#    compaction = {..., 'enabled': 'false'}. Then:
cqlsh -f schema.cql

# 4. Replay to T.
curl -X POST "http://localhost:10000/storage_service/pitr/replay_to\
?endpoint=https://s3.us-east-1.amazonaws.com:443&bucket=my-pitr-bucket\
&prefix=backups/ks/t/snap1&keyspace=ks&table=t_restored&snapshot=snap1\
&t=2026-11-04T14:05:00.000000Z"
# -> "<task id>"

# 5. Wait for the result, and keep it.
nodetool tasks wait <task id>

# 6. Check ks.t_restored, then turn compaction on for it: restate the
#    compaction option from schema.cql without 'enabled': 'false'.
cqlsh -e "ALTER TABLE ks.t_restored WITH compaction = {'class': '<class from schema.cql>'}"
```

If you disabled compaction with `nodetool disableautocompaction ks t_restored`
instead, turn it back on with `nodetool enableautocompaction ks t_restored`.
That flag is runtime state and a node restart clears it. Today #18 accepts
only the nodetool form; its fix round makes it accept the schema option.

T is UTC in ISO-8601, ending in an upper-case `Z`, with at most six fractional digits:
`YYYY-MM-DDThh:mm:ss[.ffffff]Z`. Missing digits below the second count as zero.

## RPO and RTO

### RPO

While shipping is healthy, the recovery point objective on the worst shard is
bounded by:

```
RPO ≤ S + U + 1 s
```

- **S** is `pitr_seal_interval_in_seconds`, 60 s by default. It is the one
  number you choose. A lower S gives fresher protection, but brings memtable
  flushes on sooner, even at low load.
- **U** is the time from a seal until the segment and its ledger entry are in
  the store. While uploads keep up, it is about the segment's used bytes
  divided by the shard's upload rate, plus store latency. It is not measured
  yet.
- **1 s** is the coverage margin subtracted from every capture.

The seal syncs the segment, so the periodic commitlog sync doesn't add to the
bound. Lag, which is now minus covered-through, includes all three terms.

What makes the RPO grow:
- **A store outage.** Covered-through stops while the store is down, and
  catches up after it, if the unshipped data stays below the limit.
- **The outage latch.** Once unshipped data reaches
  `pitr_unshipped_limit_in_mb`, the chain ends and no later T is
  restorable until a restart and a new backup.
- **Writes faster than uploads.** If a shard writes faster than it uploads,
  U has no bound, and the latch is reached in the end.
- **An idle shard.** Covered-through doesn't advance on a shard with no
  writes, and the node's covered-through is the lowest of its shards. On a
  lightly used node it can stop for as long as one shard gets no writes.

The RPO is the window of acknowledged writes not yet restorable. It does not
describe surviving node loss, which this feature does not cover.

### RTO

The copy-ready time runs from the replay call to `done`. It is made of:
1. loading the backup's sstables from the store;
2. fetching the shipped segments, of every table on the node, from the
   backup's bound up to T;
3. replaying them into the target, and the final flush.

Step 2 depends on how much the whole node wrote since the backup, not on the
table's size, so a small table on a busy node pays for all of the node's
writes. Taking backups more often shortens it.

### Measured run

Pending: measured after the end-to-end restore lands (#18, fetch and replay,
not built yet).

| Item | Value |
|---|---|
| Hardware (CPU, memory, disk, network) | |
| Store (provider, region) | |
| Data size (backup, history since backup) | |
| Write rate | |
| Seal interval | 60 s |
| Upload rate per shard | |
| Observed RPO (worst lag) | |
| Observed RTO (load, fetch and replay, total) | |

## Limits

1. Experimental. One node, replication factor 1, vnode keyspaces only. This
   release will refuse: more than one node or RF other than 1; tablet
   tables, which are the default for new keyspaces; strongly consistent
   tables; counter tables; tables with views or secondary indexes, and view
   and index tables; tables with CDC; `logstor` tables; system and
   Alternator tables. These refusals aren't built yet (#15).
2. No recovery from node or disk loss, or from `DROP TABLE`. The result is
   always a new table. No route back into the original has been tested.
3. Don't load the copy's files into the original unless the original has
   been truncated: later deletes in the original still win, and rows stay
   missing with no error.
4. T compares write timestamps, which clients can set:
   - clients with clocks out of step can land on the wrong side of T;
   - an old-timestamped write arriving after a purged delete can bring a
     row back;
   - setting a collection to empty can look like a delete 1 µs earlier, so
     at exactly that T the collection reads as empty;
   - a delete batched with an element added 1 µs later can look like an
     overwrite, so at exactly the delete's T older deleted elements can
     reappear. The result counts such collections as not verified; a T
     1 µs off avoids both cases;
   - within one batch only the merged result exists: a T between its
     timestamps can't recover a value the batch itself overwrote.
5. Covered-through is the capture time minus 1 s. A write appended to the
   commitlog more than 1 s after its own timestamp, including one with a
   client-supplied timestamp, can be missed.
6. TTL isn't rewound: a cell alive at T may read as expired.
7. The backup may hold data newer than T. It isn't checked, and that data
   stays in the new table.
8. The named backup isn't checked for belonging to the table that has its
   name now. If the source was dropped and recreated since the backup, a
   restore can give `done` with data missing.
9. The backup is found by its prefix and snapshot tag. If one snapshot tag
   covers several tables, a prefix pointing at a sibling table with an
   identical schema is accepted. The result names the backup that was
   loaded: check it.
10. A shard with no writes stops the node's covered-through time. Any T past
    it is refused, and the refusal names the shard and its covered-through
    time. The RPO bound holds only while every shard receives writes.
11. Encryption at rest doesn't turn shipping off. Segments are shipped as
    the node holds them: encrypted if `system_info_encryption` is on,
    unencrypted otherwise, including tables with their own
    `scylla_encryption_options`. Replay refuses on a node with
    `system_info_encryption` on, so such a node has no PITR protection.
    Protecting the bucket is up to you.
12. A second node that joins and leaves within one seal interval isn't
    detected. Writes it acknowledged aren't in the history, and a restore
    past them can give `done` with those writes missing.
13. The result is given once, as the replay task's outcome, and is also
    written to the node log. It isn't stored with the table. A second replay
    into a `done` table is refused. Keep the result.
14. The status report will show no limits, and no per-table protection. A
    `TRUNCATE` or schema change since a listed backup isn't shown either.
    After a restart, its covered-through time is the new chain's.
15. A table created as a target but never replayed into is empty, and status
    doesn't list it. Status lists a target `incomplete` from the moment
    replay passes its checks until `done`. In CQL an unfinished target looks
    like any table.
16. After any `TRUNCATE` of the source since its shipping chain started,
    every T is refused, whether the `TRUNCATE` came before or after the
    backup. Use the normal backup restore to get the backup's data back.
    PITR needs a chain that started after the `TRUNCATE`, which means a node
    restart and a new backup.
17. After any change to the source's schema since the backup, including an
    option-only `ALTER`, every T after the backup's snapshot time is
    refused. Take a new backup after each schema change.
18. The target's schema is compared with the backup's as text, apart from
    name, ID and compaction. A backup taken on another Scylla version may
    describe the same schema differently and be refused.
19. If the keyspace's `durable_writes` was turned off and back on since the
    backup, writes made while it was off are missing, with `done`. This
    release will refuse `durable_writes` off when replay runs; that refusal
    isn't built yet (#15).
20. An older Scylla can't load a snapshot or backup taken with shipping on
    until `pitr-bound.txt` is removed from it.
21. A restart, a failed seal, a failed upload or ledger write, a second
    node, or the outage latch ends the chain. Later times need a new chain,
    which starts at the next node start, and a backup taken after it. A
    restored table isn't protected until its own backup.
22. The outage latch: once unshipped data reaches the hold limit, shipping
    releases every held segment so writes keep succeeding, and stops until
    restart. The chain ends there.
23. Stopping the node can wait about 30 s per shard while the store client
    gives up on an S3 credential lookup (#79).
24. A restore needs disk space for the whole node's writes since the backup,
    however small the table, and other activity on the node can use up space
    while it runs. There is no disk-space check before it starts.
25. The target must have compaction disabled: with `'enabled': 'false'` in
    its schema (accepted once #18's fix round lands) or with
    `nodetool disableautocompaction`. After `done` it stays disabled until
    you turn it on. The nodetool flag is lost on a node restart.
26. Writes into the target between the emptiness check and the load are
    mixed into the result. Don't write to the target before `done`.
27. The backup's `manifest.json` is read whole into memory on shard 0, up
    to 32 MiB. A backup with very many sstables needs that much free memory.
28. No automatic cleanup after a failed replay. The target stays, listed
    `incomplete`. Drop it and restore under a new name.
29. One restore at a time. No dry run, and no alerting: ask the status
    report.
30. A large write can be stored across more than one segment. If it is
    incomplete in the history, it isn't applied, and the result gives an
    upper bound on how many there were. Any acknowledged one is still in
    the source table.
31. Data loaded into the source after the backup other than by writes
    (refresh, load-and-stream, restore) isn't in the result.
32. Every table's writes on the node are shipped, including rows later
    deleted. Nothing in the bucket is deleted automatically, and deleting
    shipped objects can break restores.
33. Failed uploads can leave objects no restore uses, and you still pay for
    them. In a chain that has stopped shipping (an earlier boot), a segment
    object under `segments/` that no `<sequence>.entry` of that chain names,
    or one named only by entries after a missing sequence number, is never
    read. A store rule that clears only unfinished multipart uploads is safe.
34. Damage to a segment on the node's own disk, before it is shipped, is
    found only at restore, which then refuses times past the damage.
35. A lower seal interval gives fresher protection, but memtable flushes
    come sooner, even at low load.
36. The order of `fsync` calls before `done` will not be covered by the
    node tests, which run with fsync bypassed.
37. Buckets with object lock (write once, read many) aren't supported: a
    retried upload can write an object again with the same bytes.

## Implementation status (2026-11-07)

**Merged on `ow/pitr/v1`:**
- shipping of sealed segments to the configured store, with a per-shard
  chain and its floor (#81);
- the snapshot's `pitr-bound.txt`, with a strict reader (#80);
- ledger entries written in segment-id order, holds released after them
  (#85);
- the earlier restore pieces: the T parser, the chain check, the strict
  replay reader, the T filter, and apply to target.

**In review:** the `replay_to` endpoint, its checks, the `started` mark and
the load of the backup with compaction held off (#18).

**Not built yet:**
- #18's fetch and replay: fetch, replay and apply the shipped history,
  flush, and the durable `done`;
- the target check accepts the schema option once #18's fix round lands;
- the table-kind, `durable_writes` and one-node/RF refusals (#15);
- the status GET;
- the TRUNCATE and schema-change refusals, and the encryption and idle-shard
  refusals;
- the marker that shows a backup's upload finished, so an interrupted
  backup is refused;
- the outage latch (in progress).

Until these land, a restore isn't usable: the endpoint loads the backup and
then fails on purpose, leaving the target `incomplete`.

## Testing

- **Unit tests,** one suite per module, in `test/boost/pitr_*_test.cc`:
  the T parser, the ledger entry format, the ledger writer, segment upload,
  the shipper and its wiring, the bound file and its capture, the chain
  check, the replay reader, the T filter, apply to target and, in review,
  `replay_to`.
- **Node tests** are planned, in review in #84: ten end-to-end tests
  against a single node with a local object store, in
  `test/cluster/object_store/`. They cover exact at T on every shard, deleted rows staying deleted,
  compaction during replay, T past coverage (including an idle shard),
  damaged history, TRUNCATE, a schema change, an unfinished backup upload,
  a kill during writes, and a durable `done` beside an `incomplete` target.
  Each will have an in-test control and a deliberate break, and stays
  skipped until the pieces it needs land.
- Not planned for node tests: tablets and the other refused table kinds,
  two-node cases, encryption, `durable_writes`, the outage latch, and the
  measured run.
