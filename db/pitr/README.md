# Single-node point-in-time recovery (experimental)

## What it is

Point-in-time recovery (PITR) restores one table to how it was at a chosen
time T, into a new table, without touching the original. It is meant for
undoing logical damage: a bad delete, a bad application release, a wrong
update. It works by shipping the node's sealed commitlog segments to object
storage (S3 or GCS) as they close, next to the sstable backups that
`nodetool backup` already takes. A restore loads a backup into an empty table
and then replays the shipped commitlog on top of it, up to T. It is limited
to one node, replication factor 1, and vnode keyspaces, and does not protect
against node or disk loss.

**Status: experimental, not for production.** A restore of one table to T
runs end to end, but some refusals this README lists aren't built, so some
restores report `done` on wrong data (limits 1, 16, 17 and 19). A deleted
row can also come back with `done`, on default settings too (limit 38). A
restore from encrypted segments hasn't been tested (limit 11). A full
re-review of the restore path is open (#92). See
[Implementation status](#implementation-status-2026-11-09).

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
     configured, the target exists and is not in a tablets keyspace, no
     replay is running into it or has run into it, its auto-compaction is
     off, it is empty, the backup's `manifest.json` names the given
     snapshot, `pitr-bound.txt` is present and valid, the target's schema
     matches the backup's apart from the name, the ID and compaction, and
     on every shard the chain the backup names is dense from the backup's
     bound and covers T. A refusal leaves the node as it was. There is no
     TRUNCATE or schema-change refusal yet (limits 16 and 17).
   - It writes a durable `started` mark for the target, under the node's
     work directory. From then on the target is `incomplete`.
   - It holds every compaction of the target off, on every shard, for the
     whole run, and loads the backup's sstables listed in its
     `manifest.json` through the existing restore path.
   - It fetches the shipped segments, replays them strictly, keeps only
     writes with a timestamp at or below T, and applies them to the target.
   - It flushes the target and replaces the mark with a durable `done`.
   If a compaction could have run on the target during the run, the task
   fails and never reports `done`. Its error reads `Point-in-time replay
   of <ks>.<target> failed: target_compaction_during_replay: ...`: `failed`
   rather than `refused`, because the run had already started.
5. On success the task's status says only `done`. The result line, with
   the backup loaded and the counts, goes to the node log (limit 13).

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
| `pitr_unshipped_limit_in_mb` | 256 | Unshipped data held on each shard, in MiB, before shipping gives up on protection so writes keep succeeding. 0 turns shipping off |

Shipping is off while `pitr_endpoint` and `pitr_bucket` are both unset. With
only one of them set, a value out of range, an unknown endpoint, more than
one node, or the commitlog disabled, shipping stays off and the node logs
why. The commitlog clamps the limit it holds segments against to 3/4 of the
lower of the shard's flush threshold and its share of
`commitlog_total_space_in_mb`, so a store outage reaches the limit before it
triggers memtable flushes or blocks writes. When the held segments reach
that limit, the outage latch releases them and ends the chain. The clamp is
not only a safety margin: on a shard with little memory it lands below one
segment, and then the latch trips with the store healthy (limit 22). The
commitlog logs the limit that applies at startup; the figures that line
labels `MB` are MiB.

The endpoint is configured as for backup and restore
(`docs/dev/object_storage.md`):

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
access denied. A restore tells a missing key from a failed read by asking
the store whether the key exists, and every chain check ends on a missing
key, so without `s3:ListBucket` every restore fails with a store error. The
full list of actions hasn't been checked against each store. Buckets with
object lock aren't supported (limit 37).

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

# 4. Replay to T. Wait until shipping has reached T first: about
#    pitr_seal_interval_in_seconds plus a few seconds after T (see below).
curl -X POST "http://localhost:10000/storage_service/pitr/replay_to\
?endpoint=https://s3.us-east-1.amazonaws.com:443&bucket=my-pitr-bucket\
&prefix=backups/ks/t/snap1&keyspace=ks&table=t_restored&snapshot=snap1\
&t=2026-11-04T14:05:00.000000Z"
# -> "<task id>"

# 5. Wait for the task. On success, keep the result line from the node log.
#    A refusal fails the task with the reason as its error text. On
#    t_past_coverage, T isn't restorable yet (see below).
nodetool tasks wait <task id>

# 6. Check ks.t_restored, then turn compaction on for it: restate the
#    compaction option from schema.cql without 'enabled': 'false'.
cqlsh -e "ALTER TABLE ks.t_restored WITH compaction = {'class': '<class from schema.cql>'}"
```

If you disabled compaction with `nodetool disableautocompaction ks t_restored`
instead, turn it back on with `nodetool enableautocompaction ks t_restored`.
That flag is runtime state and a node restart clears it. Replay accepts
either form.

**When T is restorable.** A write can be restored once its segment is
sealed and shipped, about S + U + 1 s after it (see [RPO](#rpo)). With the
default S of 60 s, wait about a minute and a few seconds past T before
step 4. There is no status report to ask yet (#14). Before then the replay
is refused, and the task's error reads (one line, wrapped here):

```
Point-in-time replay of ks.t_restored refused: t_past_coverage: T is
2026-11-04T14:05:00.000000Z, and shipping from this node has only reached
<covered-through>, a lag of <lag>. Retry with a T at or below that time.
(shard <n>)
```

The covered-through time is that of the shard that lags most. This check
runs before the mark, so the target stays empty and can be reused: wait and
call the replay again with the same T, or call it now with a T at or below
the time the refusal names. If covered-through doesn't move between
retries, a shard gets no writes or shipping has stopped (limits 10, 21 and
22): check the node log.

T is UTC in ISO-8601, ending in an upper-case `Z`, with at most six
fractional digits: `YYYY-MM-DDThh:mm:ss[.ffffff]Z`. Missing digits below the
second count as zero.

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
  divided by the shard's upload rate, plus store latency. Against a local
  store it was at most about 1.4 s (see below); the shipper doesn't log the
  seal, so this is derived, not measured directly (#116).
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
2. fetching the shipped segments, of every table on the node, of the whole
   chain the backup names: from where shipping started at the node start the
   backup was taken in, to the chain's last entry, past T if the chain goes
   on past it. The backup's bound and T are applied to each entry after the
   fetch (#117);
3. replaying them into the target, and the final flush.

Step 2 depends on how much the whole node wrote in the backup's chain, before
and after T, not on the table's size or the backup's age, so a small table on
a busy node pays for all of the node's writes. Taking backups more often
doesn't shorten it today; #117 would make it.

### Measured run

Measured on 2026-11-08 with `test/cluster/object_store/test_pitr_measure.py`
(run with `PITR_MEASURE=1`; sizes are environment variables). The release
build ran without LTO. The store was a local mock, so U and the fetch are
optimistic: against a remote store the fetch, one segment at a time, weighs
more.

| Item | Value |
|---|---|
| Hardware | aarch64 VM, 8 vCPU, 15.6 GB; one node, `--smp 2`, `--memory 2G` |
| Store | local mock S3, no network latency or bandwidth limit |
| Data size | 200 MB backup, then 300 s of writes (78k rows of 1 KB) |
| Write rate | 264 KB/s in total, spread over both shards |
| Seal interval | 60 s, and 10 s for the lag |
| Upload, seal to ledger entry (U) | at most about 1.4 s in 6 kills, derived as RPO − kill phase − 1 s (#116) |
| Lag, now minus covered-through (release) | S = 10 s: median 6.6 s, highest sample 8.3 s. S = 60 s: median 31 s, highest sample 56.8 s. Sampled every 5 s, so the peak, about S + U + 1 s, falls between samples |
| RPO after `kill -9` at a random point, S = 60 s (release) | 15.7 s, 28.7 s and 60.1 s: the kill's point in the seal cycle plus 1.7 to 2.4 s. No acknowledged write older than L was missing |
| RTO, release build | 7.6 to 7.8 s: checks 0.2 s, loading the backup 0.5 s, fetching and replaying 360 MB in 18 segments 6.8 to 7.1 s, flush 0.1 s |
| RTO, dev build | 12.0 to 12.5 s |
| Larger run, release | 1 GB backup and 900 s of writes (234k rows): lag highest sample 59.98 s; RTO 55 to 58 s, of which loading the backup 9 to 10 s and fetching and replaying 1.63 GB in 56 to 58 segments 45 to 47 s. RPO, 5 kills each on a separate 5 MB backup with 120 s of writes: 14.4 to 60.1 s |

## Limits

1. Experimental. One node, replication factor 1, vnode keyspaces only.
   Today replay refuses a target in a tablets keyspace, which is the
   default for new keyspaces, and shipping turns itself off on more than
   one node. Replay doesn't yet refuse RF other than 1, strongly
   consistent tables, counter tables, tables with views or secondary
   indexes, view and index tables, tables with CDC, `logstor` tables, or
   system and Alternator tables (#15): a restore of one of them goes ahead
   unchecked. Don't use PITR for them.
2. No recovery from node or disk loss, or from `DROP TABLE`. The result is
   always a new table. No route back into the original has been tested.
3. Don't load the copy's files into the original unless the original has
   been truncated: later deletes in the original still win, and rows stay
   missing with no error. Truncating first has its own cost: every write
   made to the original after T is dropped, also with no error. If any of
   those writes must stay, copy the rows you need from the new table
   instead.
4. T compares write timestamps, which clients can set:
   - clients with clocks out of step can land on the wrong side of T;
   - an old-timestamped write arriving after a purged delete can bring a
     row back, on default settings too (limit 38);
   - setting a collection to empty can look like a delete 1 µs earlier, so
     at exactly that T the collection reads as empty;
   - a delete batched with an element added 1 µs later can look like an
     overwrite, so at exactly the delete's T older deleted elements can
     reappear. The result counts such collections as not verified; a T
     1 µs off avoids both cases. Non-frozen user-defined types are
     multi-cell and take the same path as collections, so overwriting one
     as a whole has the same two shapes. Frozen collections and frozen
     UDTs are single cells and aren't affected;
   - within one batch only the merged result exists: a T between its
     timestamps can't recover a value the batch itself overwrote.
5. Covered-through is the capture time minus 1 s. A write appended to the
   commitlog more than 1 s after its own timestamp, including one with a
   client-supplied timestamp, can be missed.
6. TTL isn't rewound: a cell alive at T may read as expired.
7. The backup may hold data newer than T. It isn't checked, and that data
   stays in the new table, with `done`. Pick a backup taken at or before T.
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
    `scylla_encryption_options`. There is no refusal for a node with
    `system_info_encryption` on: replay opens the shipped segments through
    the node's commitlog extensions, and a restore from encrypted segments
    hasn't been tested. Treat such a node as having no PITR protection.
    Protecting the bucket is up to you.
12. A second node that joins and leaves within one seal interval isn't
    detected. Writes it acknowledged aren't in the history, and a restore
    past them can give `done` with those writes missing.
13. On success the replay task's status says only `done`. The result line
    (the backup loaded, the counts, and the upper bound on split writes) is
    written to the node log only. Refusals and failures are the task's
    error text. Neither is stored with the table. A second replay into a
    `done` table is refused. Copy the result line from the node log.
14. The status report will show no limits, and no per-table protection. A
    `TRUNCATE` or schema change since a listed backup isn't shown either.
    After a restart, its covered-through time is the new chain's.
15. A table created as a target but never replayed into is empty, and
    nothing records it as a target. Once replay passes its checks it writes
    a `started` mark, and the target is `incomplete` until the mark says
    `done`. In CQL an unfinished target looks like any table. Today only the
    task's status, the node log (each mark it writes) and a second replay
    into it, refused as `target_already_replayed` with the mark's phase,
    show it. The status report will list it once it is built (#14).
16. There is no `TRUNCATE` refusal yet. A restore to a T after a
    `TRUNCATE` of the source reports `done` with the truncated rows back.
    Don't restore to such a T. Use the normal backup restore to get the
    backup's data back. A T after a `TRUNCATE` needs a chain that started
    after it, which means a node restart and a new backup.
17. There is no schema-change refusal yet. After a change to the source's
    schema since the backup, a restore to a later T can report `done` with
    a dropped column still holding the backup's values, or fail partway,
    leaving the target `incomplete`, on a write at or below T to an added
    column. Take a new backup after each schema change, and restore from
    one taken after the last change before T.
18. The target's schema is compared with the backup's as text, apart from
    name, ID and compaction. A backup taken on another Scylla version may
    describe the same schema differently and be refused.
19. If the keyspace's `durable_writes` was turned off and back on since the
    backup, writes made while it was off are missing, with `done`. This
    release will refuse `durable_writes` off when replay runs; that refusal
    isn't built yet (#15). Take a new backup after `durable_writes` is
    turned back on, and restore only to a T after it.
20. An older Scylla can't load a snapshot or backup taken with shipping on
    until `pitr-bound.txt` is removed from it.
21. A restart, a failed seal, a failed upload or ledger write, a second
    node, or the outage latch ends the chain. Later times need a new chain,
    which starts at the next node start, and a backup taken after it. A
    restored table isn't protected until its own backup.
22. The outage latch: once unshipped data reaches the hold limit, shipping
    releases every held segment so writes keep succeeding, and stops until
    restart. The chain ends there. The limit that applies can be far below
    `pitr_unshipped_limit_in_mb`: the commitlog clamps it to 3/4 of the
    lower of the shard's flush threshold and its share of
    `commitlog_total_space_in_mb`, which on a shard with little memory is
    less than one segment. The latch then trips on the first closed
    segment that hasn't been recorded yet, with the store healthy, and a
    restart trips it again until the shard has more memory or the
    commitlog a larger share of it. Before the latch trips, only the
    commitlog's line in the node log at startup names the limit that
    applies; the latch's own reason then names it in bytes. No metric or
    status reports it (#88, future work).
23. Stopping the node can wait about 30 s per shard while the store client
    gives up on an S3 credential lookup (#79).
24. A restore needs disk space for the whole node's writes since the backup,
    however small the table, and other activity on the node can use up space
    while it runs. There is no disk-space check before it starts.
25. The target must have compaction disabled: with `'enabled': 'false'` in
    its schema or with `nodetool disableautocompaction`. After `done` it
    stays disabled until you turn it on. The nodetool flag is lost on a
    node restart.
26. Writes into the target between the emptiness check and the load are
    mixed into the result. Don't write to the target before `done`.
27. The backup's `manifest.json` is read whole into memory on shard 0, up
    to 32 MiB. A backup with very many sstables needs that much free memory.
28. No automatic cleanup after a failed replay. The target stays, with its
    `started` mark: the task's error text says why the run failed (for a
    compaction let in during the run, `target_compaction_during_replay`), and a
    second replay into it is refused as `target_already_replayed`. Drop it
    and restore under a new name.
29. One restore at a time into one target: a second replay into a target one
    is already running into is refused as `replay_already_running`. Replays
    into different targets run side by side, and nothing bounds how many,
    so they share the node's disk and store bandwidth. No dry run, and no
    alerting: the task's status and the node log are the only signals until
    the status report is built (#14).
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
    them. In a chain that has stopped shipping, from an earlier boot or
    broken earlier in this one (limit 21), a segment object under
    `segments/` that no `<sequence>.entry` of that chain names, or one named
    only by entries after a missing sequence number, is never read. A store
    rule that clears only unfinished multipart uploads is safe.
34. Most damage to a segment on the node's own disk, before it is shipped,
    is found only at restore, which then refuses times past the damage: a
    size other than the recorded one, a file cut or unreadable, a zeroed,
    foreign or wrong-segment header, and a sector whose checksum doesn't
    match. One kind isn't detected: an all-zero sector inside the used
    bytes of a segment whose header is intact. The read skips the
    checksum of an all-zero sector, so the restore can end that segment
    early, leave a hole with the entries after it still applied, or apply
    a mutation assembled across the hole, and still report `done` (#150).
    The upload CRC doesn't catch it, because it is computed over the
    bytes as uploaded, damage included. It is rare: it takes a zeroed
    4 KiB sector within a segment's used bytes.
35. A lower seal interval gives fresher protection, but memtable flushes
    come sooner, even at low load.
36. The order of `fsync` calls before `done` will not be covered by the
    node tests, which run with fsync bypassed.
37. Buckets with object lock (write once, read many) aren't supported: a
    retried upload can write an object again with the same bytes.
38. A restore fetches every shipped segment of the chain the backup names,
    including those holding only writes from before the backup. The
    backup's bound sits lower than the backup needs (cause open, #117), so
    many of those writes are replayed again on top of the loaded backup.
    That gives the same data as long as every write that shadows a replayed
    one is in the backup or is replayed too, which holds for deletes after
    the backup and for TTL. The exception is a tombstone the backup's
    compaction already purged, together with an older-timestamp write of
    the same row that is still in the chain: the replay can bring that row
    back and still report `done` (#126). This happens on default
    settings: PITR supports only replication factor 1, where the default
    `tombstone_gc` is `repair`, which purges a tombstone with no grace wait
    once no live commitlog segment still holds an older write to the
    table. A short `gc_grace_seconds` under `timeout` does it too. The
    older write must arrive after the delete: `USING TIMESTAMP`, or
    client-side timestamps with clock skew or a delayed retry. Plain server
    timestamps don't trigger it. Setting
    `enable_tombstone_gc_for_streaming_and_repair=true` (live-update,
    off by default) opens the same loss from the other side: loading the
    backup through `download_and_stream` reads its sstables with a
    compacting reader that may purge any collectable tombstone, which the
    target's disabled compaction (limit 25) doesn't cover, so a tombstone
    still in the backup can be dropped as it is loaded and an older write
    replayed over it brings the row back. On protected tables, set
    `tombstone_gc = {'mode': 'timeout'}` and keep `gc_grace_seconds` at
    its default of 10 days. What matters is how far the backup's replay
    start can lag behind the backup, not the backup interval alone: the
    start can go back to the oldest live commitlog segment of the shard,
    which stays live while any table on that shard has unflushed writes
    in it, or while shipping is held by a store outage, so it can be
    hours or days old. A lower `gc_grace_seconds` must stay well above
    that lag plus the backup interval, and even the 10-day default is
    beaten by a store outage that holds shipping for longer than it. It
    also costs time: RTO grows with everything the node wrote in that
    chain. In the 1 GB measured run, all 1.28 million writes were
    replayed, about 80% of them already in the backup.
39. An I/O error while the replay reads a fetched segment copy from the
    restore's work directory can isolate the node, not only fail the run.
    The reader reads the copy through `commitlog::read_log_file`, which
    opens it through the commitlog's own error handler
    (`commit_error_handler`), so a read error on that copy raises the
    commitlog's disk-error signal and the node acts on it under its
    commit failure policy (#149). Fixing it needs the commitlog read path
    to take an error handler of its own.

## Implementation status (2026-11-09)

**Merged on `ow/pitr/v1`:**
- shipping of sealed segments to the configured store, with a per-shard
  chain and its floor (#81), ledger entries in segment-id order (#85), and
  the outage latch (#87);
- the snapshot's `pitr-bound.txt`, with a strict reader (#80), and the bound
  taken from the sstables, raised to the chain floor (#106);
- the `replay_to` endpoint, its checks, the `started` mark and the load of
  the backup with compaction held off (#94-#96);
- the chain check of every shard before the mark (#107), the fetch of each
  segment with size and CRC checks (#113), and the replay into the target,
  the flush and the durable `done` (#114);
- the measured-run harness (#115) and its numbers above (#118);
- the slim-core black-box node tests (#84), see [Testing](#testing);
- the end of a chain: a next entry the store itself reports as absent
  (S3 `NoSuchKey`, GCS `ENOENT`) ends the chain where it is read, instead
  of failing a replay that raced the shipper's next entry (#132, fixes
  #131);
- the reason code `target_compaction_during_replay` on both compaction
  guards, so the failure can be told apart by its code (#137, #111);
- the S3 per-client metrics labelled with the endpoint's host and port
  when the port isn't the scheme's default, so two endpoints on one host
  can each have a client on a shard (#136, fixes #112). The group-client
  metrics are still labelled by host alone (#135).

A restore of one table to T runs end to end, and the slim-core node
tests of the core requirements pass on the merged code (#84). A
full-level re-review of the restore path is still open (#92) before a
restore is called usable in production.

**Not built yet:**
- the table-kind, `durable_writes` and one-node/RF refusals (#15);
- the TRUNCATE, schema-change and encryption refusals (limits 16, 17 and
  11);
- the status GET (#14);
- the marker that shows a backup's upload finished, so an interrupted
  backup is refused (#12).

**Future work, found by the measured run (#21):**
- make RTO follow the history since the backup: skip the chain segments
  wholly below the backup's bound, and find out why the bound sits lower
  than the backup needs, so writes already in the backup aren't replayed
  again (#117). Besides the time, a tighter bound narrows the purged-
  tombstone case in limit 38;
- log the seal instant, so U is measured directly rather than derived
  (#116).

## Testing

- **Unit tests,** one suite per module, in `test/boost/pitr_*_test.cc`:
  the T parser, the ledger entry format, the ledger writer, segment upload,
  the shipper and its wiring, the bound file and its capture, the chain
  check, the replay reader, the T filter, apply to target and
  `replay_to`. The outage latch and the more-than-one-node refusal are
  covered here, in the shipper's suite.
- **Node tests,** against a single node with a local object store, in
  `test/cluster/object_store/`. `test_pitr_replay_endpoint.py` runs on the
  merged code: the endpoint's refusals, including a tablets target, the
  load of the backup, a full run to exact data at T on every shard with
  its result line in the node log, the compaction hold, and a failure or
  an abort leaving the target `started`. `test_pitr_measure.py` is the
  measured run, skipped unless `PITR_MEASURE=1`.
- **Black-box node tests,** merged on 2026-11-09 (#84), in
  `test_pitr_restore.py` (the restore, the T filter and the deleted-row
  cases), `test_pitr_refusals.py` (the refusals and the bound file),
  `test_pitr_shipping.py` (shipping off, and the seal interval with its
  range) and `test_pitr_failures.py` (a `kill -9` never gives `done` with
  an acknowledged write missing). They use only what an operator has:
  CQL, REST, nodetool, configuration, the bucket, and starting or killing
  the node.
- Of the tests in those files, the 30 that cover the feature's seven core
  requirements each ran 100 of 100, or 20 of 20 for the slower ones: the
  kill, restart, interrupted-backup and shipping tests, with no failure.
  The files hold further tests that ran 5 of 5 or 10 of 10 and have not
  been reviewed or run 100 of 100. One of those is skipped on #100: the
  `done` result carries no warning against loading the copy back into
  the original (limits 3 and 13), because a task has no success text.
- A black-box test stays skipped until every product piece it needs is
  listed in `LANDED_PHASES` in `pitr_util.py`, which holds `ep.3`, the
  `replay_to` endpoint. Still skipped are the idle-shard refusal, which
  waits on a refusal that names the idle shard (see [RPO](#rpo), "An
  idle shard"); the refusal of a `TRUNCATE` before the backup, which
  waits on a refusal that isn't built (limit 16); the schema-change
  refusal, which waits on that and on the status GET (limit 17, #14);
  the refusal of a backup whose upload didn't finish, which waits on the
  marker of #12; and `incomplete` and a durable `done` across a kill,
  which wait on the status GET (#14). The test for a `TRUNCATE` after the
  backup is skipped on #143, and the one for a purged tombstone on #126
  (limit 38). Most tests that take a requirement as written, ignoring
  today's limits, aren't written yet.
- No running node test covers the other refused table kinds, two nodes,
  encryption, `durable_writes` or the outage latch.
