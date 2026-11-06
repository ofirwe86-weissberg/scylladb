# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current task

See `docs-work/assignment.md`, and `docs-work/handoff.md` for the current state. `docs-work/` is in the main checkout only, not in worktrees, so use its absolute path there.

- **Scope:** a home assignment meant for 2-3 days, deadline 2026-11-09. Build the minimum that gives a correct restore of one table to time T, or a clear refusal. Everything else becomes a stated limit or future work in the README. The one exception is anything whose absence would give a silently wrong restore: that stays.
- **Team level:** every task runs at the level its label gives (see "Team level per task"). The process cuts P1-P4 in `docs-work/implementation-plan.md` ("Estimate and corner cuts") are the light level. The PRs P1 names as risky (#12, #15 and the `replay_to(T)` endpoint) are full. The documents stay frozen unless an issue means silent wrong data.

## Repo

**ofirwe86-weissberg/scylladb** — a fork of ScyllaDB, a Cassandra/DynamoDB-compatible, shared-nothing NoSQL database written in C++ on the Seastar framework. Core values: performance, correctness, readability.

Upstream: `scylladb/scylladb`. Default branch: `master`. This checkout has two remotes: `origin` is **upstream** (`scylladb/scylladb`) and `fork` is `ofirwe86-weissberg/scylladb`. Push only to `fork` (`git push fork <branch>`), never to `origin`, and always target the fork explicitly with `gh`: `-R ofirwe86-weissberg/scylladb` for `gh pr` / `gh issue` / `gh label`, and the repo as a positional argument for `gh repo view ofirwe86-weissberg/scylladb`.

`AGENTS.md` routes to detailed style guides. Read the relevant one before editing:
- `.github/copilot-instructions.md` — build/test commands, code and test philosophy (always)
- `.github/instructions/cpp.instructions.md` — for `*.cc`/`*.hh` (Seastar patterns, memory, error handling, naming, formatting)
- `.github/instructions/python.instructions.md` — for `*.py` (`test/cqlpy` and `test/alternator` deliberately omit type hints/docstrings)

## Stack & layout

- Language: C++ built with `-std=gnu++26` (see `configure.py`; the style guide says "C++23, prefer modern features, especially coroutines"). Rust components under `rust/`; Python for tests and tooling.
- Concurrency: Seastar — shared-nothing, one thread per core (shard), futures/coroutines, cooperative scheduling.
- Build: `configure.py` → `build.ninja` → ninja (a CMake build exists but is secondary). Toolchain: frozen container toolchain `tools/toolchain/dbuild` (image in `tools/toolchain/image`). `dbuild` uses podman if present, else docker; on this Linux/aarch64 host it's podman. **Run everything through `./tools/toolchain/dbuild`**: ninja, uv, clang-format and ccache live inside the toolchain image and are intentionally not installed on the host, so don't install them natively. Verified here: `./tools/toolchain/dbuild ninja -j6 build/dev/scylla` and `./tools/toolchain/dbuild ./test.py --mode=dev test/cqlpy/test_json.py` both work with no override flags. Use `dev` mode with `-j6`. Don't mix dbuild and native builds.
- Submodules (`seastar`, `abseil`, `fmt`, `c-ares`, `swagger-ui`, `tools/cqlsh`, `tools/python3`) must be initialized: `git submodule update --init --force --recursive`.
- Key directories (full map: `docs/dev/repository_layout.md`):
  - `cql3/`, `transport/` — CQL grammar/statements and native protocol; `alternator/` — DynamoDB API; `api/` — REST API
  - `service/` — storage_proxy, storage_service, topology coordinator, group0 (`service/raft/`); `raft/` — standalone Raft library; `locator/` — replication, tablets; `gms/` — gossip; `message/` + `idl/` — inter-node RPC and its serialized types
  - `replica/`, `mutation/`, `readers/`, `sstables/`, `db/` (commitlog, config, system keyspaces), `compaction/`, `repair/`, `streaming/` — storage engine
  - `test/` — all tests; `test/pylib` and `test/lib` hold shared helpers

## Build & test commands

```sh
# Configure (once per worktree; re-runs automatically when configure.py changes)
./tools/toolchain/dbuild ./configure.py

# Build — dev is the fastest iteration mode; build the narrowest target
./tools/toolchain/dbuild ninja build/dev/scylla                    # server binary; enough for Python tests
./tools/toolchain/dbuild ninja build/dev/test/boost/<binary>       # a C++ test binary
./tools/toolchain/dbuild ninja dev-headers                         # self-contained-header check (touch configure.py first if headers were added/removed)

# Unit tests (C++) — test.py does NOT rebuild; many tests live in composite binaries
# (e.g. test/boost/combined_tests): find the binary in configure.py or test/<suite>/CMakeLists.txt
./tools/toolchain/dbuild ./test.py --mode=dev test/boost/<name>_test.cc
./tools/toolchain/dbuild ./test.py --mode=dev test/boost/<name>_test.cc::<test_case>
# known-good example (33 tests, ~1s once build/dev/scylla is current):
./tools/toolchain/dbuild ./test.py --mode=dev test/boost/group0_voter_calculator_test.cc

# Functional / integration tests (Python); full path with .py
./tools/toolchain/dbuild ./test.py --mode=dev test/cqlpy/<file>.py::<test_function>
./tools/toolchain/dbuild ./test.py --mode=dev test/cluster/<file>.py -v --repeat 5
# cqlpy and alternator also run standalone: test/cqlpy/run <file>.py::<test>  (uses newest build/*/scylla, or $SCYLLA)

# Sanitizer / debug (slow — use for memory/lifetime-sensitive changes)
./tools/toolchain/dbuild ninja build/sanitize/scylla   # or build/debug/...; then ./test.py --mode=sanitize ...

# Format: new files whole, changed lines only in existing files. Never run git inside dbuild (see below)
./tools/toolchain/dbuild clang-format -i <new_file>
./tools/toolchain/dbuild clang-format -i --lines=<start>:<end> <existing_file>
```

**Don't run git inside `dbuild`.** In a worktree it fails, because the main `.git` isn't mounted. Mounting it with `-v` coincided to the second with Git LFS hooks appearing in the shared `.git/hooks`, which blocked all pushes until `git-lfs` was installed on the VM.

**When builds overlap,** give each its own sccache port, or sccache fails with "No such file": `./tools/toolchain/dbuild --env SCCACHE_SERVER_PORT=<port> -- ninja -j3 <target>`.

Build modes: `debug`, `dev`, `release`, `sanitize`. Agents use `dev` by default. If `test.py` hits cgroup-metrics permission errors, add `--no-gather-metrics`. Logs land in `testlog/<mode>/`. A full build is long (hours on modest hardware, ~2–3 GB RAM per compile/link thread), so build the narrowest target. `configure.py --compiler-cache` defaults to `auto` and picks up the image's `sccache` (`build.ninja`: `cxx_launcher = /usr/bin/sccache`), which is what makes a rebuild in a fresh worktree survivable. Measured here at `-j6`: an incremental `build/dev/scylla` relink took 11m32s.

**Max parallel agents: 3, and only for tasks that don't block each other.** Every agent works in its own worktree with its own build tree, so N agents means N full builds competing for cores and RAM. The team lead's caution on this 15 GB VM is at most 2 heavy builds at once.

**Before trusting a test result on this Lima VM:** `fs.aio-max-nr` is 1048576, and clangd background indexing is off (`--background-index=false`). Both produced failures with no code at fault.

Test suites: `boost`/`unit`/`raft` (C++), `cqlpy` and `alternator` (single-node Python), `cluster` (multi-node, `test/pylib` manager), `cql` (approval tests; review any `.reject` diff before overwriting `.result`), `rest_api`, `nodetool`, `scylla_gdb`; also `vector_search`, `ldap`, `tools`, `perf`, `dist_test`, `manual` and `pylib_test` (`ls test/` for the full set). cqlpy tests should also pass on Cassandra.

## Conventions

- Formatting: root `.clang-format`; apply to changed lines only, never reformat untouched code. Four spaces, braces on all scopes, `snake_case`, `_member` prefix. Follow the Seastar coding style and the surrounding file.
- Error handling: exceptions by default (futures propagate them); in coroutines prefer `co_return coroutine::exception(...)`; on the data path avoid exceptions (`std::expected`/`boost::outcome`). Invariants: `throwing_assert()` (`utils/assert.hh`), `SCYLLA_ASSERT` only where abort is unavoidable, `on_internal_error()` for should-never-happen. Never `new`/`delete`, `std::mutex`, blocking calls, or ad-hoc macros.
- Memory/ids: `std::unique_ptr` by default, `lw_shared_ptr` within a shard, `foreign_ptr` across shards; new UUID-style ids use `utils::tagged_uuid`; `utils::chunked_vector` for allocations >128KB.
- Logging: `seastar::logger` per subsystem, structured, never log credentials/PII.
- Commit messages: `module: imperative summary` (e.g. `sstables: close file-descriptor on error`; `tree:` for whole-tree changes); body explains why; reference issues (`Fixes #N`). Signed-off-by is not consistently used in recent history. Each commit must compile and pass tests (bisectable). See `docs/dev/review-checklist.md`.
- New files: SPDX header with `LicenseRef-ScyllaDB-Source-Available-1.1`, current year for new files only.
- Tests: every bug fix comes with a test that fails before and passes after, and cites the GitHub/JIRA issue. New tests must be stable (5 of 5 while developing, and 100 of 100 after review at the full level, as in Development rules), fast, deterministic, and free of sleeps; reduce iterations in debug mode.
- Question requests rather than implementing blindly; flag trade-offs.

## Development rules

These apply to all implementation and review on this project.

**Reactor stalls: treat this code as highly stall-sensitive.**
- A stall is any command that takes time without giving the shard back. One shard runs everything on one thread, so while it runs, every other request on that shard waits. Code that isn't effectively instant must either be asynchronous (`co_await`, which frees the shard) or yield regularly.
- While writing code, ask of every loop, parse, serialization, checksum and allocation: how big can its input get, and does it yield? Bound the work or yield with `coroutine::maybe_yield()`, and avoid large contiguous allocations. Never make a blocking call.
- Reviewers check for stalls explicitly, on the worst-case input and not the typical one.
- Passing tests don't show the absence of stalls: `test/pylib/cpp/base.py:47` sets `--blocked-reactor-notify-ms=2000000` for C++ tests, so stalls aren't reported there.
- **If you're not sure whether something can stall, don't decide alone.** Discuss it with `architect-check` first, then raise a flag to the human with what you concluded and what's still unknown.

**Tests are written with the code, not after it.**
- Work in small steps: write a test, see it fail for the right reason, write the code, see it pass, continue. Don't write the tests at the end to fit finished code.
- Don't commit while a test fails, and ask the human if any test fails, even one that looks unrelated. Each commit holds a step's test and code together, so it compiles and passes.
- New tests run 5 of 5 (`--repeat 5 --max-failures 1`) while developing, and, at the full level, 100 of 100 after review passes, before merge (see "Team level per task").
- While developing, build the task's own small test binary and run only relevant cases. Build composite binaries like `combined_tests` once per task, at the end.
- Show that a test can fail: break the code on purpose, or record its failure before the fix. A test that has never failed proves nothing.
- QA's tests are checked before and after: each is run without the change and must fail, then with the change and must pass, and both results go in the PR. Failing only because an endpoint doesn't exist isn't enough: the test must also catch wrong behaviour.

**The test bar is p99, not the happy path.** Passing is the minimum. Tests must take the code to its edge cases: boundary values, maximum sizes, empty inputs, failures at each step, restarts, concurrency and reordering. Before finishing, list the edge cases you considered, and say which are tested and which aren't. Random inputs must use a fixed, logged seed so that a failure can be reproduced.

**Comments explain why, and only what isn't obvious.** Match the repo: headers carry the contract (median about 20%, up to about 45% of lines), `.cc` files less (median about 10%, up to about 25%). GitHub issue numbers are fine, but don't reference files or requirement IDs outside the repo.

**The detail lives in the task, not the code or commit.** The GitHub issue holds the full reasoning, decisions, evidence and test notes, and the team lead makes sure it's detailed enough for the developer to work from. Commit bodies stay around 15-30 lines (repo median 13, 90th percentile 32) and refer to the issue. Comment blocks in `.cc` files stay under about 5-10 lines (repo 90th percentile 5), unless they explain an algorithm.

**Keep tests simple.** One behaviour per test, named after that behaviour, with minimal setup, so a failure says what broke. Group them in batches by functionality. Prefer many small tests over one complex scenario.

**Test design is agreed with the human before test code is written** for a new feature: what's tested, at which level, and with which control points.

## Architecture (big picture)

- **Seastar** (`seastar/`): sharded runtime; most code runs on one shard without locks; cross-shard work via `smp::submit_to`, `sharded<>`, `foreign_ptr`. `main.cc` wires every `sharded<>` service and its startup/shutdown order.
- **Frontends**: `transport/` → `cql3/` (CQL); `alternator/` (DynamoDB over HTTP); `api/` (REST, swagger-defined, used by nodetool).
- **Coordination**: `service/storage_proxy` (coordinator read/write path), `storage_service` (node lifecycle), topology coordinator. Cluster metadata (schema, topology, auth) is replicated through **Raft group0** (`raft/` library + `service/raft/`); see `docs/dev/raft-in-scylla.md` and `docs/dev/topology-over-raft.md`. `locator/` holds replication strategies and **tablets** (vs. vnodes in `dht/`); `gms/` gossip; `message/` RPC with wire types in `idl/*.idl.hh` compiled by `idl-compiler.py`.
- **Storage engine**: `replica/` (database, table, memtables, row cache) → `mutation/` → `readers/` → `sstables/` + commitlog (`db/`) + `compaction/`, `repair/`, `streaming/`. Reads are bounded by `reader_concurrency_semaphore`.
- **Layered features**: `cdc/`, materialized views, `index/`, `vector_search/`, `auth/`, `audit/`, `tracing/`, `tasks/`, `node_ops/`, `lang/` (UDFs), `tools/` (nodetool, `scylla sstable`), `types/`, `utils/`.
- **PITR** (this fork, `db/pitr/` on `ow/pitr/v1`; design in `docs-work/design-must.md`):
  - *Ship path:* `pitr_shipper` (config, states, seal timer, upload with retry and release) → `segment_upload` → `ledger_writer` / `ledger_entry` (format frozen; kind 1 entries, kind 2 reserved, kind 3 planned). It hooks into the commitlog's segment hold, close callback and clamp in `db/commitlog/`.
  - *Backup side:* the replay bound is captured together with the snapshot in `replica/table.cc` `snapshot_sstables()`.
  - *Restore path:* `t_parser` (T) → `chain_check` (dense chain of ledger entries, or a refusal) → `replay_reader` (strict replay of the shipped segments, counting split writes) → `t_filter` → `apply_to_target`. `refusal` holds the refusal reasons.
  - Not built yet: shipping wired to a real object-storage client, ledger writes from the shipper, and the `replay_to(T)` REST endpoint.
- Debugging: `scylla-gdb.py`, `docs/dev/debugging.md`. Other docs: `HACKING.md`, `docs/dev/building.md`, `docs/dev/testing.md`.

## Architecture constraints

> Decisions already made that a change must not silently override. `architect-check` compares
> structural changes against this list; `tl-intake` checks incoming requests against it.
> Confirmed by the owner on 2026-11-06. The owner edits this list.

**Concurrency model**
- Shared-nothing per shard: no shared mutable state across shards; cross-shard work goes through `smp::submit_to` / `sharded<>` / `foreign_ptr`.
- Never block the reactor: no blocking syscalls, `std::mutex`, `sleep`, or synchronous file I/O on a shard thread. Use `seastar::semaphore`/`gate`, not std primitives.
- Long-running loops must yield (`maybe_yield`); no large contiguous allocations on hot paths (`utils::chunked_vector`).
- Futures are never dropped; prefer coroutines, and watch capture lifetimes in continuations and coroutine lambdas.

**Compatibility**
- On-disk formats (sstables, commitlog, hints), the inter-node RPC protocol (`idl/`, `message/`), client protocols (CQL, Alternator, REST), and system-table schemas (`system`, `system_schema`, `system_distributed`) must stay readable across a rolling upgrade.
- Behaviour that changes any of them is gated behind a **cluster feature** (`gms` feature flag) until every node supports it. Schema and topology changes go through group0 / Raft, not ad-hoc gossip.
- Tablets vs. vnodes behaviour, and group0/topology state-machine changes, are structural.

**Dependencies**
- New third-party libraries or submodule bumps (seastar, abseil, fmt, c-ares, tools/*) are structural changes and need explicit approval.

**Explicitly rejected — do not relitigate**
- None recorded yet; the owner adds decisions here with a link to the discussion.

## Out of scope for agents

> Hard stops for autonomous work. Read freely and propose changes, but stop and hand to a human
> rather than landing a change here as part of a ticket — unless the ticket explicitly scopes it,
> and even then an agent never merges.

- On-disk format readers/writers: `sstables/`, commitlog (`db/commitlog/`), hints, and other persisted formats.
- Security: `auth/`, TLS, encryption at rest (`ent/`, `docs/dev/file_encryption.md`), `audit/`.
- Toolchain and CI: `tools/toolchain/`, `.github/workflows/`, `dist/`, `reloc/`, `install*.sh`, release scripts, `SCYLLA-VERSION-GEN`.

## Agent skills

- Issue tracker: GitHub Issues on `ofirwe86-weissberg/scylladb` — see `docs/agents/issue-tracker.md`. (Issues are enabled on the fork.)
- Triage labels: see `docs/agents/triage-labels.md`.

## SDLC workflow

Subagents in `.claude/agents/` (`product-manager`, `tester`, `implementer`, `architect-check`, `reviewer-qa`, `pr-reviewer`) take work from request to reviewed PR, orchestrated by `/tl-intake` (request → issue) and `/sdlc-workflow` (issue → PR). The session acts as team lead. Roles, models, the feature-work order and PR rules at the full level: `docs/agents/sdlc-workflow.md`.

### Team level per task

Every task issue carries one level label, `team:full` or `team:light`. `tl-intake` proposes it in the issue with a one-line reason, and the human confirms it along with the issue. `sdlc-workflow` reads the label before starting and asks if it's missing.

**`team:full`** applies if any of these is true:
- the change is structural (see "Architecture constraints"): a format, RPC, public API or config change, a feature flag, threading or sharding, or a dependency;
- it changes concurrency, lifetimes, shutdown or drain, cross-shard work, or code whose worst-case input could stall;
- a bug in it could give silently wrong data, for example restore correctness, tombstones, the replay bound or the chain checks;
- it touches an "Out of scope for agents" area that the ticket explicitly scopes;
- it is new user-visible behaviour that needs requirements, or its design isn't settled;
- it is expected to exceed ~300 product lines.

**`team:light`** is everything else: a confined change to a settled design, such as a refusal check, a parser, wiring of a reviewed design, tests, docs or tooling.

When in doubt, use full. Anyone (the team lead, `reviewer-qa`, `architect-check`) can raise a task to full when the work shows risk: a lifetime question, a possible stall, or an overrun of its time box. Lowering a level needs the human.

| | `team:full` | `team:light` |
|---|---|---|
| Requirements | `product-manager` first, for feature work and product decisions | `product-manager` only if user-visible text changes |
| Test plan | `tester` plan before code, for feature work | Tests written with the code |
| Review | `implementer` → `architect-check` if structural → `reviewer-qa` → PR → `pr-reviewer` | `implementer` → `reviewer-qa` → PR; the team lead checks the description's claims instead of `pr-reviewer` |
| PR size | 100-300 product lines, ceiling 500 | Up to ~400 product lines, ceiling 500 |
| Sequencing | Each PR reviewed before the next starts | May overlap; stacked, merged in batches |
| Test repeats | 5 of 5 while developing, 100 of 100 before merge | 5 of 5 |

Flow at the full level: intake → implement → architect-check *if structural* → reviewer-qa → PR → pr-reviewer. Rules that hold at both levels, together with all of "Development rules":
- **Nothing is merged automatically, and agents never merge.** The PR still gets human review.
- **One commit and one PR per task,** on its own branch, with the PR title starting with the task ID. Dependent PRs stack on the previous task's branch.
- **Every code-writing agent is launched with the Agent tool's `isolation: "worktree"`**, and checks out a real feature branch (`fix/<issue>-<slug>`) inside it before editing.
- **No piece runs longer than foreseen.** Each piece gets a stated time; at the limit it stops and reports: ship what passes, split the rest, and tell the human.
- **Everything an agent writes into GitHub is signed** — it opens with `**[claude-agent: <role>]**` on its own line, because `gh` posts under a human's account.
- **`product-manager` is a peer, not a report.** The team lead relays every issue it raises to the human without filtering, and only the human closes one.
- `.claude/hooks/guard-destructive-ops.sh` blocks merges, pushes to the default branch, force pushes, `git reset --hard`, `git clean -fdx`, and wiping the build tree. It is a speed bump, not a security boundary — branch protection is the control that holds.
- `.claude/hooks/check-cpp-format.sh` checks changed lines of edited C/C++ files against `.clang-format` and asks the agent to fix them. The hook runs on the host, where `git-clang-format` is absent, so it is a silent no-op; `clang-format` and `git-clang-format` live in the toolchain image, so format changed lines with `./tools/toolchain/dbuild clang-format -i --lines=<start>:<end> <file>` yourself rather than relying on the hook.
