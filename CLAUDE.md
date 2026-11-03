# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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

# Format changed lines only
git clang-format master
```

Build modes: `debug`, `dev`, `release`, `sanitize`. Agents use `dev` by default. If `test.py` hits cgroup-metrics permission errors, add `--no-gather-metrics`. Logs land in `testlog/<mode>/`. A full build is long (hours on modest hardware, ~2–3 GB RAM per compile/link thread), so build the narrowest target. `configure.py --compiler-cache` defaults to `auto` and picks up the image's `sccache` (`build.ninja`: `cxx_launcher = /usr/bin/sccache`), which is what makes a rebuild in a fresh worktree survivable. Measured here at `-j6`: an incremental `build/dev/scylla` relink took 11m32s.

**Max parallel agents: 2.** Every agent works in its own worktree with its own build tree, so N agents means N full builds competing for cores and RAM.

Test suites: `boost`/`unit`/`raft` (C++), `cqlpy` and `alternator` (single-node Python), `cluster` (multi-node, `test/pylib` manager), `cql` (approval tests; review any `.reject` diff before overwriting `.result`), `rest_api`, `nodetool`, `scylla_gdb`; also `vector_search`, `ldap`, `tools`, `perf`, `dist_test`, `manual` and `pylib_test` (`ls test/` for the full set). cqlpy tests should also pass on Cassandra.

## Conventions

- Formatting: root `.clang-format`; apply to changed lines only (`git clang-format`), never reformat untouched code. Four spaces, braces on all scopes, `snake_case`, `_member` prefix. Follow the Seastar coding style and the surrounding file.
- Error handling: exceptions by default (futures propagate them); in coroutines prefer `co_return coroutine::exception(...)`; on the data path avoid exceptions (`std::expected`/`boost::outcome`). Invariants: `throwing_assert()` (`utils/assert.hh`), `SCYLLA_ASSERT` only where abort is unavoidable, `on_internal_error()` for should-never-happen. Never `new`/`delete`, `std::mutex`, blocking calls, or ad-hoc macros.
- Memory/ids: `std::unique_ptr` by default, `lw_shared_ptr` within a shard, `foreign_ptr` across shards; new UUID-style ids use `utils::tagged_uuid`; `utils::chunked_vector` for allocations >128KB.
- Logging: `seastar::logger` per subsystem, structured, never log credentials/PII.
- Commit messages: `module: imperative summary` (e.g. `sstables: close file-descriptor on error`; `tree:` for whole-tree changes); body explains why; reference issues (`Fixes #N`). Signed-off-by is not consistently used in recent history. Each commit must compile and pass tests (bisectable). See `docs/dev/review-checklist.md`.
- New files: SPDX header with `LicenseRef-ScyllaDB-Source-Available-1.1`, current year for new files only.
- Tests: every bug fix comes with a test that fails before and passes after, and cites the GitHub/JIRA issue. New tests must be stable (`--repeat 100 --max-failures 1`), fast, deterministic, and free of sleeps; reduce iterations in debug mode.
- Question requests rather than implementing blindly; flag trade-offs.

## Architecture (big picture)

- **Seastar** (`seastar/`): sharded runtime; most code runs on one shard without locks; cross-shard work via `smp::submit_to`, `sharded<>`, `foreign_ptr`. `main.cc` wires every `sharded<>` service and its startup/shutdown order.
- **Frontends**: `transport/` → `cql3/` (CQL); `alternator/` (DynamoDB over HTTP); `api/` (REST, swagger-defined, used by nodetool).
- **Coordination**: `service/storage_proxy` (coordinator read/write path), `storage_service` (node lifecycle), topology coordinator. Cluster metadata (schema, topology, auth) is replicated through **Raft group0** (`raft/` library + `service/raft/`); see `docs/dev/raft-in-scylla.md` and `docs/dev/topology-over-raft.md`. `locator/` holds replication strategies and **tablets** (vs. vnodes in `dht/`); `gms/` gossip; `message/` RPC with wire types in `idl/*.idl.hh` compiled by `idl-compiler.py`.
- **Storage engine**: `replica/` (database, table, memtables, row cache) → `mutation/` → `readers/` → `sstables/` + commitlog (`db/`) + `compaction/`, `repair/`, `streaming/`. Reads are bounded by `reader_concurrency_semaphore`.
- **Layered features**: `cdc/`, materialized views, `index/`, `vector_search/`, `auth/`, `audit/`, `tracing/`, `tasks/`, `node_ops/`, `lang/` (UDFs), `tools/` (nodetool, `scylla sstable`), `types/`, `utils/`.
- Debugging: `scylla-gdb.py`, `docs/dev/debugging.md`. Other docs: `HACKING.md`, `docs/dev/building.md`, `docs/dev/testing.md`.

## Architecture constraints

> Decisions already made that a change must not silently override. `architect-check` compares
> structural changes against this list; `tl-intake` checks incoming requests against it.
> **Proposed by the installer from the codebase and docs — the owner must confirm or edit.**

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

A team of subagents in `.claude/agents/` takes work from request to reviewed PR. Skills in `.claude/skills/` orchestrate them. Use `/tl-intake` to turn a request into an issue and `/sdlc-workflow` to take an issue to a PR.

| Piece | Model | Role |
|---|---|---|
| `tl-intake` (skill) | session model | Conversation → well-scoped GitHub issues. Stops at the issue. |
| `sdlc-workflow` (skill) | session model | Issue → PR. Delegates, sequences, decides whether to open the PR. |
| `implementer` (agent) | Opus 5 | Builds one issue into tested code. Flags structural changes. |
| `architect-check` (agent) | Opus 5.5 | Runs only on structural changes, against "Architecture constraints". |
| `reviewer-qa` (agent) | Opus 5.5 | Adversarial review of the diff before a human sees it. Builds and runs tests. |
| `pr-reviewer` (agent) | Opus 5.5 | Cold read of the opened PR — diff, commit message, and whether the description's claims are true. Posts a GitHub review. Always a different instance from the two above. |

Flow: intake → implement → architect-check *if structural* → reviewer-qa → PR → pr-reviewer. **Nothing is merged automatically**; the PR still gets human review.

- **Every code-writing agent is launched with the Agent tool's `isolation: "worktree"`**, and checks out a real feature branch (`fix/<issue>-<slug>`) inside it before editing.
- **Everything an agent writes into GitHub is signed** — it opens with `**[claude-agent: <role>]**` on its own line, because `gh` posts under a human's account.
- `.claude/hooks/guard-destructive-ops.sh` blocks merges, pushes to the default branch, force pushes, `git reset --hard`, `git clean -fdx`, and wiping the build tree. It is a speed bump, not a security boundary — branch protection is the control that holds.
- `.claude/hooks/check-cpp-format.sh` checks changed lines of edited C/C++ files against `.clang-format` and asks the agent to fix them. The hook runs on the host, where `git-clang-format` is absent, so it is a silent no-op; `clang-format` and `git-clang-format` live in the toolchain image, so format changed lines with `./tools/toolchain/dbuild git clang-format master` yourself rather than relying on the hook.
