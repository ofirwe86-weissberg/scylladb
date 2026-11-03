---
name: sdlc-workflow
description: Use this skill whenever you are asked to take a GitHub issue (or an in-chat feature request that should become one) through to an implemented, reviewed pull request using the implementer, reviewer-qa, architect-check and pr-reviewer subagents. Triggers include being @mentioned on a GitHub issue/PR, being assigned an issue, or a person asking to "build", "implement", "fix" or "pick up" a ticket. Do not use this for quick one-off edits with no issue behind them, or for pure Q&A about the codebase.
---

# SDLC workflow

This skill defines how the subagents in `.claude/agents/` work together on one ticket. You (the orchestrating session) act as the lead: you don't write the implementation yourself — you delegate, sequence, and make the final call on whether to open the PR.

## Process

1. **Scope the ticket.** Read the GitHub issue (`gh issue view <n> --comments`). If acceptance criteria are missing or contradictory, either ask a clarifying question (if a human is available) or proceed with a best-effort interpretation and state it clearly in the PR description — never invent scope silently. If the issue touches a surface listed under "Out of scope for agents" in `CLAUDE.md`, stop and say so unless the issue explicitly scopes it.

2. **Delegate to `implementer`.** Hand it the issue number and point it at `CLAUDE.md`. Wait for its summary and pushed branch.

   **Launch every code-writing agent with the Agent tool's `isolation: "worktree"`** — always, not only when several run in parallel. Two agents editing one checkout race each other's builds, checkouts and commits, and one agent's recovery can silently commit the other's work.

   **Tell it in the brief to check out a real feature branch** (e.g. `fix/<issue>-<slug>`) inside its worktree before editing. The auto-named `worktree-agent-*` branch is never the PR branch.

   **C/C++ build cost.** Each worktree has its own build tree, so the first build in a fresh worktree is a full one. Keep parallelism low (see `CLAUDE.md` — "Max parallel agents"), tell the implementer to build the narrowest target in the fastest iteration mode, and make sure a compiler cache (ccache/sccache) is configured if the project supports it.

3. **Check whether the change is structural**, using the implementer's flag as a starting signal, not the final word — look at the diff yourself. Structural means: new third-party dependency or submodule bump, build-system change, on-disk/wire/protocol/schema format change, public API/ABI or config change, new feature flag / upgrade-path requirement, or a change to the threading/sharding model. If yes, delegate to `architect-check`. If it comes back with a conflict, do not proceed to review — send it back to `implementer` with the conflict and re-run this step after the fix.

4. **Delegate to `reviewer-qa`.** Give it the issue number and the branch/diff — **not** the implementer's summary, so its review stays independent. It runs from the implementer's worktree (or its own checkout of the branch) so it can build and test.
   - **Request changes**: send the concrete blocking items back to `implementer`, get an updated branch, and re-run `reviewer-qa`. Keep iterating while the fixes are mechanical; escalate to a human when the disagreement is about design, or after the third round without convergence.
   - **Approve** / **Approve with nits**: proceed.

5. **Verify the pushed head, then open the PR.** Before opening, confirm the worktree's `HEAD` equals `origin/<branch>` — resumed agents sometimes leave fixes committed but unpushed, or on a detached HEAD. Never merge. The PR description includes: what the issue asked for, what was built, which build modes/targets and tests were actually run, scope deliberately excluded, the QA verdict and any non-blocking nits, the architect-check result if it ran, and `Fixes #<n>` (or the project's equivalent).

6. **Delegate to `pr-reviewer`** — a cold read of the PR now that it is open. It must be a *different agent instance* from the implementer and from the `reviewer-qa` that negotiated fixes, because by then `reviewer-qa` is reviewing a solution it co-authored.

   Give it the PR number and the issue number and nothing else — no summaries, no prior verdicts. One `pr-reviewer` instance per PR. It posts its verdict as a GitHub review.
   - **Request changes**: send the items back to `implementer`, push the fix, re-run `pr-reviewer`.
   - **Approve** / **Approve with nits**: the PR is ready for its human reviewer. Say so, with the review URL.

   If it finds the PR description overstates what was done, fix the description — that is a real defect.

7. **If anything surfaces real ambiguity** (conflicting requirements, an architecture conflict with no clear resolution, QA and implementer disagreeing on design) — stop and post it as a comment on the issue rather than picking a side silently.

## Which model each agent runs on

Set in each agent's frontmatter as an exact model ID (`claude-opus-5`, `claude-opus-5-5`), not the bare `sonnet`/`opus` alias, so a model change is a deliberate, reviewed edit rather than a silent one. Do not override it per call without a reason you state in the same message.

| Band | Model | Agents |
|---|---|---|
| **Implementation** | Opus 5 | `implementer` |
| **Verification** | Opus 5.5 | `reviewer-qa`, `pr-reviewer` |
| **Design / architecture** | Opus 5.5 | `architect-check` |

Reviewers run on Opus deliberately: in C/C++ the expensive defects — lifetime bugs, UB, races, compatibility breaks — need cross-file reasoning, and a missed one costs far more than the review.

## Signing what an agent writes to GitHub

Everything an agent writes into GitHub — an issue, an issue comment, a PR description, a PR comment, a PR review — **must open with a signature line naming the agent that wrote it**, on its own line:

```
**[claude-agent: pr-reviewer]**
```

Use the role name: `implementer`, `reviewer-qa`, `architect-check`, `pr-reviewer`, `tl-intake`, or `sdlc-workflow` for what the orchestrating session writes itself (PR descriptions, its own issue comments).

`gh` authenticates as a human account, so without the line every one of these lands under that person's name and avatar, and nobody can later tell whose words were whose — or which agent's verdict a finding came from.

- Never sign as an agent that did not write the text. If relaying a subagent's findings, sign as yourself and say whose findings they are.
- Never omit the line because the content is "obviously" machine-written.

## What this skill does not do

It does not decide whether a ticket is ready to be worked — that's a human or `tl-intake`. It does not release or deploy. It does not replace human PR review; it produces a PR that has already been through two independent adversarial passes, so the human's job is faster, not unnecessary. **Nothing here merges anything.**
