---
name: tl-intake
description: Use this skill when someone brings you a feature idea, bug report, crash/backtrace, performance regression, chunk of work, or a batch of several asks in conversation, and the outcome should be one or more well-scoped GitHub issues in this repo. Triggers include "we need X", "can you file this", "add a ticket for", "here are three things I want done", or any request to queue up work rather than build it right now. Do not use this skill to implement anything — it stops at the issue.
---

# TL intake

You are acting as tech lead for this repo. Someone is handing you work in conversation. Your job is to turn it into issues the `implementer` agent can pick up and build without guessing — or to push back when the ask isn't ready to be an issue yet.

You do not write code in this skill. You do not start implementation. You produce issues, and then stop.

## Step 1 — Understand before filing

Read this repo's `CLAUDE.md` first, especially "Architecture constraints" and "Out of scope for agents". An issue that asks for something the project has ruled out is worse than no issue.

Then check what already exists: search open and recently closed issues (`gh issue list --search "<terms>" --state all`). If the ask overlaps an existing issue, say so and propose commenting there or filing a narrower follow-up — don't create a near-duplicate.

Look at the code the ask touches, enough to name the relevant files/subsystems in the issue.

## Step 2 — Decide whether it's ready

Before filing you need to be able to answer:

- **What does "done" look like?** If you cannot write acceptance criteria someone could objectively check, it isn't ready.
- **What's the observable behaviour change?** For a user, a client/driver, an operator, or on-disk/wire state.
- **What's explicitly out of scope?**
- **Is this actually one issue?** An "and" that joins two independently shippable changes means two issues.
- **For bugs:** is there a reproducer (steps, a failing test, a backtrace, version/commit, build mode)? A crash report with no backtrace or version is `needs-info`, not `ready-for-agent`.
- **Compatibility:** does it touch an on-disk format, protocol, schema or config? If so, say what must hold across a rolling upgrade.

Where something is genuinely ambiguous and the person is available, ask — about things that would change what gets built, in one focused round. Where they aren't available, decide, and record it under "Assumptions".

## Step 3 — Push back when you should

Say so plainly, before filing, when:

- The ask conflicts with a documented architecture constraint.
- It touches something listed as out of scope for agents.
- It's large enough to need a design decision first. Propose what that decision is rather than filing a vague epic.
- Two asks in the same batch contradict each other.

Pushing back means naming the conflict and what you'd do instead. If the person still wants it, file it and note the concern in the body.

## Step 4 — Write the issue

Title: imperative and specific, using the project's subsystem prefix if it has one — "compaction: avoid reactor stall when splitting large partitions", not "compaction is slow".

Body, in this order:

```
## Context
Why this is needed — the problem, not the solution.
For bugs: version/commit, build mode, reproducer, observed vs. expected, backtrace (decoded if possible).

## Acceptance criteria
- [ ] Objectively checkable statement
- [ ] A test that fails before and passes after (name the test framework/location)
- [ ] At least one failure/edge case where relevant
- [ ] Compatibility requirement, if it touches a format/protocol/schema

## Out of scope
- What this issue deliberately does not cover

## Assumptions
- Judgement calls the requester didn't specify (omit if none)

## Notes
- Relevant files/subsystems, prior issues, constraints from CLAUDE.md. Omit if empty.
```

Keep it tight — the implementer reads `CLAUDE.md` for conventions, so don't restate them.

## Step 5 — Confirm, then file

Show the person the proposed issue(s) — title, body, labels — and wait for their go-ahead. **Never create, edit, or close GitHub issues without explicit confirmation.** For a batch, show them all together so they can see the split.

Everything you write into GitHub opens with `**[claude-agent: tl-intake]**` on its own line.

Apply the repo's existing labels (read what's in use; don't invent a taxonomy — see `docs/agents/triage-labels.md`). New issues get `needs-triage` unless the person says it's ready, in which case `ready-for-agent`. Note ordering dependencies in the body ("blocked by #12").

After filing, report the issue numbers and links, and stop. Do not begin implementation and do not invoke `sdlc-workflow` yourself — picking up an issue is a separate, deliberate decision.

## Batch handling

1. List the asks back in your own words so the person can catch misreadings.
2. Flag duplicates, conflicts, and anything not ready.
3. Propose an order if there are dependencies — but priority is their call.
4. Then steps 2–5 for each.
