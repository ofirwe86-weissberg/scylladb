# SDLC workflow

Moved from `CLAUDE.md`, which keeps a summary. This file describes the `team:full` level. A `team:light` task follows the lighter column of "Team level per task" in `CLAUDE.md`.

A team of subagents in `.claude/agents/` takes work from request to reviewed PR. Skills in `.claude/skills/` orchestrate them. Use `/tl-intake` to turn a request into an issue and `/sdlc-workflow` to take an issue to a PR.

| Piece | Model | Role |
|---|---|---|
| `product-manager` (agent) | Opus 5.5 | Peer of the team lead, not a report. Owns the requirements (the what; the dev team owns the how). Works from documents, not code, and raises issues in the product issue register. |
| `tester` (agent) | Opus 5.5 | QA. Checks requirements are testable, writes the full end-to-end test plan from them, and implements it as black-box tests. Plan reviewed by `product-manager`, the team lead and `code-reviewer`. |
| `tl-intake` (skill) | session model | Conversation → well-scoped GitHub issues. Stops at the issue. |
| `sdlc-workflow` (skill) | session model | Issue → PR. Delegates, sequences, decides whether to open the PR. |
| `implementer` (agent) | Opus 5 | Builds one issue into tested code. Flags structural changes. |
| `architect-check` (agent) | Opus 5.5 | Runs on structural changes, against "Architecture constraints". Also reviews integration test coverage across units, from documents only. |
| `code-reviewer` (agent) | Opus 5.5 | Adversarial review of the diff before a human sees it. Builds and runs tests. |
| `pr-reviewer` (agent) | Opus 5.5 | Cold read of the opened PR — diff, commit message, and whether the description's claims are true. Posts a GitHub review. Always a different instance from the two above. |

Flow: intake → implement → architect-check *if structural* → code-reviewer → PR → pr-reviewer. **Nothing is merged automatically**; the PR still gets human review.

The session acts as team lead. For feature work, run `product-manager` before requirements go to the tester and before bringing a product decision to the human. It doesn't report to the team lead: it writes its issues to the register directly, the team lead relays every new issue to the human without filtering, and only the human closes one. `product-manager` reviews documents, including the test plan, and doesn't review the implementation.

For feature work, the order is: requirements (`product-manager`), then a full test plan based on them (`tester`, reviewed by `product-manager`, the team lead and `code-reviewer`), and only then code. The test plan is QA's, and QA implements it as black-box, end-to-end tests against the real system (`tester`, reviewed by `code-reviewer` and `pr-reviewer` like other code). Unit and integration tests are the developer's work, written with the code, and based on mocking rather than end to end. The design lists the units, their connections, and the integration tests for each. `architect-check` reviews that list from the documents, not the code, to confirm every connection is tested and the design holds together.

- **Models:** development always uses Opus; Sonnet didn't hold up there. QA planning uses Opus. Implementing tests from a detailed plan uses Sonnet at `team:light` and Opus at `team:full`, and reviewers stay on Opus.
- **Start a fresh agent for each PR,** since long-running agents use far more of the session limit. For risky tasks, each PR is reviewed before the next starts; simple tasks may overlap. Change headers once and early, to limit rebuilds. The task's detail is in its issue. A fresh agent can reuse a built worktree.
- **Keep commits and reviews small: aim for 100-300 product lines per PR,** tests not counted, with 500 as a ceiling. For scale, half the repo's recent PRs are under 47 product lines, and 90% under 543. A larger task is split into smaller self-contained tasks, each with its own commit and PR.
- **No piece runs longer than foreseen.** Each piece gets a stated time when it starts, from its task's size, with one behaviour per piece. At the limit it stops and reports: ship what passes, split the rest, and tell the human.
- **One commit and one PR per task.** Each task issue gets its own commit on its own branch and its own PR, opened for review. Agents never merge. Dependent PRs stack on the previous task's branch, so each diff shows only its own task. Each PR title starts with the task ID, and its description states the task, its requirements, what changed, tests run with results, what it depends on, and what's not done.
- **Every code-writing agent is launched with the Agent tool's `isolation: "worktree"`**, and checks out a real feature branch (`fix/<issue>-<slug>`) inside it before editing.
- **Everything an agent writes into GitHub is signed** — it opens with `**[claude-agent: <role>]**` on its own line, because `gh` posts under a human's account.
- `.claude/hooks/guard-destructive-ops.sh` blocks merges, pushes to the default branch, force pushes, `git reset --hard`, `git clean -fdx`, and wiping the build tree. It is a speed bump, not a security boundary — branch protection is the control that holds.
- `.claude/hooks/check-cpp-format.sh` checks changed lines of edited C/C++ files against `.clang-format` and asks the agent to fix them. The hook runs on the host, where `git-clang-format` is absent, so it is a silent no-op; `clang-format` and `git-clang-format` live in the toolchain image, so format changed lines with `./tools/toolchain/dbuild clang-format -i --lines=<start>:<end> <file>` yourself rather than relying on the hook.
