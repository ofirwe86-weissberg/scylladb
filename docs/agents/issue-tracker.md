# Issue Tracker

Issues for this repo live in **GitHub Issues** on `ofirwe86-weissberg/scylladb`.

## CLI

Use the `gh` CLI for all issue operations — always name the repo explicitly if this checkout has
more than one remote (e.g. a fork with `upstream`), so issues never land on the upstream project:

```sh
gh issue list   -R ofirwe86-weissberg/scylladb --label ready-for-agent
gh issue view   -R ofirwe86-weissberg/scylladb <number> --comments
gh issue create -R ofirwe86-weissberg/scylladb --title "..." --body-file body.md --label needs-triage
gh issue edit   -R ofirwe86-weissberg/scylladb <number> --add-label ready-for-agent --remove-label needs-triage
gh pr create    -R ofirwe86-weissberg/scylladb --base master --head <branch> --body-file pr.md
```

## Conventions

- Every new issue gets `needs-triage` on creation unless the requester says it is ready.
- A human moves an issue to `ready-for-agent`; that label is the signal an agent may pick it up.
- PRs reference their issue with `Fixes #<n>`.
- Agent-written issues, comments, PR descriptions and reviews open with `**[claude-agent: <role>]**`.
