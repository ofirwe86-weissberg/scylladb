# Triage Labels

| Role | Label | Meaning |
|---|---|---|
| Needs evaluation | `needs-triage` | Maintainer needs to assess the issue |
| Waiting on reporter | `needs-info` | Blocked on more information (repro, backtrace, version) |
| Agent ready | `ready-for-agent` | Fully specified; an agent can pick it up with no human context |
| Human only | `ready-for-human` | Needs human judgement or implementation |
| Won't fix | `wontfix` | Will not be actioned |

Create the ones that don't exist yet (`gh label list` first — reuse existing labels if the repo
already has an equivalent taxonomy, and update this file to match):

```sh
gh label create needs-triage    --color "e4e669" --description "Needs maintainer evaluation"
gh label create needs-info      --color "0075ca" --description "Waiting on reporter"
gh label create ready-for-agent --color "008672" --description "Fully specified; agent can pick up"
gh label create ready-for-human --color "d93f0b" --description "Needs human implementation"
# wontfix already exists on this fork - reused
```
