#!/usr/bin/env bash
# PreToolUse hook (Bash): blocks destructive and workflow-breaking commands.
#
# This is a SPEED BUMP, NOT A SECURITY BOUNDARY. It pattern-matches command
# strings, and command strings can be written a hundred ways. The controls that
# actually hold are branch protection on GitHub and the permissions of the
# account `gh` is logged in as.
#
# Exit 2 = block (stderr is shown to Claude as the reason). Exit 0 = allow.
# Requires: jq

set -uo pipefail

command -v jq >/dev/null 2>&1 || exit 0   # fail open if jq is missing; INSTALL.md says to install it

input="$(cat)"

tool_name="$(printf '%s' "$input" | jq -r '.tool_name // empty')"
[ "$tool_name" = "Bash" ] || exit 0

cmd="$(printf '%s' "$input" | jq -r '.tool_input.command // empty')"
[ -n "$cmd" ] || exit 0

block() {
  printf 'BLOCKED by guard-destructive-ops: %s\n\n' "$1" >&2
  printf 'Agents deliver work as a feature branch + PR; a human merges.\n' >&2
  printf 'If this really needs to happen, ask the user to run it themselves (they can type `! <command>`).\n' >&2
  exit 2
}

# --- Merging / pushing to the default branch --------------------------------
# Nothing in the SDLC workflow merges. Humans merge.
case "$cmd" in
  *"gh pr merge"*)
    block "gh pr merge (agents never merge; human review is the gate)" ;;
esac
if printf '%s' "$cmd" | grep -Eq 'git[[:space:]]+push([[:space:]].*)?[[:space:]](origin[[:space:]]+)?(HEAD:)?(refs/heads/)?(main|master|next|branch-[0-9][^[:space:]]*)([[:space:]]|$)'; then
  block "push directly to the default/release branch (push a feature branch and open a PR)"
fi

# --- Git footguns ------------------------------------------------------------
case "$cmd" in
  *"git push --force"*|*"git push -f "*|*"git push -f"|*"--force-with-lease"*)
    block "force push" ;;
  *"git reset --hard"*)
    block "git reset --hard (discards uncommitted work)" ;;
  *"git clean -"*[xX]*|*"git clean -"*d*f*|*"git clean -"*f*d*)
    block "git clean -fd/-x (deletes untracked files, including build trees and local config)" ;;
  *"git checkout -- ."*|*"git restore ."*|*"git restore --staged --worktree ."*)
    block "discarding all working-tree changes" ;;
  *"git branch -D"*|*"git push origin --delete"*|*"git push --delete"*)
    block "deleting a branch" ;;
  *"git submodule deinit"*)
    block "git submodule deinit" ;;
esac

# --- Filesystem footguns -----------------------------------------------------
if printf '%s' "$cmd" | grep -Eq 'rm[[:space:]]+(-[a-zA-Z]*[[:space:]]+)*(/|~|\$HOME|\.|\*)([[:space:]]|/?$)'; then
  block "rm against /, ~, . or * (too broad)"
fi
# Wiping a whole C/C++ build tree costs hours of rebuild for every worktree.
# Clean a single target via the build system instead.
if printf '%s' "$cmd" | grep -Eq 'rm[[:space:]]+-[a-zA-Z]*[rR][a-zA-Z]*[[:space:]]+(\./)?(build|out|cmake-build[^[:space:]]*)/?([[:space:]]|$)'; then
  block "recursive rm of the whole build tree (rebuild cost is hours; clean the specific target instead, or ask the user)"
fi

# --- GitHub mutations outside the workflow ----------------------------------
case "$cmd" in
  *"gh repo delete"*|*"gh repo edit"*|*"gh release delete"*|*"gh release create"*)
    block "repository / release administration" ;;
  *"gh issue delete"*|*"gh issue transfer"*)
    block "deleting or transferring issues" ;;
esac

# --- Credential fishing ------------------------------------------------------
case "$cmd" in
  *".ssh/id_"*|*"gh auth token"*|*".config/gh/hosts.yml"*|*".aws/credentials"*)
    block "reading raw credentials" ;;
esac

# --- Project-specific additions go below this line --------------------------
# e.g. block wiping a local data directory used by integration tests:
# if printf '%s' "$cmd" | grep -Eq 'rm[[:space:]]+-[a-zA-Z]*r.*/var/lib/scylla'; then
#   block "recursive rm of a local data directory"
# fi

exit 0
