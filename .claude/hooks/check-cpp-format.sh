#!/usr/bin/env bash
# PostToolUse hook (Edit|Write|MultiEdit): after an agent edits a C/C++ file,
# check the lines it changed against the repo's .clang-format and tell the
# agent if they are misformatted.
#
# Silent no-op when:
#   - the edited file isn't C/C++
#   - the repo has no .clang-format
#   - clang-format / git-clang-format isn't installed
#
# Only CHANGED lines are checked (git clang-format --diff), so the agent is
# never pushed into reformatting untouched legacy code — that churn makes C++
# diffs unreviewable.
#
# Exit 2 = feed stderr back to Claude (it already made the edit; this asks it
# to fix formatting). Exit 0 = fine.
# Requires: jq

set -uo pipefail
command -v jq >/dev/null 2>&1 || exit 0

input="$(cat)"
file="$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')"
[ -n "$file" ] || exit 0

case "$file" in
  *.c|*.cc|*.cpp|*.cxx|*.h|*.hh|*.hpp|*.hxx|*.ipp|*.inl) ;;
  *) exit 0 ;;
esac

dir="$(dirname "$file")"
root="$(git -C "$dir" rev-parse --show-toplevel 2>/dev/null)" || exit 0
[ -f "$root/.clang-format" ] || exit 0
command -v git-clang-format >/dev/null 2>&1 || exit 0

# Diff of formatting fixes for this file's changed lines, relative to HEAD.
out="$(cd "$root" && git clang-format --diff HEAD -- "$file" 2>/dev/null)"
case "$out" in
  ""|*"no modified files to format"*|*"clang-format did not modify any files"*) exit 0 ;;
esac

{
  printf 'clang-format: lines you changed in %s do not match .clang-format.\n' "$file"
  printf 'Apply the fix to your changed lines only (e.g. `git clang-format HEAD -- %s`), do not reformat the rest of the file.\n\n' "$file"
  printf '%s\n' "$out" | head -60
} >&2
exit 2
