---
name: review-loop
description: >-
  Multi-agent review/refine loop. Use after a change or milestone, or when asked to review work:
  dispatch a fresh-context full-spectrum reviewer plus a dissimilar correctness reviewer,
  consolidate and fix, add a regression test for every defect, and repeat until a round is clean.
---

# Adversarial review/refine loop

After a change or milestone, or when prompted, dispatch fresh-context review agents at
MAXIMUM THINKING EFFORT, then consolidate, fix, and repeat.
The goal is adversarial, diverse, independent coverage.

The prompts given to the agents shall be extremely terse, at most a few sentences.
Giving excessive detail may constrain their thinking causing the tunnel vision syndrome.
They must be given the opportunity to look at the work without bias or prejudice.

## The reviewer pair

Run two reviewers in parallel per round:

- An *ultrathink* Claude agent with the FULL-SPECTRUM remit, in priority order: functional CORRECTNESS and
  ROBUSTNESS first, then SIMPLIFICATION opportunities, ARCHITECTURAL CLEANLINESS and CODE QUALITY,
  and POLICY/STYLE compliance with the project's own docs.

- Codex running the *most advanced model* in *ultra* effort focusing on CORRECTNESS only, to maximize perspective
  diversity and minimize blind spots.

## Reviewers are read-only

Review agents must not modify the worktree or run mutating commands. If one needs a mutable environment,
it copies the worktree elsewhere.

## Reviewers do not re-run the project test suites

The tests normally should already be green when the review loop is invoked; re-running them
duplicates work and, for the broad sessions, wastes minutes of compute per round. State this in the
reviewer prompts. Reviewer effort goes instead into adversarial counterexamples for behaviors the
existing tests do NOT cover, executed in a scratch clone. Probes must run under the repo's own test
interpreter (e.g. `.nox/tests/bin/python`, which mutates nothing) rather than whatever is on PATH:
a version-skewed interpreter or dependency set can produce findings that do not apply to the project
or miss ones that do. Reproducing their own findings before reporting remains mandatory.

## Consolidate and act

When all reviewers return, merge their findings, discard the noise, and fix what is real.
For every correctness defect, add a regression test verified to fail before the fix and pass after.

## When to stop

A round is clean when the reviewers surface only trivial feedback or none; the first clean round ends the loop.
Do not chase literal zero feedback: with no real issues left, agents degrade into nitpicking,
so a round is clean as soon as significant findings cease.

## Operational notes

High-effort agents can go silent for a long time — set generous timeouts and do not assume a quiet agent is stuck.
Have agents background long-running commands (tests especially); blocking on a foreground command
is a common cause of stream-idle timeouts.

Some headless agents hang waiting on stdin (like Codex) — redirect from `/dev/null`.

Retry agents that fail on a transient or connection error until they succeed.
If an agent gets stuck or hits a security guardrail, try resuming it first instead of restarting its work from scratch.
