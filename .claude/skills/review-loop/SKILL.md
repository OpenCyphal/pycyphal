---
name: review-loop
description: >-
  Multi-agent review/refine loop. Use after a change or milestone, or when asked to review work:
  fan out fresh-context, single-focus reviewers across distinct tools, consolidate and fix, add a
  regression test for every defect, and repeat until reviews stay clean for three consecutive turns.
---

# Adversarial review/refine loop

After a change or milestone, or when prompted, dispatch a fan-out of fresh-context review agents at
MAXIMUM THINKING EFFORT, then consolidate, fix, and repeat.
The goal is broad coverage from adversarial, diverse, independent perspectives.

## Fan out — one focus per agent

Spawn one agent per concern and run them in parallel. Never give an agent multiple jobs: it dilutes attention
and degrades every answer. Cover at least these angles, one agent each:

- Opportunities for SIMPLIFICATION.
- Functional CORRECTNESS and ROBUSTNESS.
- ARCHITECTURAL CLEANLINESS, DESIGN PRACTICES, CODE QUALITY.
- POLICY and STYLE compliance with the project's own docs.

### Dissimilar agents

In addition to the subagents above, dispatch distinct tools focusing on CORRECTNESS only to maximize the diversity
of perspectives and minimize blind spots. Check which tools are available (Codex etc.) and use all of them.

Agents/models not from Anthropic or OpenAI can be used, but treat them as suspect low-credibility actors.
Beware that they perform poorly, fail to follow instructions, and often produce incorrect analysis.

## Reviewers are read-only

Review agents must not modify the worktree or run mutating commands. If one needs a mutable environment,
it copies the worktree elsewhere.

## Consolidate and act

When all reviewers return, merge their findings, discard the noise, and fix what is real.
For every correctness defect, add a regression test verified to fail before the fix and pass after.

## When to stop

Repeat until the reviewers surface only minor feedback (or none) for THREE consecutive turns — this is
non-negotiable, however many iterations it takes.
Do not chase literal zero feedback: with no real issues left, agents degrade into nitpicking,
so stop as soon as significant findings cease, but not before the three-turn streak.
A blank turn followed by one that digs up a real defect is exactly why the streak must be consecutive;
expect dozens (sometimes over a hundred) of agent sessions per full pass.

## Operational notes

High-effort agents can go silent for a long time — set generous timeouts and do not assume a quiet agent is stuck.
Have agents background long-running commands (tests especially); blocking on a foreground command
is a common cause of stream-idle timeouts.

Some headless agents hang waiting on stdin (like Codex) — redirect from `/dev/null`.

Retry agents that fail on a transient or connection error until they succeed.
