# Working Agreement

## Agree before coding

- Do not modify code, config, or files until we have discussed the change and I have agreed to it.
- For any non-trivial change, first explain the plan and the tradeoffs, then wait for my go-ahead.
- Analysis, reading code, searching, and answering questions do not need prior approval.

## Executing an agreed step

- Once I approve a step, carry it out fully without pausing: make the edits, compile, run the
  relevant tests, and report back.
- After finishing an agreed step, stop and confirm the next step before touching code again.
- Only interrupt an approved step to ask if there is a genuine fork with materially different
  outcomes, or if a build or test fails in a way that needs my input.

## Commits and PRs

- Commit only when I ask. Group related changes into one commit per logical step.
- Prefer staging specific files over `git add .`.
- Do not push or open PRs unless I ask.
