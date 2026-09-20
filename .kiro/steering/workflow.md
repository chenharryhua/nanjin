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

## Wire format

- Always notify me when a change impacts wire format, before finalizing or committing it. This
  includes: JSON object keys (e.g. `derives Codec.AsObject` field renames), Avro record field
  names, serialized enum/case names, CloudWatch dimension or metric names, OAuth/token fields, and
  any other externally-observed or persisted key or value. Type/identifier renames that do not
  change the serialized bytes are wire-safe and do not need a heads-up.
- When flagging, state exactly what the old and new serialized form is, and where it is read or
  persisted, so I can decide whether the break is acceptable.

## Commits and PRs

- Before every commit, run `sbt scalafixAll scalafmtAll` and stage any resulting changes so the
  commit is already linted and formatted.
- Commit only when I ask. Group related changes into one commit per logical step.
- Prefer staging specific files over `git add .`.
- Do not push or open PRs unless I ask.
