# Batch execution

The `batch` package runs a group of named jobs under a shared metric scope, records per-job
timing and outcomes, and produces a single aggregate `BatchResult`. This document describes how
the pieces fit together.

## Entry points

`Batch[F]` is the façade. From it you pick one of three execution shapes, then choose how failures
are handled.

```mermaid
flowchart TD
    Batch["Batch[F]"] -->|"sequential(fas*)"| Seq["Batch.Sequential[F, A]"]
    Batch -->|"parallel(n)(fas*)"| Par["Batch.Parallel[F, A]"]
    Batch -->|"monadic(build)"| JB["JobBuilder[F] -> Monadic[A]"]

    Seq -->|quasiBatch| QSeq["QuasiBatch[A]"]
    Seq -->|valueBatch| VSeq["ValueBatch[A]"]
    Par -->|quasiBatch| QPar["QuasiBatch[A]"]
    Par -->|valueBatch| VPar["ValueBatch[A]"]
    JB  -->|monadicBatch| MB["MonadicBatch[A]"]

    QSeq --- note1["Resource[F, _]; run with .use"]:::note
    classDef note fill:#f6f6f6,stroke:#bbb,color:#333,font-style:italic;
```

- **Sequential / Parallel** — independent jobs. `quasiBatch` collects every outcome (failures
  included); `valueBatch` propagates the first failure and keeps only successful values.
- **Monadic** — later jobs depend on earlier results, composed with `map`/`flatMap`. Produces a
  `MonadicBatch` carrying the step history and the final `Either`.

All of `quasiBatch`, `valueBatch`, and `monadicBatch` return a `Resource[F, _]`; the batch runs
when the resource is used, and the active gauge / writers are released on close.

## The two failure models

The `BatchKind` on each job decides what a failure means. This is the core distinction between
`quasiBatch` and `valueBatch`.

```mermaid
flowchart TD
    job["Run one job: (attempt kickoff >> effect)"] --> outcome{"outcome"}
    outcome -->|threw| ex["Left(exception)"]
    outcome -->|value| pred{"predicate(value)?"}
    pred -->|true| ok["Right(value), succeeded = true"]
    pred -->|false| miss["predicate miss"]

    subgraph Quasi["kind = Quasi (quasiBatch)"]
        qex["record Left, succeeded = false<br/>KEEP going"]
        qmiss["record Right(value), succeeded = false<br/>KEEP value + going"]
    end

    subgraph Value["kind = Value (valueBatch)"]
        vex["Left(exception)<br/>raise, ABORT batch"]
        vmiss["Left(PostConditionUnsatisfied)<br/>raise, ABORT batch"]
    end

    ex -.-> qex
    miss -.-> qmiss
    ex -.-> vex
    miss -.-> vmiss
```

So:

- **Quasi** always runs to completion. `succeeded` on the result is always `true`; `allPassed`
  is `false` if any job threw or was rejected by its predicate.
- **Value** stops at the first failing or rejected job by raising, so a `ValueBatch` only ever
  exists when every retained job succeeded (`succeeded` and `allPassed` are both `true`).

## Per-job lifecycle

Every tracked job (sequential, parallel, or monadic) runs through the same steps. Timing brackets
the kickoff log plus the effect; the completion log is written after `end` and is not part of
`took`.

```mermaid
sequenceDiagram
    participant R as Runner
    participant J as Job effect
    participant P as Panel
    participant L as Logger

    R->>R: start = monotonic
    R->>L: logKickoff, info level
    R->>J: run effect inside attempt
    J-->>R: Right value or Left ex
    R->>R: end = monotonic, took = end minus start
    R->>R: build JobRecord and JobState
    Note over R,L: guaranteeCase handleOutcome
    alt completed
        R->>P: updatePanel record, bump ratio and append progress
        R->>L: logCompleted: Succeeded, Unsatisfied, Nonfatal, Critical
    else canceled
        R->>L: logCanceled, warn level
    end
```

`handleOutcome` runs in a finalizer (`guaranteeCase`), so progress and completion logging happen
even on cancellation. `Outcome.Errored` is treated as impossible because the kickoff and effect
are wrapped in `attempt`.

## How the three modes differ

```mermaid
flowchart LR
    subgraph S["Sequential"]
        s1["jobs.traverse(runJob)"] --> s2["List[JobState/JobValue]"]
    end
    subgraph P["Parallel"]
        p1["jobs.parTraverseN(parallelism)(runJob)"] --> p2["List[JobState/JobValue]"]
    end
    subgraph M["Monadic"]
        m1["Kleisli over StateT[Resource[F], JobCursor]"] --> m2["ExecutionState: eoa + reversed history"]
    end
```

- **Sequential** threads jobs with `traverse`; **Parallel** with `parTraverseN`. Both time the
  whole traversal (`spent`) and build the result from the collected `JobState`/`JobValue` list.
- **Monadic** threads a `JobCursor` (running index + carry-over start time) through a
  `StateT`/`Kleisli`, so each job's `start` is the previous job's `end`. Its `took` therefore
  absorbs any invisible `untracked`/`pure` steps between jobs, and the per-job durations sum
  exactly to `spent`. A `Left` short-circuits the remaining chain; `withFilter` turns a rejected
  value into a `PostConditionUnsatisfied` `Left`.

## Result types

```mermaid
classDiagram
    class BatchResult~A~ {
        <<sealed>>
        scope: MetricScope
        spent: Duration
        mode: BatchMode
        batchId: BatchId
        jobs: List[A]
        succeeded: Boolean
        allPassed: Boolean
    }
    BatchResult <|-- QuasiBatch
    BatchResult <|-- ValueBatch
    BatchResult <|-- MonadicBatch

    class QuasiBatch~A~ {
        jobs: List[JobState[A]]
        succeeded = true
        allPassed = jobs.forall(_.succeeded)
    }
    class ValueBatch~A~ {
        jobs: List[JobValue[A]]
        succeeded = true
        allPassed = true
    }
    class MonadicBatch~A~ {
        jobs: List[JobRecord]
        result: Either[Throwable, A]
        succeeded = result.isRight
        allPassed = jobs.forall(_.succeeded)
    }
```

- `succeeded` — did the batch operation itself complete? Quasi and Value always do; Monadic
  completes only when its chain is not short-circuited.
- `allPassed` — did every job satisfy its post-condition? Can be `false` even when `succeeded`
  is `true` (a completed quasi/monadic batch with some rejected jobs).

See `../src/main/scala/com/github/chenharryhua/nanjin/guard/batch/data.scala` for the result and job types, and `internal.scala` for `ExecutionState`,
`JobCursor`, and the log-entry classification.
