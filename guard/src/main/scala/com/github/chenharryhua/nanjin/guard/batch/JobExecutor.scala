package com.github.chenharryhua.nanjin.guard.batch

import cats.effect.kernel.Temporal
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.logging.Log
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import org.typelevel.otel4s.trace.SpanContext

/** A job that has not yet run: its display name, 1-based position in the batch, and the effect to execute.
  *
  * The effect yields the span context of the job's own span (`None` for untraced batches, which open no span)
  * paired with the job's outcome as an `Either`. The job's own failure is captured in the inner `Either`
  * rather than failing the effect, so the span context is retained even when the job throws; the outer `F`
  * fails only for an unexpected error around the job (e.g. in the span machinery itself).
  */
final private case class JobNameIndex[F[_], A](
  name: String,
  index: Int,
  fa: F[(Option[SpanContext], Either[Throwable, A])])

/** A successful value job: the produced value paired with the job's completion record. Used internally by the
  * `valueJob` path to carry results before they are folded into a `ValueBatch`.
  */
final private case class JobValue[A](record: JobRecord, result: A)

/** A job that has been prepared but not yet run.
  *
  * @param compute
  *   the effect that runs the job and yields its `JobState` (timing, outcome, and produced value)
  * @param job
  *   the job's static metadata, which `Batch` threads into `lifecycle.handleOutcome` for lifecycle logging
  *   and panel updates; `BatchLight` ignores it and uses only `compute`
  */
final private case class ComputeJob[F[_], A](compute: F[JobState[A]], job: Job)

/** Builds the per-job effect shared by both batch front ends.
  *
  * `Batch` and `BatchLight` differ in wrapper (`Resource`/metrics vs. plain `F`) and in whether they log, but
  * the construction of a single job — timing it, running it under `attempt`, and classifying the outcome
  * against the post-condition `predicate` — is identical. That construction lives here so the quasi and value
  * rules have a single source of truth.
  *
  * The quasi and value builders differ in exactly one respect: how they treat a value the `predicate`
  * rejects. See `quasiJob` and `valueJob`.
  *
  * @param predicate
  *   the post-condition applied to a successful value to decide whether the job counts as succeeded
  * @param mode
  *   sequential, parallel, or monadic execution mode, recorded on each `Job`
  * @param scope
  *   the metric scope (label and domain) the jobs run under
  * @param log
  *   `Some` for `Batch`, which emits a kickoff log before each job; `None` for `BatchLight`, where kickoff
  *   logging is a no-op
  */
final private class JobExecutor[F[_], A](
  predicate: A => Boolean,
  mode: BatchMode,
  scope: MetricScope,
  log: Option[Log[F]])(using F: Temporal[F]) {

  private def makeJob(kind: BatchKind, jni: JobNameIndex[F, A], batchId: BatchId) =
    Job(jni.name, jni.index, scope, mode, Some(kind), batchId)

  /** Build a value job: a predicate miss folds into `Left(PostConditionUnsatisfied)` so the value batch can
    * raise it and abort. An exception is likewise a `Left`. Either `Left` is flagged `JobFlag.Failed`; a
    * value that satisfies the predicate is flagged `JobFlag.Accepted`.
    */
  def valueJob(jni: JobNameIndex[F, A], batchId: BatchId): ComputeJob[F, A] = {
    val job: Job = makeJob(BatchKind.Value, jni, batchId)
    val compute: F[JobState[A]] = for {
      start <- F.monotonic
      _ <- log.traverse(lifecycle.logKickoff(_, job))
      outcome <- jni.fa.attempt
      end <- F.monotonic
    } yield {
      val (spanContext, eoa) =
        outcome.fold[(Option[SpanContext], Either[Throwable, A])](ex => (None, Left(ex)), identity)
      val result: Either[Throwable, A] =
        eoa.flatMap { a =>
          if (predicate(a))
            Right(a)
          else
            Left(PostConditionUnsatisfied(Some(job)))
        }
      JobState(
        JobRecord(job, start, end, spanContext),
        result.fold(_ => JobFlag.Failed, _ => JobFlag.Accepted),
        result)
    }
    ComputeJob(compute, job)
  }

  /** Build a quasi job: a predicate miss is flagged `JobFlag.Unmet` but keeps the value as the result, so the
    * quasi batch retains the outcome and completes. An exception stays a `Left`, flagged `JobFlag.Failed`.
    */
  def quasiJob(jni: JobNameIndex[F, A], batchId: BatchId): ComputeJob[F, A] = {
    val job: Job = makeJob(BatchKind.Quasi, jni, batchId)
    val compute: F[JobState[A]] = for {
      start <- F.monotonic
      _ <- log.traverse(lifecycle.logKickoff(_, job))
      outcome <- jni.fa.attempt
      end <- F.monotonic
    } yield {
      val (spanContext, eoa) =
        outcome.fold[(Option[SpanContext], Either[Throwable, A])](ex => (None, Left(ex)), identity)
      val flag: JobFlag =
        eoa.fold(_ => JobFlag.Unmet, v => if predicate(v) then JobFlag.Accepted else JobFlag.Unmet)
      JobState(JobRecord(job, start, end, spanContext), flag, eoa)
    }
    ComputeJob(compute, job)
  }
}
