package com.github.chenharryhua.nanjin.guard.batch

import cats.effect.kernel.Temporal
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.logging.Log
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope

/** A job that has been prepared but not yet run.
  *
  * @param compute
  *   the effect that runs the job and yields its `JobState` (timing, outcome, and produced value)
  * @param job
  *   the job's static metadata, which `Batch` threads into `handleOutcome` for lifecycle logging and panel
  *   updates; `BatchLight` ignores it and uses only `compute`
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
    * raise it and abort. An exception is likewise a `Left`.
    */
  def valueJob(jni: JobNameIndex[F, A], batchId: BatchId): ComputeJob[F, A] = {
    val job: Job = makeJob(BatchKind.Value, jni, batchId)
    val compute: F[JobState[A]] = for {
      start <- F.monotonic
      _ <- log.traverse(logKickoff(_, job))
      eoa <- jni.fa.attempt
      end <- F.monotonic
    } yield {
      val result: Either[Throwable, A] =
        eoa.flatMap { a =>
          if (predicate(a))
            Right(a)
          else
            Left(PostConditionUnsatisfied(Some(job)))
        }
      JobState(JobRecord(job, start, end, result.isRight), result)
    }
    ComputeJob(compute, job)
  }

  /** Build a quasi job: a predicate miss records `succeeded = false` but keeps the value as the result, so
    * the quasi batch retains the outcome and completes. An exception stays a `Left`.
    */
  def quasiJob(jni: JobNameIndex[F, A], batchId: BatchId): ComputeJob[F, A] = {
    val job: Job = makeJob(BatchKind.Quasi, jni, batchId)
    val compute: F[JobState[A]] = for {
      start <- F.monotonic
      _ <- log.traverse(logKickoff(_, job))
      eoa <- jni.fa.attempt
      end <- F.monotonic
    } yield {
      val succeeded = eoa.fold(_ => false, predicate)
      JobState(JobRecord(job, start, end, succeeded), eoa)
    }
    ComputeJob(compute, job)
  }
}
