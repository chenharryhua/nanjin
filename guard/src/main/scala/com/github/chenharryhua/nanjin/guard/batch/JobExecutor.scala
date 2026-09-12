package com.github.chenharryhua.nanjin.guard.batch

import cats.data.Reader
import cats.effect.kernel.Temporal
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.logging.Log
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope

final private case class ComputeJob[F[_], A](compute: F[JobState[A]], job: Job)

final private class JobExecutor[F[_], A](
  predicate: Reader[A, Boolean],
  mode: BatchMode,
  scope: MetricScope,
  log: Option[Log[F]])(using F: Temporal[F]) {

  private def makeJob(kind: BatchKind, jni: JobNameIndex[F, A], batchId: BatchId) =
    Job(jni.name, jni.index, scope, mode, Some(kind), batchId)

  // Value job: a predicate miss folds into Left(PostConditionUnsatisfied) so the value batch can raise it
  // and abort. An exception is likewise a Left.
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
          if (predicate.run(a))
            Right(a)
          else
            Left(PostConditionUnsatisfied(Some(job)))
        }
      JobState(JobRecord(job, start, end, result.isRight), result)
    }
    ComputeJob(compute, job)
  }

  // Quasi job: a predicate miss records `succeeded = false` but keeps the value as the result, so the quasi
  // batch retains the outcome and completes. An exception stays a Left.
  def quasiJob(jni: JobNameIndex[F, A], batchId: BatchId): ComputeJob[F, A] = {
    val job: Job = makeJob(BatchKind.Quasi, jni, batchId)
    val compute: F[JobState[A]] = for {
      start <- F.monotonic
      _ <- log.traverse(logKickoff(_, job))
      eoa <- jni.fa.attempt
      end <- F.monotonic
    } yield {
      val succeeded = eoa.fold(_ => false, predicate.run)
      JobState(JobRecord(job, start, end, succeeded), eoa)
    }
    ComputeJob(compute, job)
  }
}
