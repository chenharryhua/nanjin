package com.github.chenharryhua.nanjin.guard.batch

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Sync}
import cats.implicits.{catsSyntaxMonadErrorRethrow, toFunctorOps, toTraverseOps}
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import monocle.Monocle.toAppliedFocusOps

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object LightBatch {

  sealed protected trait Runner[F[_], A] {
    def withJobRename(f: Endo[String]): Runner[F, A]
    def withPredicate(f: A => Boolean): Runner[F, A]
    def quasiBatch: F[BatchResultState]
    def batchValue: F[BatchResultValue[List[A]]]
  }

  final class Parallel[F[_], A] private[LightBatch] (
    predicate: Reader[A, Boolean],
    metrics: Metrics[F],
    parallelism: Int,
    jobs: List[JobNameIndex[F, A]])(implicit F: Async[F])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasiBatch: F[BatchResultState] =
      F.timed(F.parTraverseN(parallelism)(jobs) { case JobNameIndex(name, idx, fa) =>
        val job = BatchJob(name, idx, metrics.metricLabel, mode, JobKind.Quasi)
        F.timed(F.attempt(fa)).map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
          JobResultState(job, fd.toJava, eoa.fold(_ => false, predicate.run))
        }
      }).map { case (fd: FiniteDuration, jrs: List[JobResultState]) =>
        BatchResultState(metrics.metricLabel, fd.toJava, mode, jrs.sortBy(_.job.index))
      }

    override def batchValue: F[BatchResultValue[List[A]]] =
      F.timed(F.parTraverseN(parallelism)(jobs) { case JobNameIndex(name, idx, fa) =>
        val job = BatchJob(name, idx, metrics.metricLabel, mode, JobKind.Value)
        F.timed(F.attempt(fa))
          .map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
            eoa.flatMap { a =>
              if (predicate.run(a)) {
                val jrs = JobResultState(job, fd.toJava, done = true)
                Right(JobResultValue(jrs, a))
              } else
                Left(PostConditionUnsatisfied(job))
            }
          }
          .rethrow
      }).map { case (fd: FiniteDuration, jrv: List[JobResultValue[A]]) =>
        val sorted = jrv.sortBy(_.resultState.job.index)
        val brs: BatchResultState =
          BatchResultState(metrics.metricLabel, fd.toJava, mode, sorted.map(_.resultState))
        BatchResultValue(brs, sorted.map(_.value))
      }

    override def withJobRename(f: String => String): Parallel[F, A] =
      new Parallel[F, A](predicate, metrics, parallelism, jobs.map(_.focus(_.name).modify(f)))

    override def withPredicate(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](predicate = Reader(f), metrics, parallelism, jobs)
  }

  final class Sequential[F[_], A] private[LightBatch] (
    predicate: Reader[A, Boolean],
    metrics: Metrics[F],
    jobs: List[JobNameIndex[F, A]])(implicit F: Sync[F])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Sequential

    override def quasiBatch: F[BatchResultState] =
      jobs.traverse { case JobNameIndex(name, idx, fa) =>
        val job = BatchJob(name, idx, metrics.metricLabel, mode, JobKind.Quasi)
        F.timed(F.attempt(fa)).map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
          JobResultState(job, fd.toJava, eoa.fold(_ => false, predicate.run))
        }
      }.map(sequentialBatchResultState(metrics, mode))

    override def batchValue: F[BatchResultValue[List[A]]] =
      jobs.traverse { case JobNameIndex(name, idx, fa) =>
        val job = BatchJob(name, idx, metrics.metricLabel, mode, JobKind.Value)
        F.timed(F.attempt(fa))
          .map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
            eoa.flatMap { a =>
              if (predicate.run(a)) {
                val jrs = JobResultState(job, fd.toJava, done = true)
                Right(JobResultValue(jrs, a))
              } else
                Left(PostConditionUnsatisfied(job))
            }
          }
          .rethrow
      }.map(sequentialBatchResultValue(metrics, mode))

    override def withJobRename(f: String => String): Sequential[F, A] =
      new Sequential[F, A](predicate, metrics, jobs.map(_.focus(_.name).modify(f)))

    override def withPredicate(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](predicate = Reader(f), metrics, jobs)

  }
}

final class LightBatch[F[_]: Async] private[guard] (metrics: Metrics[F]) {

  def sequential[A](fas: (String, F[A])*): LightBatch.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new LightBatch.Sequential[F, A](Reader(_ => true), metrics, jobs)
  }

  def parallel[A](parallelism: Int)(fas: (String, F[A])*): LightBatch.Parallel[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new LightBatch.Parallel[F, A](Reader(_ => true), metrics, parallelism, jobs)
  }

  def parallel[A](fas: (String, F[A])*): LightBatch.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

}
