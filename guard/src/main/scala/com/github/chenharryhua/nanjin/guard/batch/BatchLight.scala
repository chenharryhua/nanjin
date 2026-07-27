package com.github.chenharryhua.nanjin.guard.batch

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Sync}
import cats.implicits.{catsSyntaxMonadErrorRethrow, toFlatMapOps, toFunctorOps, toTraverseOps}
import com.github.chenharryhua.nanjin.guard.event.MetricLabel
import monocle.Monocle.focus

import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object BatchLight {

  sealed protected trait Runner[F[_], A] {
    def withJobRename(f: Endo[String]): Runner[F, A]
    def withPredicate(f: A => Boolean): Runner[F, A]
    def quasiBatch: F[BatchResultState]
    def batchValue: F[BatchResultValue[List[A]]]
  }

  final class Parallel[F[_], A] private[BatchLight] (
    metricLabel: MetricLabel,
    predicate: Reader[A, Boolean],
    parallelism: Int,
    jobs: List[JobNameIndex[F, A]],
    uuidGenerator: F[UUID])(implicit F: Async[F])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasiBatch: F[BatchResultState] =
      uuidGenerator.flatMap { batchId =>
        F.timed(F.parTraverseN[List, JobNameIndex[F, A], JobResultState](parallelism)(jobs) {
          case JobNameIndex(name, idx, fa) =>
            val job = BatchJob(name, idx, metricLabel, mode, BatchKind.Quasi, batchId)
            F.timed(F.attempt(fa)).map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
              JobResultState(job, fd.toJava, eoa.fold(_ => false, predicate.run))
            }
        }).map { case (fd: FiniteDuration, jrs: List[JobResultState]) =>
          BatchResultState(metricLabel, fd.toJava, mode, batchId, jrs.sortBy(_.job.index))
        }
      }

    override def batchValue: F[BatchResultValue[List[A]]] =
      uuidGenerator.flatMap { batchId =>
        F.timed(F.parTraverseN[List, JobNameIndex[F, A], JobResultValue[A]](parallelism)(jobs) {
          case JobNameIndex(name, idx, fa) =>
            val job = BatchJob(name, idx, metricLabel, mode, BatchKind.Value, batchId)
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
            BatchResultState(metricLabel, fd.toJava, mode, batchId, sorted.map(_.resultState))
          BatchResultValue(brs, sorted.map(_.value))
        }
      }

    override def withJobRename(f: String => String): Parallel[F, A] =
      new Parallel[F, A](
        metricLabel,
        predicate,
        parallelism,
        jobs.map(_.focus(_.name).modify(f)),
        uuidGenerator)

    override def withPredicate(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](metricLabel, predicate = Reader(f), parallelism, jobs, uuidGenerator)
  }

  final class Sequential[F[_], A] private[BatchLight] (
    metricLabel: MetricLabel,
    predicate: Reader[A, Boolean],
    jobs: List[JobNameIndex[F, A]],
    uuidGenerator: F[UUID])(implicit F: Sync[F])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Sequential

    override def quasiBatch: F[BatchResultState] =
      uuidGenerator.flatMap { batchId =>
        jobs.traverse { case JobNameIndex(name, idx, fa) =>
          val job = BatchJob(name, idx, metricLabel, mode, BatchKind.Quasi, batchId)
          F.timed(F.attempt(fa)).map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
            JobResultState(job, fd.toJava, eoa.fold(_ => false, predicate.run))
          }
        }.map(sequential_batch_result_state(metricLabel, mode, batchId))
      }

    override def batchValue: F[BatchResultValue[List[A]]] =
      uuidGenerator.flatMap { batchId =>
        jobs.traverse { case JobNameIndex(name, idx, fa) =>
          val job = BatchJob(name, idx, metricLabel, mode, BatchKind.Value, batchId)
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
        }.map(sequential_batch_result_value(metricLabel, mode, batchId))
      }

    override def withJobRename(f: String => String): Sequential[F, A] =
      new Sequential[F, A](metricLabel, predicate, jobs.map(_.focus(_.name).modify(f)), uuidGenerator)

    override def withPredicate(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](metricLabel, predicate = Reader(f), jobs, uuidGenerator)

  }
}

final class BatchLight[F[_]: Async] private[guard] (metricLabel: MetricLabel, uuidGenerator: F[UUID]) {

  def sequential[A](fas: (String, F[A])*): BatchLight.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new BatchLight.Sequential[F, A](metricLabel, Reader(_ => true), jobs, uuidGenerator)
  }

  def parallel[A](parallelism: Int)(fas: (String, F[A])*): BatchLight.Parallel[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new BatchLight.Parallel[F, A](metricLabel, Reader(_ => true), parallelism, jobs, uuidGenerator)
  }

  def parallel[A](fas: (String, F[A])*): BatchLight.Parallel[F, A] =
    parallel[A](fas.size)(fas*)
    
}
