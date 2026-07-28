package com.github.chenharryhua.nanjin.guard.batch

import cats.Endo
import cats.data.{Kleisli, NonEmptyList, Reader, StateT}
import cats.effect.kernel.{Async, Sync}
import cats.effect.syntax.clock.given
import cats.implicits.{catsSyntaxMonadErrorRethrow, toFlatMapOps, toFunctorOps, toTraverseOps}
import cats.syntax.applicativeError.given
import com.github.chenharryhua.nanjin.guard.event.MetricLabel
import monocle.Monocle.focus

import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object BatchLight {

  final private case class MonadicExecutionState[A](
    eoa: Either[Throwable, A],
    history: NonEmptyList[JobState]) {
    def update[B](ex: Throwable): MonadicExecutionState[B] = copy(eoa = Left(ex))
    def prependHistory[B](js: MonadicExecutionState[B]): MonadicExecutionState[B] =
      MonadicExecutionState[B](js.eoa, js.history ::: history)

    def map[B](f: A => B): MonadicExecutionState[B] = copy(eoa = eoa.map(f))
  }

  final private case class Callbacks(renameJob: Option[Endo[String]], batchId: UUID)

  final class JobBuilder[F[_]] private[BatchLight] (metricLabel: MetricLabel, uuidGenerator: F[UUID])(using
    async: Async[F]) {

    private val mode: BatchMode = BatchMode.Monadic

    def apply[A](name: String, fa: F[A]): Monadic[F, A] =
      new Monadic[F, A](
        metricLabel = metricLabel,
        kleisli = Kleisli { cb =>
          StateT { (index: Int) =>
            val job: BatchJob = BatchJob(
              name = cb.renameJob.fold(name)(_.apply(name)),
              index = index,
              label = metricLabel,
              mode = mode,
              kind = BatchKind.Value,
              batchId = cb.batchId)

            fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
              val jrs = JobState(job, fd.toJava, eoa.isRight)
              (index + 1, MonadicExecutionState(eoa = eoa, history = NonEmptyList.one(jrs)))
            }
          }
        },
        renameJob = None,
        uuidGenerator = uuidGenerator
      )

    def failSafe(name: String, fa: F[Boolean]): Monadic[F, Boolean] =
      new Monadic[F, Boolean](
        metricLabel = metricLabel,
        kleisli = Kleisli { cb =>
          StateT { (index: Int) =>
            val job: BatchJob = BatchJob(
              name = cb.renameJob.fold(name)(_.apply(name)),
              index = index,
              label = metricLabel,
              mode = mode,
              kind = BatchKind.Quasi,
              batchId = cb.batchId)

            fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, Boolean]) =>
              val jrs = JobState(job, fd.toJava, eoa.fold(_ => false, identity))
              (index + 1, MonadicExecutionState(eoa = Right(jrs.done), history = NonEmptyList.one(jrs)))
            }
          }
        },
        renameJob = None,
        uuidGenerator = uuidGenerator
      )
  }

  final class Monadic[F[_], A] private[BatchLight] (
    metricLabel: MetricLabel,
    private val kleisli: Kleisli[StateT[F, Int, *], Callbacks, MonadicExecutionState[A]],
    renameJob: Option[Endo[String]],
    uuidGenerator: F[UUID])(using async: Async[F]) {

    def withJobRename(f: String => String): Monadic[F, A] =
      new Monadic[F, A](metricLabel, kleisli, renameJob = Some(f), uuidGenerator)

    def flatMap[B](f: A => Monadic[F, B]): Monadic[F, B] = {
      val runB: Kleisli[StateT[F, Int, *], Callbacks, MonadicExecutionState[B]] = Kleisli { cb =>
        StateT { (index: Int) =>
          kleisli(cb).run(index).flatMap { case (nextIndex, jobState) =>
            jobState.eoa match {
              case Left(ex) => async.pure((nextIndex, jobState.update[B](ex)))
              case Right(a) =>
                f(a).kleisli(cb).run(nextIndex).map { case (finalIndex, nextState) =>
                  (finalIndex, jobState.prependHistory[B](nextState))
                }
            }
          }
        }
      }
      new Monadic[F, B](metricLabel, runB, renameJob, uuidGenerator)
    }

    def map[B](f: A => B): Monadic[F, B] =
      new Monadic[F, B](
        metricLabel = metricLabel,
        kleisli = kleisli.map(_.map(f)),
        renameJob = renameJob,
        uuidGenerator = uuidGenerator)

    def withFilter(f: A => Boolean): Monadic[F, A] =
      new Monadic[F, A](
        metricLabel = metricLabel,
        kleisli = Kleisli { cb =>
          kleisli(cb).map { case unchange @ MonadicExecutionState(eoa, history) =>
            eoa match {
              case Left(_)      => unchange
              case Right(value) =>
                if (f(value)) unchange
                else MonadicExecutionState[A](Left(PostConditionUnsatisfied(history.head.job)), history)
            }
          }
        },
        renameJob = renameJob,
        uuidGenerator = uuidGenerator
      )

    def batchValue: F[MonadicValue[A]] =
      uuidGenerator.flatMap { batchId =>
        kleisli(Callbacks(renameJob, batchId))
          .run(1)
          .map { case (_, MonadicExecutionState(eoa, history)) =>
            eoa.map { a =>
              val bs = BatchState(
                label = metricLabel,
                spent = history.map(_.took).foldLeft(Duration.ZERO)(_.plus(_)),
                mode = BatchMode.Monadic,
                batchId = batchId,
                jobs = history.toList)
              MonadicValue(bs, a)
            }
          }
          .rethrow
      }
  }

  sealed protected trait Runner[F[_], A] {
    def withJobRename(f: Endo[String]): Runner[F, A]
    def withPredicate(f: A => Boolean): Runner[F, A]
    def quasiBatch: F[BatchState]
    def batchValue: F[BatchValue[A]]
  }

  final class Parallel[F[_], A] private[BatchLight] (
    metricLabel: MetricLabel,
    predicate: Reader[A, Boolean],
    parallelism: Int,
    jobs: List[JobNameIndex[F, A]],
    uuidGenerator: F[UUID])(implicit F: Async[F])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasiBatch: F[BatchState] =
      uuidGenerator.flatMap { batchId =>
        F.timed(F.parTraverseN[List, JobNameIndex[F, A], JobState](parallelism)(jobs) {
          case JobNameIndex(name, idx, fa) =>
            val job = BatchJob(name, idx, metricLabel, mode, BatchKind.Quasi, batchId)
            F.timed(F.attempt(fa)).map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
              JobState(job, fd.toJava, eoa.fold(_ => false, predicate.run))
            }
        }).map { case (fd: FiniteDuration, jobs: List[JobState]) =>
          BatchState(label = metricLabel, spent = fd.toJava, mode = mode, batchId = batchId, jobs = jobs)
        }
      }

    override def batchValue: F[BatchValue[A]] =
      uuidGenerator.flatMap { batchId =>
        F.timed(F.parTraverseN[List, JobNameIndex[F, A], JobValue[A]](parallelism)(jobs) {
          case JobNameIndex(name, idx, fa) =>
            val job = BatchJob(name, idx, metricLabel, mode, BatchKind.Value, batchId)
            F.timed(F.attempt(fa))
              .map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                eoa.flatMap { a =>
                  if (predicate.run(a)) {
                    val jrs = JobState(job, fd.toJava, done = true)
                    Right(JobValue(jrs, a))
                  } else
                    Left(PostConditionUnsatisfied(job))
                }
              }
              .rethrow
        }).map { case (fd: FiniteDuration, jobs: List[JobValue[A]]) =>
          BatchValue(label = metricLabel, spent = fd.toJava, mode = mode, batchId = batchId, jobs = jobs)
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

    override def quasiBatch: F[BatchState] =
      uuidGenerator.flatMap { batchId =>
        jobs.traverse { case JobNameIndex(name, idx, fa) =>
          val job = BatchJob(name, idx, metricLabel, mode, BatchKind.Quasi, batchId)
          F.timed(F.attempt(fa)).map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
            JobState(job, fd.toJava, eoa.fold(_ => false, predicate.run))
          }
        }.map(jobs =>
          BatchState(
            label = metricLabel,
            spent = jobs.map(_.took).foldLeft(Duration.ZERO)(_.plus(_)),
            mode = mode,
            batchId = batchId,
            jobs = jobs
          ))
      }

    override def batchValue: F[BatchValue[A]] =
      uuidGenerator.flatMap { batchId =>
        jobs.traverse { case JobNameIndex(name, idx, fa) =>
          val job = BatchJob(name, idx, metricLabel, mode, BatchKind.Value, batchId)
          F.timed(F.attempt(fa))
            .map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
              eoa.flatMap { a =>
                if (predicate.run(a)) {
                  val jrs = JobState(job, fd.toJava, done = true)
                  Right(JobValue(jrs, a))
                } else
                  Left(PostConditionUnsatisfied(job))
              }
            }
            .rethrow
        }.map { jobs =>
          BatchValue(
            label = metricLabel,
            spent = jobs.map(_.state.took).foldLeft(Duration.ZERO)(_.plus(_)),
            mode = mode,
            batchId = batchId,
            jobs = jobs)
        }
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

  def monadic[A](f: BatchLight.JobBuilder[F] => A): A =
    f(new BatchLight.JobBuilder[F](metricLabel, uuidGenerator))

}
