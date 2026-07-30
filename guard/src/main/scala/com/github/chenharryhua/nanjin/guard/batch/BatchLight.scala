package com.github.chenharryhua.nanjin.guard.batch

import cats.data.{Kleisli, Reader, StateT}
import cats.effect.kernel.{Async, Sync}
import cats.effect.syntax.clock.clockOps
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import cats.{Applicative, Endo}
import com.github.chenharryhua.nanjin.guard.event.MetricLabel
import monocle.Monocle.focus

import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object BatchLight {
  /*
   * Monadic
   */

  final private case class Callbacks(renameJob: Option[Endo[String]], batchId: UUID)

  final class JobBuilder[F[_]] private[BatchLight] (val metricLabel: MetricLabel, val uuidGenerator: F[UUID])(
    using async: Async[F]):

    private val mode: BatchMode = BatchMode.Monadic

    final class Monadic[A] private[BatchLight] (
      metricLabel: MetricLabel,
      private val kleisli: Kleisli[StateT[F, Int, *], Callbacks, MonadicExecutionState[A]],
      renameJob: Option[Endo[String]],
      uuidGenerator: F[UUID])(using async: Async[F]):

      def withJobRename(f: String => String): Monadic[A] =
        new Monadic[A](metricLabel, kleisli, renameJob = Some(f), uuidGenerator)

      def flatMap[B](f: A => Monadic[B]): Monadic[B] = {
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
        new Monadic[B](metricLabel, runB, renameJob, uuidGenerator)
      }

      def map[B](f: A => B): Monadic[B] =
        new Monadic[B](
          metricLabel = metricLabel,
          kleisli = kleisli.map(_.map(f)),
          renameJob = renameJob,
          uuidGenerator = uuidGenerator)

      def withFilter(f: A => Boolean): Monadic[A] =
        new Monadic[A](
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

      def monadicBatch: F[MonadicBatch[A]] =
        uuidGenerator.flatMap { batchId =>
          kleisli(Callbacks(renameJob, batchId))
            .run(1)
            .map { case (_, MonadicExecutionState(eoa, history)) =>
              MonadicBatch(
                label = metricLabel,
                spent = history.map(_.took).foldLeft(Duration.ZERO)(_.plus(_)),
                batchId = batchId,
                jobs = history,
                result = eoa)
            }
        }
    end Monadic

    private def pureMonadic[A](a: A): Monadic[A] =
      new Monadic[A](
        metricLabel = metricLabel,
        kleisli = Kleisli { _ =>
          StateT(index => async.pure(index -> MonadicExecutionState(Right(a), Nil)))
        },
        renameJob = None,
        uuidGenerator = uuidGenerator
      )

    given Applicative[Monadic] with
      override def pure[A](a: A): Monadic[A] = pureMonadic(a)
      override def ap[A, B](ff: Monadic[A => B])(fa: Monadic[A]): Monadic[B] =
        ff.flatMap(f => fa.map(f))
    end given

    // job constructors
    def pure[A](a: A): Monadic[A] = pureMonadic(a)

    def apply[A](name: String, fa: F[A]): Monadic[A] =
      new Monadic[A](
        metricLabel = metricLabel,
        kleisli = Kleisli { cb =>
          StateT { (index: Int) =>
            val job: Job = Job(
              name = cb.renameJob.fold(name)(_.apply(name)),
              index = index,
              label = metricLabel,
              mode = mode,
              kind = BatchKind.Value,
              batchId = cb.batchId)

            fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
              val js = JobState(CompletedJob(job, fd.toJava, eoa.isRight), eoa)
              (index + 1, MonadicExecutionState(eoa = eoa, history = List(js.completed)))
            }
          }
        },
        renameJob = None,
        uuidGenerator = uuidGenerator
      )

    def failSafe(name: String, fa: F[Boolean]): Monadic[Boolean] =
      new Monadic[Boolean](
        metricLabel = metricLabel,
        kleisli = Kleisli { cb =>
          StateT { (index: Int) =>
            val job: Job = Job(
              name = cb.renameJob.fold(name)(_.apply(name)),
              index = index,
              label = metricLabel,
              mode = mode,
              kind = BatchKind.Quasi,
              batchId = cb.batchId)

            fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, Boolean]) =>
              val done = eoa.fold(_ => false, identity)
              val completed = CompletedJob(job, fd.toJava, done)
              (index + 1, MonadicExecutionState(eoa = Right(done), history = List(completed)))
            }
          }
        },
        renameJob = None,
        uuidGenerator = uuidGenerator
      )
  end JobBuilder

  /*
   * Runners
   */
  sealed protected trait BatchRunner[F[_], A] {
    def withJobRename(f: Endo[String]): BatchRunner[F, A]
    def withPostCondition(f: A => Boolean): BatchRunner[F, A]
    def quasiBatch: F[QuasiBatch[A]]
    def batchValue: F[BatchValue[A]]
  }

  /*
   * Parallel
   */
  final class Parallel[F[_], A] private[BatchLight] (
    metricLabel: MetricLabel,
    predicate: Reader[A, Boolean],
    parallelism: Int,
    jobs: List[JobNameIndex[F, A]],
    uuidGenerator: F[UUID])(implicit F: Async[F])
      extends BatchRunner[F, A] {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasiBatch: F[QuasiBatch[A]] =
      uuidGenerator.flatMap { batchId =>
        F.timed(F.parTraverseN[List, JobNameIndex[F, A], JobState[A]](parallelism)(jobs) {
          case JobNameIndex(name, idx, fa) =>
            val job = Job(name, idx, metricLabel, mode, BatchKind.Quasi, batchId)
            F.timed(F.attempt(fa)).map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
              val result: Either[Throwable, A] =
                eoa.flatMap(a => if predicate(a) then Right(a) else Left(PostConditionUnsatisfied(job)))
              JobState(CompletedJob(job, fd.toJava, result.isRight), result)
            }
        }).map { case (fd: FiniteDuration, jobs: List[JobState[A]]) =>
          QuasiBatch(label = metricLabel, spent = fd.toJava, mode = mode, batchId = batchId, jobs = jobs)
        }
      }

    override def batchValue: F[BatchValue[A]] =
      uuidGenerator.flatMap { batchId =>
        F.timed(F.parTraverseN[List, JobNameIndex[F, A], JobValue[A]](parallelism)(jobs) {
          case JobNameIndex(name, idx, fa) =>
            val job = Job(name, idx, metricLabel, mode, BatchKind.Value, batchId)
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                eoa.flatMap(a =>
                  if predicate(a) then Right(a) else Left(PostConditionUnsatisfied(job))) match {
                  case Left(ex)     => F.raiseError[JobValue[A]](ex)
                  case Right(value) => JobValue(CompletedJob(job, fd.toJava, true), value).pure[F]
                }
              }
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

    override def withPostCondition(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](metricLabel, predicate = Reader(f), parallelism, jobs, uuidGenerator)
  }

  /*
   * Sequential
   */
  final class Sequential[F[_], A] private[BatchLight] (
    metricLabel: MetricLabel,
    predicate: Reader[A, Boolean],
    jobs: List[JobNameIndex[F, A]],
    uuidGenerator: F[UUID])(implicit F: Sync[F])
      extends BatchRunner[F, A] {

    private val mode: BatchMode = BatchMode.Sequential

    override def quasiBatch: F[QuasiBatch[A]] =
      uuidGenerator.flatMap { batchId =>
        jobs.traverse { case JobNameIndex(name, idx, fa) =>
          val job = Job(name, idx, metricLabel, mode, BatchKind.Quasi, batchId)
          F.timed(F.attempt(fa)).map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
            val result: Either[Throwable, A] =
              eoa.flatMap(a => if predicate(a) then Right(a) else Left(PostConditionUnsatisfied(job)))

            JobState(CompletedJob(job, fd.toJava, result.isRight), result)
          }
        }.map(jobs =>
          QuasiBatch(
            label = metricLabel,
            spent = jobs.map(_.completed.took).foldLeft(Duration.ZERO)(_.plus(_)),
            mode = mode,
            batchId = batchId,
            jobs = jobs
          ))
      }

    override def batchValue: F[BatchValue[A]] =
      uuidGenerator.flatMap { batchId =>
        jobs.traverse { case JobNameIndex(name, idx, fa) =>
          val job = Job(name, idx, metricLabel, mode, BatchKind.Value, batchId)
          F.timed(F.attempt(fa))
            .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
              eoa.flatMap(a => if predicate(a) then Right(a) else Left(PostConditionUnsatisfied(job))) match {
                case Left(ex)     => F.raiseError[JobValue[A]](ex)
                case Right(value) => JobValue(CompletedJob(job, fd.toJava, true), value).pure[F]
              }
            }
        }.map { jobs =>
          BatchValue(
            label = metricLabel,
            spent = jobs.map(_.completed.took).foldLeft(Duration.ZERO)(_.plus(_)),
            mode = mode,
            batchId = batchId,
            jobs = jobs)
        }
      }

    override def withJobRename(f: String => String): Sequential[F, A] =
      new Sequential[F, A](metricLabel, predicate, jobs.map(_.focus(_.name).modify(f)), uuidGenerator)

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](metricLabel, predicate = Reader(f), jobs, uuidGenerator)

  }
}

/** BatchLight is a simpler API for short-lived jobs. It intentionally avoids the richer lifecycle and
  * progress-tracking machinery of Batch and focuses on straightforward, low-overhead execution.
  */
final class BatchLight[F[_]: Async] private[guard] (metricLabel: MetricLabel, uuidGenerator: F[UUID]) {

  /** Creates a lightweight sequential batch from a list of named effects. */
  def sequential[A](fas: (String, F[A])*): BatchLight.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new BatchLight.Sequential[F, A](metricLabel, Reader(_ => true), jobs, uuidGenerator)
  }

  /** Creates a lightweight parallel batch from a list of named effects using the given parallelism. */
  def parallel[A](parallelism: Int)(fas: (String, F[A])*): BatchLight.Parallel[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new BatchLight.Parallel[F, A](metricLabel, Reader(_ => true), parallelism, jobs, uuidGenerator)
  }

  /** Creates a lightweight parallel batch with parallelism inferred from the number of jobs. */
  def parallel[A](fas: (String, F[A])*): BatchLight.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

  /** Builds a lightweight monadic batch using a fluent job builder. */
  def monadic[A](f: BatchLight.JobBuilder[F] => A): A = {
    val builder = new BatchLight.JobBuilder[F](metricLabel, uuidGenerator)
    f(builder)
  }
}
