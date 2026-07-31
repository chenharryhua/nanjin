package com.github.chenharryhua.nanjin.guard.batch

import cats.Applicative
import cats.data.{Kleisli, Reader, StateT}
import cats.effect.kernel.{Async, Sync}
import cats.effect.syntax.clock.clockOps
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.guard.event.MetricLabel

import java.time.Duration
import java.util.UUID
import scala.Right
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object BatchLight:
  /*
   * Monadic
   */

  final class JobBuilder[F[_]: Async] private[BatchLight] (
    val metricLabel: MetricLabel,
    val uuidGenerator: F[UUID]):

    private val mode: BatchMode = BatchMode.Monadic

    final class Monadic[A] private[BatchLight] (
      private val kleisli: Kleisli[StateT[F, Int, *], UUID, ExecutionState[A]]):

      def flatMap[B](f: A => Monadic[B]): Monadic[B] = {
        val runB: Kleisli[StateT[F, Int, *], UUID, ExecutionState[B]] = Kleisli { (batchId: UUID) =>
          StateT { (index: Int) =>
            kleisli(batchId).run(index).flatMap { case (nextIndex, jobState) =>
              jobState.eoa match {
                case Left(ex) => (nextIndex -> jobState.update[B](ex)).pure[F]
                case Right(a) =>
                  f(a).kleisli(batchId).run(nextIndex).map { case (finalIndex, nextState) =>
                    finalIndex -> jobState.prependHistory[B](nextState)
                  }
              }
            }
          }
        }
        new Monadic[B](runB)
      }

      def map[B](f: A => B): Monadic[B] = new Monadic[B](kleisli.map(_.map(f)))

      def withFilter(f: A => Boolean): Monadic[A] =
        new Monadic[A](
          Kleisli { (batchId: UUID) =>
            kleisli(batchId).map { case unchange @ ExecutionState(eoa, history) =>
              eoa match {
                case Left(_)      => unchange
                case Right(value) =>
                  if (f(value))
                    unchange
                  else {
                    val err = PostConditionUnsatisfied(history.headOption.map(_.job))
                    ExecutionState[A](Left(err), history)
                  }
              }
            }
          }
        )

      def monadicBatch: F[MonadicBatch[A]] =
        uuidGenerator.flatMap { (batchId: UUID) =>
          kleisli(batchId)
            .run(1)
            .map { case (_, ExecutionState(eoa, history)) =>
              MonadicBatch(
                label = metricLabel,
                spent = history.map(_.took).foldLeft(Duration.ZERO)(_.plus(_)),
                batchId = batchId,
                jobs = history.reverse,
                result = eoa)
            }
        }
    end Monadic
    object Monadic:
      given Applicative[Monadic] with
        override def pure[A](a: A): Monadic[A] = JobBuilder.this.pure(a)
        override def ap[A, B](ff: Monadic[A => B])(fa: Monadic[A]): Monadic[B] =
          ff.flatMap(fa.map)
      end given
    end Monadic

    // job constructors

    def pure[A](a: A): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(index => (index -> ExecutionState(Right(a), Nil)).pure[F])
      })

    def apply[A](name: String, fa: F[A]): Monadic[A] =
      new Monadic[A](
        Kleisli { (batchId: UUID) =>
          StateT { (index: Int) =>
            val job: Job = Job(
              name = name,
              index = index,
              label = metricLabel,
              mode = mode,
              kind = BatchKind.Value,
              batchId = batchId)

            fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
              val completed = CompletedJob(job, fd.toJava, eoa.isRight)
              index + 1 -> ExecutionState(eoa = eoa, history = List(completed))
            }
          }
        }
      )

    def failSafe(name: String, fa: F[Boolean]): Monadic[Boolean] =
      new Monadic[Boolean](
        Kleisli { (batchId: UUID) =>
          StateT { (index: Int) =>
            val job: Job = Job(
              name = name,
              index = index,
              label = metricLabel,
              mode = mode,
              kind = BatchKind.Quasi,
              batchId = batchId)

            fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, Boolean]) =>
              val done = eoa.fold(_ => false, identity)
              val completed = CompletedJob(job, fd.toJava, done)
              index + 1 -> ExecutionState(eoa = Right(done), history = List(completed))
            }
          }
        }
      )
  end JobBuilder

  /*
   * Runners
   */
  sealed protected trait BatchRunner[F[_], A] {
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
      uuidGenerator.flatMap { (batchId: UUID) =>
        F.timed(F.parTraverseN[List, JobNameIndex[F, A], JobState[A]](parallelism)(jobs) {
          case JobNameIndex(name, idx, fa) =>
            val job = Job(name, idx, metricLabel, mode, BatchKind.Quasi, batchId)
            F.timed(F.attempt(fa)).map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
              val result: Either[Throwable, A] =
                eoa.flatMap { a =>
                  if (predicate(a))
                    Right(a)
                  else
                    Left(PostConditionUnsatisfied(Some(job)))
                }
              JobState(CompletedJob(job, fd.toJava, result.isRight), result)
            }
        }).map { case (fd: FiniteDuration, jobs: List[JobState[A]]) =>
          QuasiBatch(
            label = metricLabel,
            spent = fd.toJava,
            mode = mode,
            batchId = batchId,
            jobs = jobs.sortBy(_.completed.job.index))
        }
      }

    override def batchValue: F[BatchValue[A]] =
      uuidGenerator.flatMap { (batchId: UUID) =>
        F.timed(F.parTraverseN[List, JobNameIndex[F, A], JobValue[A]](parallelism)(jobs) {
          case JobNameIndex(name, idx, fa) =>
            val job = Job(name, idx, metricLabel, mode, BatchKind.Value, batchId)
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                eoa.flatMap { a =>
                  if (predicate(a))
                    Right(a)
                  else
                    Left(PostConditionUnsatisfied(Some(job)))
                }.match {
                  case Left(ex)     => F.raiseError[JobValue[A]](ex)
                  case Right(value) => JobValue(CompletedJob(job, fd.toJava, true), value).pure[F]
                }
              }
        }).map { case (fd: FiniteDuration, jobs: List[JobValue[A]]) =>
          BatchValue(
            label = metricLabel,
            spent = fd.toJava,
            mode = mode,
            batchId = batchId,
            jobs = jobs.sortBy(_.completed.job.index))
        }
      }

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
      uuidGenerator.flatMap { (batchId: UUID) =>
        jobs.traverse { case JobNameIndex(name, idx, fa) =>
          val job = Job(name, idx, metricLabel, mode, BatchKind.Quasi, batchId)
          F.timed(F.attempt(fa)).map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
            val result: Either[Throwable, A] =
              eoa.flatMap { a =>
                if (predicate(a))
                  Right(a)
                else
                  Left(PostConditionUnsatisfied(Some(job)))
              }

            JobState(CompletedJob(job, fd.toJava, result.isRight), result)
          }
        }.map(jobs =>
          QuasiBatch(
            label = metricLabel,
            spent = jobs.map(_.completed.took).foldLeft(Duration.ZERO)(_.plus(_)),
            mode = mode,
            batchId = batchId,
            jobs = jobs))
      }

    override def batchValue: F[BatchValue[A]] =
      uuidGenerator.flatMap { (batchId: UUID) =>
        jobs.traverse { case JobNameIndex(name, idx, fa) =>
          val job = Job(name, idx, metricLabel, mode, BatchKind.Value, batchId)
          F.timed(F.attempt(fa))
            .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
              eoa.flatMap { a =>
                if (predicate(a))
                  Right(a)
                else
                  Left(PostConditionUnsatisfied(Some(job)))
              }.match {
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

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](metricLabel, predicate = Reader(f), jobs, uuidGenerator)
  }

end BatchLight

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
