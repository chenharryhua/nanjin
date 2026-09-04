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
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import scala.Right
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** Low-overhead batch construction API for short-lived jobs.
  *
  * Obtain a `BatchLight` from `Agent.batchLight(label)`. It has the same sequential, parallel, and monadic
  * shapes as `Batch`, but returns `F` values directly and omits the metrics-backed progress machinery. Use
  * `quasiBatch` to retain per-job failures or `valueBatch` to raise them.
  */
object BatchLight:
  /*
   * Monadic
   */

  final class JobBuilder[F[_]: Async] private[BatchLight] (
    val scope: MetricScope,
    val batchIdGenerator: AtomicLong):

    private val mode: BatchMode = BatchMode.Monadic

    final class Monadic[A] private[BatchLight] (
      private val kleisli: Kleisli[StateT[F, Int, *], Long, ExecutionState[A]]):

      /** Sequence a dependent monadic job when the previous job succeeds. */
      def flatMap[B](f: A => Monadic[B]): Monadic[B] = {
        val runB: Kleisli[StateT[F, Int, *], Long, ExecutionState[B]] =
          Kleisli { (batchId: Long) =>
            StateT { (idx: Int) =>
              kleisli(batchId).run(idx).flatMap { case (nextIdx: Int, execState: ExecutionState[A]) =>
                execState.eoa match {
                  case Left(ex) => (nextIdx -> execState.update[B](ex)).pure[F]
                  case Right(a) =>
                    f(a).kleisli(batchId).run(nextIdx).map {
                      case (finalIdx: Int, nextState: ExecutionState[B]) =>
                        finalIdx -> execState.prependHistory[B](nextState)
                    }
                }
              }
            }
          }
        new Monadic[B](runB)
      }

      /** Transform a successful monadic job value without adding a job. */
      def map[B](f: A => B): Monadic[B] = new Monadic[B](kleisli.map(_.map(f)))

      /** Filter a successful monadic value; a rejected value becomes a failed quasi-job. */
      def withFilter(f: A => Boolean): Monadic[A] =
        new Monadic[A](
          Kleisli { (batchId: Long) =>
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

      /** Execute the monadic batch and return its result in `F`. */
      def monadicBatch: F[MonadicBatch[A]] = {
        val batchId: Long = batchIdGenerator.getAndIncrement()
        kleisli(batchId)
          .run(1)
          .map { case (_, ExecutionState(eoa, history)) =>
            MonadicBatch(
              scope = scope,
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

    /** Lift a pure value into the monadic batch without creating a job. */
    def pure[A](a: A): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(idx => (idx -> ExecutionState(Right(a), Nil)).pure[F])
      })

    /** Lift an effectful value into the monadic batch without creating a job.
      *
      * The effect is not tracked, timed, or reported. If it fails, the exception propagates uncaught and
      * crashes the batch.
      */
    def lift[A](fa: F[A]): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(idx => fa.map(a => idx -> ExecutionState(Right(a), Nil)))
      })

    /** Add a named effect-backed value job. */
    def apply[A](name: String, fa: F[A]): Monadic[A] =
      new Monadic[A](
        Kleisli { (batchId: Long) =>
          StateT { (index: Int) =>
            val job: Job = Job(
              name = name,
              index = index,
              scope = scope,
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

    /** Add a boolean job whose failure or false result is retained as a quasi failure. */
    def failSafe(name: String, fa: F[Boolean]): Monadic[Boolean] =
      new Monadic[Boolean](
        Kleisli { (batchId: Long) =>
          StateT { (index: Int) =>
            val job: Job = Job(
              name = name,
              index = index,
              scope = scope,
              mode = mode,
              kind = BatchKind.Quasi,
              batchId = batchId)

            fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, Boolean]) =>
              val succeeded = eoa.fold(_ => false, identity)
              val completed = CompletedJob(job, fd.toJava, succeeded)
              index + 1 -> ExecutionState(eoa = Right(succeeded), history = List(completed))
            }
          }
        }
      )
  end JobBuilder

  /*
   * Runners
   */
  sealed protected trait BatchRunner[F[_], A] {

    /** Reject successful values that do not satisfy `f`. */
    def withPostCondition(f: A => Boolean): BatchRunner[F, A]

    /** Execute while preserving per-job success or failure state. */
    def quasiBatch: F[QuasiBatch[A]]

    /** Execute and raise on failure, returning successful values. */
    def valueBatch: F[ValueBatch[A]]
  }

  /*
   * Parallel
   */
  final class Parallel[F[_], A] private[BatchLight] (
    scope: MetricScope,
    predicate: Reader[A, Boolean],
    parallelism: Int,
    jobs: List[JobNameIndex[F, A]],
    batchIdGenerator: AtomicLong)(implicit F: Async[F])
      extends BatchRunner[F, A] {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasiBatch: F[QuasiBatch[A]] = {
      val batchId: Long = batchIdGenerator.getAndIncrement()
      F.timed(F.parTraverseN[List, JobNameIndex[F, A], JobState[A]](parallelism)(jobs) {
        case JobNameIndex(name, idx, fa) =>
          val job = Job(name, idx, scope, mode, BatchKind.Quasi, batchId)
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
        QuasiBatch(scope = scope, spent = fd.toJava, mode = mode, batchId = batchId, jobs = jobs)
      }
    }

    override def valueBatch: F[ValueBatch[A]] = {
      val batchId: Long = batchIdGenerator.getAndIncrement()
      F.timed(F.parTraverseN[List, JobNameIndex[F, A], JobValue[A]](parallelism)(jobs) {
        case JobNameIndex(name, idx, fa) =>
          val job = Job(name, idx, scope, mode, BatchKind.Value, batchId)
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
        ValueBatch(scope = scope, spent = fd.toJava, mode = mode, batchId = batchId, jobs = jobs)
      }
    }

    override def withPostCondition(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](scope, predicate = Reader(f), parallelism, jobs, batchIdGenerator)
  }

  /*
   * Sequential
   */
  final class Sequential[F[_], A] private[BatchLight] (
    scope: MetricScope,
    predicate: Reader[A, Boolean],
    jobs: List[JobNameIndex[F, A]],
    batchIdGenerator: AtomicLong)(implicit F: Sync[F])
      extends BatchRunner[F, A] {

    private val mode: BatchMode = BatchMode.Sequential

    override def quasiBatch: F[QuasiBatch[A]] = {
      val batchId: Long = batchIdGenerator.getAndIncrement()
      jobs.traverse { case JobNameIndex(name, idx, fa) =>
        val job = Job(name, idx, scope, mode, BatchKind.Quasi, batchId)
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
          scope = scope,
          spent = jobs.map(_.record.took).foldLeft(Duration.ZERO)(_.plus(_)),
          mode = mode,
          batchId = batchId,
          jobs = jobs))
    }

    override def valueBatch: F[ValueBatch[A]] = {
      val batchId: Long = batchIdGenerator.getAndIncrement()
      jobs.traverse { case JobNameIndex(name, idx, fa) =>
        val job = Job(name, idx, scope, mode, BatchKind.Value, batchId)
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
        ValueBatch(
          scope = scope,
          spent = jobs.map(_.record.took).foldLeft(Duration.ZERO)(_.plus(_)),
          mode = mode,
          batchId = batchId,
          jobs = jobs)
      }
    }

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](scope, predicate = Reader(f), jobs, batchIdGenerator)
  }

end BatchLight

/** Lightweight batch façade for short-lived jobs.
  *
  * It intentionally avoids the richer lifecycle and progress-tracking machinery of Batch and focuses on
  * straightforward, low-overhead execution.
  */
final class BatchLight[F[_]: Async] private[guard] (scope: MetricScope, batchIdGenerator: AtomicLong) {

  /** Create a sequential batch from named effects. */
  def sequential[A](fas: (String, F[A])*): BatchLight.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new BatchLight.Sequential[F, A](scope, Reader(_ => true), jobs, batchIdGenerator)
  }

  /** Create a parallel batch with an explicit positive parallelism. */
  def parallel[A](parallelism: Int)(fas: (String, F[A])*): BatchLight.Parallel[F, A] = {
    require(parallelism > 0, s"parallelism must be > 0, but was $parallelism")
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new BatchLight.Parallel[F, A](scope, Reader(_ => true), parallelism, jobs, batchIdGenerator)
  }

  def parallel[A](fas: (String, F[A])*): BatchLight.Parallel[F, A] =
    parallel[A](math.max(1, fas.size))(fas*)

  def monadic[A](f: BatchLight.JobBuilder[F] => A): A = {
    val builder = new BatchLight.JobBuilder[F](scope, batchIdGenerator)
    f(builder)
  }
}
