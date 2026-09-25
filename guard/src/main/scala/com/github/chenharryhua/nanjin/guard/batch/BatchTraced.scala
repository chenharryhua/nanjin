package com.github.chenharryhua.nanjin.guard.batch

import cats.Applicative
import cats.data.{Kleisli, StateT}
import cats.effect.Temporal
import cats.effect.kernel.Async
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.either.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.monadError.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import org.typelevel.otel4s.trace.Span

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object BatchTraced:

  /*
   * Runners
   */

  sealed abstract protected class BatchRunner[F[_], A](using F: Temporal[F]) {

    protected def mode: BatchMode

    protected def scope: MetricScope

    protected def jobs: List[JobNameIndex[F, A]]

    protected def batchIdGenerator: AtomicLong

    protected def executor: JobExecutor[F, A]

    protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]]

    protected def batchTracer: BatchTracer[F]

    private def nextBatchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())

    def withPostCondition(f: A => Boolean): BatchRunner[F, A]

    final def quasiBatch: F[QuasiBatch[A]] = batchTracer.parent.surround {
      val batchId: BatchId = nextBatchId
      F.timed(traverseJobs(executor.quasiJob(_, batchId).compute)).map {
        case (fd: FiniteDuration, js: List[JobState[A]]) =>
          QuasiBatch(scope = scope, spent = fd.toJava, mode = mode, batchId = batchId, outcomes = js)
      }
    }

    final def valueBatch: F[ValueBatch[A]] = batchTracer.parent.surround {
      val batchId: BatchId = nextBatchId
      F.timed(traverseJobs { jni =>
        executor.valueJob(jni, batchId)
          .compute
          .map(js => js.result.map(JobValue(js.record, _)))
          .rethrow
      }).map { case (fd: FiniteDuration, jv: List[JobValue[A]]) =>
        ValueBatch(
          scope = scope,
          spent = fd.toJava,
          mode = mode,
          batchId = batchId,
          outcomes = jv.map(v => JobState(v.record, Right(v.result))),
          result = jv.map(_.result))
      }
    }
  }

  /*
   * Parallel
   */
  final class Parallel[F[_], A] private[BatchTraced] (
    predicate: A => Boolean,
    protected val scope: MetricScope,
    parallelism: Int,
    protected val jobs: List[JobNameIndex[F, A]],
    protected val batchIdGenerator: AtomicLong,
    protected val batchTracer: BatchTracer[F]
  )(using F: Async[F])
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Parallel(parallelism)
    override protected val executor: JobExecutor[F, A] =
      JobExecutor[F, A](predicate = predicate, mode = mode, scope = scope, log = None)

    override protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]] =
      F.parTraverseN(parallelism)(jobs)(f)

    override def withPostCondition(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](predicate = f, scope, parallelism, jobs, batchIdGenerator, batchTracer)
  }

  /*
   * Sequential
   */
  final class Sequential[F[_], A] private[BatchTraced] (
    predicate: A => Boolean,
    protected val scope: MetricScope,
    protected val jobs: List[JobNameIndex[F, A]],
    protected val batchIdGenerator: AtomicLong,
    protected val batchTracer: BatchTracer[F])(using F: Temporal[F])
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Sequential
    override protected val executor: JobExecutor[F, A] =
      JobExecutor[F, A](predicate = predicate, mode = mode, scope = scope, log = None)

    override protected def traverseJobs[B](f: JobNameIndex[F, A] => F[B]): F[List[B]] =
      jobs.traverse(f)

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Sequential[F, A](predicate = f, scope, jobs, batchIdGenerator, batchTracer)
  }

  /*
   * Monadic
   */

  final class JobBuilder[F[_]] private[BatchTraced] (
    val scope: MetricScope,
    val batchIdGenerator: AtomicLong,
    batchTracer: BatchTracer[F])(using F: Temporal[F]):

    final class Monadic[A] private[BatchTraced] (
      private val kleisli: Kleisli[StateT[F, JobCursor, *], BatchId, ExecutionState[A]]):

      def flatMap[B](f: A => Monadic[B]): Monadic[B] = {
        val runB: Kleisli[StateT[F, JobCursor, *], BatchId, ExecutionState[B]] =
          Kleisli { (batchId: BatchId) =>
            StateT { (cursor: JobCursor) =>
              kleisli(batchId).run(cursor).flatMap {
                case (nextCursor: JobCursor, execState: ExecutionState[A]) =>
                  execState.eoa match {
                    case Left(ex) => (nextCursor -> execState.update[B](ex)).pure[F]
                    case Right(a) =>
                      f(a).kleisli(batchId).run(nextCursor).map {
                        case (finalCursor: JobCursor, nextState: ExecutionState[B]) =>
                          finalCursor -> execState.prependHistory[B](nextState)
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
          Kleisli { (batchId: BatchId) =>
            kleisli(batchId).map { case unchange @ ExecutionState(eoa, history) =>
              eoa match {
                case Left(_)      => unchange
                case Right(value) =>
                  if (f(value))
                    unchange
                  else {
                    val err = PostConditionUnsatisfied(history.headOption.map(_.record.job))
                    ExecutionState[A](Left(err), history)
                  }
              }
            }
          }
        )

      def monadicBatch: F[MonadicBatch[A]] = {
        val batchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())
        val mb = for {
          start <- F.monotonic
          (_, ExecutionState(eoa, history)) <- kleisli(batchId).run(JobCursor(1, start))
          end <- F.monotonic
        } yield MonadicBatch(
          scope = scope,
          spent = (end - start).toJava,
          batchId = batchId,
          outcomes = history.reverse,
          result = eoa)
        batchTracer.parent.surround(mb)
      }
    end Monadic
    object Monadic:
      given Applicative[Monadic] with
        override def pure[A](a: A): Monadic[A] = JobBuilder.this.pure(a)
        override def ap[A, B](ff: Monadic[A => B])(fa: Monadic[A]): Monadic[B] =
          ff.flatMap(fa.map)
      end given
    end Monadic

    def pure[A](a: A): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor => (cursor -> ExecutionState(Right(a), Nil)).pure[F])
      })
    def untracked[A](fa: F[A]): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor =>
          fa.attempt.map(a => cursor -> ExecutionState(a.leftMap(UntrackedStepException(_)), Nil)))
      })
    private def create[A](name: String, f: Span[F] => F[A], predicate: A => Boolean): Monadic[A] =
      new Monadic[A](
        Kleisli { (batchId: BatchId) =>
          StateT { case JobCursor(index: Int, start: FiniteDuration) =>
            val job: Job =
              Job(
                name = name,
                index = index,
                scope = scope,
                mode = BatchMode.Monadic,
                kind = None,
                batchId = batchId)

            for {
              eoa <- batchTracer.tracer.span(name).use(f).attempt
              end <- Temporal[F].monotonic
            } yield {
              val succeeded = eoa.fold(_ => false, predicate)
              val completed = JobState(JobRecord(job, start, end, succeeded), eoa.as(()))
              JobCursor(index + 1, end) -> ExecutionState(eoa = eoa, history = List(completed))
            }
          }
        }
      )
    def apply[A](name: String, fa: F[A]): Monadic[A] = create[A](name, _ => fa, _ => true)
    def apply[A](name: String, f: Span[F] => F[A]): Monadic[A] = create[A](name, f, _ => true)
    def apply[A](name: String, fa: F[A], predicate: A => Boolean): Monadic[A] =
      create[A](name, _ => fa, predicate)
    def apply[A](name: String, f: Span[F] => F[A], predicate: A => Boolean): Monadic[A] =
      create[A](name, f, predicate)

  end JobBuilder
end BatchTraced

final class BatchTraced[F[_]: Async] private[guard] (
  scope: MetricScope,
  batchIdGenerator: AtomicLong,
  batchTracer: BatchTracer[F]):

  def sequential[A](fas: (String, Span[F] => F[A])*): BatchTraced.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, f), idx) =>
      JobNameIndex[F, A](name, idx + 1, batchTracer.tracer.span(name).use(f))
    }
    new BatchTraced.Sequential[F, A](_ => true, scope, jobs, batchIdGenerator, batchTracer)
  }

  def parallel[A](parallelism: Int)(fas: (String, Span[F] => F[A])*): BatchTraced.Parallel[F, A] = {
    require(parallelism > 0, s"parallelism must be > 0, but was $parallelism")
    val jobs = fas.toList.zipWithIndex.map { case ((name, f), idx) =>
      JobNameIndex[F, A](name, idx + 1, batchTracer.tracer.span(name).use(f))
    }
    new BatchTraced.Parallel[F, A](_ => true, scope, parallelism, jobs, batchIdGenerator, batchTracer)
  }

  def parallel[A](fas: (String, Span[F] => F[A])*): BatchTraced.Parallel[F, A] =
    parallel[A](math.max(1, fas.size))(fas*)

  def monadic[A](f: BatchTraced.JobBuilder[F] => A): A = {
    val builder = new BatchTraced.JobBuilder[F](scope, batchIdGenerator, batchTracer)
    f(builder)
  }
end BatchTraced
