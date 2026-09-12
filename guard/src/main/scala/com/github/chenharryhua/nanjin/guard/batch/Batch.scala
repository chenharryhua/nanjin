package com.github.chenharryhua.nanjin.guard.batch

import cats.Applicative
import cats.data.{Kleisli, Reader, StateT}
import cats.effect.kernel.syntax.concurrent.given
import cats.effect.kernel.{Async, Resource}
import cats.effect.syntax.clock.given
import cats.effect.syntax.monadCancel.given
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.logging.Log
import com.github.chenharryhua.nanjin.guard.metrics.MetricsHub

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** Primary API for structured batch execution with lifecycle logging, metrics, and observable progress. */
object Batch:

  /*
   * Runners
   */

  /** Common runner operations for sequential and parallel batches. */
  sealed abstract protected class BatchRunner[F[_], A] { outer =>

    /** Reject successful values that do not satisfy `f`. */
    def withPostCondition(f: A => Boolean): BatchRunner[F, A]

    protected def mode: BatchMode

    /** Exceptions from individual jobs are captured as failed job results, allowing the overall batch to
      * complete and report per-job outcomes.
      *
      * @return
      *   a batch result where each job is marked as succeeded only when it completes and satisfies the
      *   post-condition; otherwise it is marked as failed.
      */
    def quasiBatch: Resource[F, QuasiBatch[A]]

    /** Exceptions from individual jobs are propagated, causing the batch operation to fail immediately, and a
      * post-condition failure is reported as `PostConditionUnsatisfied`.
      */
    def valueBatch: Resource[F, ValueBatch[A]]
  }

  /*
   * Parallel
   */
  final class Parallel[F[_]: Async, A] private[Batch] (
    predicate: Reader[A, Boolean],
    log: Log[F],
    metrics: MetricsHub[F],
    parallelism: Int,
    jobs: List[JobNameIndex[F, A]],
    batchIdGenerator: AtomicLong)
      extends BatchRunner[F, A] {
    override protected val mode: BatchMode = BatchMode.Parallel(parallelism)

    private val executor: JobExecutor[F, A] =
      JobExecutor[F, A](predicate = predicate, mode = mode, scope = metrics.scope, log = Some(log))

    override def quasiBatch: Resource[F, QuasiBatch[A]] = {

      def exec(batchPanel: BatchMetrics[F], batchId: BatchId): F[(FiniteDuration, List[JobState[A]])] =
        jobs
          .parTraverseN(parallelism) { jni =>
            val ComputeJob(compute, job) = executor.quasiJob(jni, batchId)
            compute.guaranteeCase(handleOutcome(log, job, batchPanel.updatePanel))
          }
          .timed
          .guarantee(batchPanel.activeGauge.deactivate)

      val batchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())

      createPanel(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(bp => exec(bp, batchId)).map {
        case (fd: FiniteDuration, jobs: List[JobState[A]]) =>
          QuasiBatch(scope = metrics.scope, spent = fd.toJava, mode = mode, batchId = batchId, jobs = jobs)
      }
    }

    override def valueBatch: Resource[F, ValueBatch[A]] = {

      def exec(batchPanel: BatchMetrics[F], batchId: BatchId): F[(FiniteDuration, List[JobValue[A]])] =
        jobs
          .parTraverseN(parallelism) { jni =>
            val ComputeJob(compute, job) = executor.valueJob(jni, batchId)
            compute.guaranteeCase(handleOutcome(log, job, batchPanel.updatePanel))
              .flatMap(js =>
                js.result match {
                  case Left(ex)     => ex.raiseError[F, JobValue[A]]
                  case Right(value) => JobValue(js.record, value).pure[F]
                })
          }
          .timed
          .guarantee(batchPanel.activeGauge.deactivate)

      val batchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())
      createPanel(metrics, jobs.size, BatchKind.Value, mode).evalMap(bp => exec(bp, batchId)).map {
        case (fd: FiniteDuration, jobs: List[JobValue[A]]) =>
          ValueBatch(scope = metrics.scope, spent = fd.toJava, mode = mode, batchId = batchId, jobs = jobs)
      }
    }

    override def withPostCondition(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](predicate = Reader(f), log, metrics, parallelism, jobs, batchIdGenerator)
  }

  /*
   * Sequential
   */

  final class Sequential[F[_]: Async, A] private[Batch] (
    predicate: Reader[A, Boolean],
    log: Log[F],
    metrics: MetricsHub[F],
    jobs: List[JobNameIndex[F, A]],
    batchIdGenerator: AtomicLong)
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Sequential

    private val executor: JobExecutor[F, A] =
      JobExecutor[F, A](predicate = predicate, mode = mode, scope = metrics.scope, log = Some(log))

    override def quasiBatch: Resource[F, QuasiBatch[A]] = {
      def exec(batchPanel: BatchMetrics[F], batchId: BatchId): F[(FiniteDuration, List[JobState[A]])] =
        jobs
          .traverse { jni =>
            val ComputeJob(compute, job) = executor.quasiJob(jni, batchId)
            compute.guaranteeCase(handleOutcome(log, job, batchPanel.updatePanel))
          }.timed
          .guarantee(batchPanel.activeGauge.deactivate)

      val batchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())
      createPanel(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(bp => exec(bp, batchId)).map {
        case (fd: FiniteDuration, jobs: List[JobState[A]]) =>
          QuasiBatch(scope = metrics.scope, spent = fd.toJava, mode = mode, batchId = batchId, jobs = jobs)
      }
    }

    override def valueBatch: Resource[F, ValueBatch[A]] = {

      def exec(batchPanel: BatchMetrics[F], batchId: BatchId): F[(FiniteDuration, List[JobValue[A]])] =
        jobs
          .traverse { jni =>
            val ComputeJob(compute, job) = executor.valueJob(jni, batchId)
            compute.guaranteeCase(handleOutcome(log, job, batchPanel.updatePanel))
              .flatMap(js =>
                js.result match {
                  case Left(ex)     => ex.raiseError[F, JobValue[A]]
                  case Right(value) => JobValue(js.record, value).pure[F]
                })
          }
          .timed
          .guarantee(batchPanel.activeGauge.deactivate)

      val batchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())
      createPanel(metrics, jobs.size, BatchKind.Value, mode).evalMap(bp => exec(bp, batchId)).map {
        case (fd: FiniteDuration, jobs: List[JobValue[A]]) =>
          ValueBatch(scope = metrics.scope, spent = fd.toJava, mode = mode, batchId = batchId, jobs = jobs)
      }
    }

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Batch.Sequential[F, A](predicate = Reader(f), log, metrics, jobs, batchIdGenerator)
  }

  /*
   * Monadic
   */

  final private case class Context[F[_]](updatePanel: UpdatePanel[F], log: Log[F], batchId: BatchId)

  /** Builder for monadic batches whose jobs are composed with `map` and `flatMap`. */
  final class JobBuilder[F[_]: Async] private[Batch] (
    log: Log[F],
    metrics: MetricsHub[F],
    batchIdGenerator: AtomicLong):

    private val mode: BatchMode = BatchMode.Monadic

    final class Monadic[A] private[Batch] (
      private val kleisli: Kleisli[StateT[Resource[F, *], JobCursor, *], Context[F], ExecutionState[A]]):

      /** Sequence a dependent monadic job when the previous job succeeds. */
      def flatMap[B](f: A => Monadic[B]): Monadic[B] = {
        val runB: Kleisli[StateT[Resource[F, *], JobCursor, *], Context[F], ExecutionState[B]] =
          kleisli.tapWithF { (ctx: Context[F], execState: ExecutionState[A]) =>
            execState.eoa match {
              case Left(ex) => StateT((cursor: JobCursor) => (cursor -> execState.update[B](ex)).pure)
              case Right(a) => f(a).kleisli(ctx).map(execState.prependHistory[B])
            }
          }
        new Monadic[B](runB)
      }

      /** Transform a successful monadic job value without adding a job. */
      def map[B](f: A => B): Monadic[B] = new Monadic[B](kleisli.map(_.map(f)))

      /** Filter a successful monadic value; a rejected value becomes a failed quasi-job. */
      def withFilter(f: A => Boolean): Monadic[A] =
        new Monadic[A](
          kleisli.map { case unchange @ ExecutionState(eoa, history) =>
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
        )

      /** Execute the monadic batch, reporting lifecycle events through the batch logger as JSON. */
      def monadicBatch: Resource[F, MonadicBatch[A]] = {
        val batchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())
        for {
          BatchMetrics(updatePanel, activeGauge) <- createMonadicPanel[F](metrics)
          start <- Resource.eval(Async[F].monotonic)
          (_, ExecutionState(eoa, history)) <- kleisli
            .run(Context[F](updatePanel, log, batchId))
            .run(JobCursor(1, start))
            .guarantee(Resource.eval(activeGauge.deactivate))
        } yield MonadicBatch(
          scope = metrics.scope,
          spent = history.headOption.map(_.record.end - start).map(_.toJava).getOrElse(Duration.ZERO),
          batchId = batchId,
          jobs = history.reverse,
          result = eoa
        )
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

    /** Add a pure value to the monadic batch without creating a job. */
    def pure[A](a: A): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor => (cursor -> ExecutionState(Right(a), Nil)).pure)
      })

    /** Add an effectful value to the monadic batch without creating a job.
      *
      * The effect is not tracked, timed, or reported. If it fails, the exception propagates uncaught and
      * crashes the batch.
      */
    def untracked[A](fa: F[A]): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor => Resource.eval(fa).map(a => cursor -> ExecutionState(Right(a), Nil)))
      })

    /** Add a resource to the monadic batch without creating a job.
      *
      * The resource is acquired when this step runs and released when the batch's resource scope closes. It
      * is not tracked, timed, or reported. If acquisition fails, the exception propagates uncaught and
      * crashes the batch.
      */
    def untracked[A](ra: Resource[F, A]): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(cursor => ra.map(a => cursor -> ExecutionState(Right(a), Nil)))
      })

    /** Add a named resource-backed value job.
      *
      * Exceptions from individual jobs are propagated through the monadic result, causing the remainder of
      * the monadic chain to stop at the first failure.
      *
      * @param name
      *   name of the job
      * @param rfa
      *   the resource-backed job
      */
    private def create[A](name: String, rfa: Resource[F, A], predicate: Reader[A, Boolean]): Monadic[A] =
      new Monadic[A](
        Kleisli { case Context(updatePanel, log, batchId) =>
          StateT { case JobCursor(index: Int, start: FiniteDuration) =>
            val job: Job =
              Job(
                name = name,
                index = index,
                scope = metrics.scope,
                mode = mode,
                kind = None,
                batchId = batchId)

            val compute = for {
              eoa <- rfa.preAllocate(logKickoff(log, job)).attempt
              end <- Resource.eval(Async[F].monotonic)
            } yield {
              val succeeded = eoa.fold(_ => false, predicate.run)
              JobState(JobRecord(job, start, end, succeeded), eoa)
            }

            compute
              .guaranteeCase(handleOutcomeR(log, job, updatePanel))
              .map { js =>
                JobCursor(index + 1, js.record.end) -> ExecutionState(js.result, List(js.as(())))
              }
          }
        }
      )

    /** Add a named resource-backed job. The job succeeds unless its resource acquisition or effect throws, in
      * which case the exception stops the chain.
      *
      * @param name
      *   name of the job
      * @param rfa
      *   the resource-backed job
      */
    def apply[A](name: String, rfa: Resource[F, A]): Monadic[A] =
      create[A](name, rfa, Reader(_ => true))

    /** Add a named effect-backed job. The job succeeds unless its effect throws, in which case the exception
      * stops the chain.
      */
    def apply[A](name: String, fa: F[A]): Monadic[A] =
      create[A](name, Resource.eval(fa), Reader(_ => true))

    /** Add a named resource-backed job whose success is decided by `predicate`.
      *
      * A rejected value (`predicate` returns false) marks the job as failed in its `JobRecord` but does not
      * stop the chain: the value still flows to later jobs. To reject a value and stop the chain instead, use
      * `withFilter`. A thrown exception is always recorded as failed and stops the chain, regardless of
      * `predicate`.
      *
      * @param name
      *   name of the job
      * @param rfa
      *   the resource-backed job
      * @param predicate
      *   applied to a successful value to decide whether the job counts as succeeded
      */
    def apply[A](name: String, rfa: Resource[F, A], predicate: A => Boolean): Monadic[A] =
      create[A](name, rfa, Reader(predicate))

    /** Add a named effect-backed job whose success is decided by `predicate`.
      *
      * A rejected value (`predicate` returns false) marks the job as failed in its `JobRecord` but does not
      * stop the chain: the value still flows to later jobs. To reject a value and stop the chain instead, use
      * `withFilter`. A thrown exception is always recorded as failed and stops the chain, regardless of
      * `predicate`.
      *
      * @param name
      *   name of the job
      * @param fa
      *   the effect to run
      * @param predicate
      *   applied to a successful value to decide whether the job counts as succeeded
      */
    def apply[A](name: String, fa: F[A], predicate: A => Boolean): Monadic[A] =
      create[A](name, Resource.eval(fa), Reader(predicate))

  end JobBuilder
end Batch

/** Metrics-backed façade for long-running or stateful work.
  *
  * Use `sequential` or `parallel` for independent jobs, and `monadic` when later jobs depend on earlier
  * results. Acquire `quasiBatch` or `valueBatch` with `.use`; both execution styles report progress and
  * lifecycle events.
  */
final class Batch[F[_]: Async] private[guard] (
  log: Log[F],
  metrics: MetricsHub[F],
  batchIdGenerator: AtomicLong) {

  /** Create a sequential batch from named effects; jobs run in input order.
    */
  def sequential[A](fas: (String, F[A])*): Batch.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new Batch.Sequential[F, A](
      predicate = Reader(_ => true),
      log = log,
      metrics = metrics,
      jobs = jobs,
      batchIdGenerator = batchIdGenerator)
  }

  /** Create a parallel batch from named effects using the given parallelism.
    *
    * `parallelism` must be greater than zero.
    */
  def parallel[A](parallelism: Int)(fas: (String, F[A])*): Batch.Parallel[F, A] = {
    require(parallelism > 0, s"parallelism must be > 0, but was $parallelism")
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new Batch.Parallel[F, A](
      predicate = Reader(_ => true),
      log = log,
      metrics = metrics,
      parallelism = parallelism,
      jobs = jobs,
      batchIdGenerator = batchIdGenerator)
  }

  /** Create a parallel batch with parallelism inferred from the job count. */
  def parallel[A](fas: (String, F[A])*): Batch.Parallel[F, A] =
    parallel[A](math.max(1, fas.size))(fas*)

  /** Build a monadic batch using a fluent job builder for dependent steps. */
  def monadic[A](f: Batch.JobBuilder[F] => A): A = {
    val builder = new Batch.JobBuilder[F](log, metrics, batchIdGenerator)
    f(builder)
  }
}
