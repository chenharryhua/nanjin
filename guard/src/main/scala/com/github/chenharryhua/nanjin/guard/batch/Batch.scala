package com.github.chenharryhua.nanjin.guard.batch

import cats.data.{Ior, Kleisli, Reader, StateT}
import cats.effect.kernel.syntax.concurrent.given
import cats.effect.kernel.{Async, Outcome, Resource, Temporal}
import cats.effect.syntax.clock.given
import cats.effect.syntax.monadCancel.given
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.apply.given
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.show.given
import cats.syntax.traverse.given
import cats.{Applicative, MonadThrow}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.metrics.{MetricScope, MetricsHub}
import com.github.chenharryhua.nanjin.guard.metrics.api.gauges.ActiveGauge
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** Primary API for structured batch execution with lifecycle hooks, metrics, and observable progress. */
object Batch:
  private def shouldNeverHappenException(e: Throwable): Exception =
    new RuntimeException("[Batch internal error] unexpected outcome", e)

  private val translator: Ior[Long, Long] => Json = {
    case Ior.Left(a)    => Json.fromString(s"$a/0")
    case Ior.Right(b)   => Json.fromString(s"0/$b")
    case Ior.Both(a, b) =>
      val expression = s"$a/$b"
      if (b === 0) { Json.fromString(expression) }
      else {
        val rounded: Float =
          BigDecimal(BigInt(a) * 100)./(BigDecimal(b)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
        Json.fromString(s"$rounded% ($expression)")
      }
  }

  private def toJson(results: List[CompletedJob]): Json =
    if (results.isEmpty) Json.Null
    else {
      val pairs: List[(String, Json)] = results.sortBy(_.job.index).map { (cj: CompletedJob) =>
        val took: String = defaultFormatter.format(cj.took)
        val result: String = if (cj.succeeded) took else s"$took (failed)"
        cj.job.displayName -> result.asJson
      }
      Json.obj(pairs*)
    }

  private type UpdatePanel[F[_]] = Kleisli[F, CompletedJob, Unit]

  final private case class BatchMetrics[F[_]](updatePanel: UpdatePanel[F], activeGauge: ActiveGauge[F])

  private def createPanel[F[_]](mtx: MetricsHub[F], size: Int, kind: BatchKind, mode: BatchMode)(using
    F: Async[F]): Resource[F, BatchMetrics[F]] =
    for {
      active <- mtx.activeGauge("Active")
      ratio <- mtx
        .ratio(show"$mode $kind completion", _.withTranslator(translator))
        .evalTap(_.incDenominator(size.toLong))
      progress <- Resource.eval(F.ref[List[CompletedJob]](Nil))
      _ <- mtx.gauge("Completed jobs", _.register(progress.get.map(toJson)))
    } yield BatchMetrics(
      Kleisli { (cj: CompletedJob) =>
        F.uncancelable(_ => ratio.incNumerator(1) *> progress.update(_.appended(cj)))
      },
      active)

  private def createMonadicPanel[F[_]](mtx: MetricsHub[F])(using F: Async[F]): Resource[F, BatchMetrics[F]] =
    for {
      active <- mtx.activeGauge("Active")
      progress <- Resource.eval(F.ref[List[CompletedJob]](Nil))
      _ <- mtx.gauge(show"${BatchMode.Monadic} jobs completed", _.register(progress.get.map(toJson)))
    } yield BatchMetrics(
      Kleisli((cj: CompletedJob) => F.uncancelable(_ => progress.update(_.appended(cj)))),
      active)

  private def handleOutcome[F[_], A](job: Job, jobHook: JobHook[F, A], updatePanel: UpdatePanel[F])(
    outcome: Outcome[F, Throwable, JobState[A]])(using F: MonadThrow[F]): F[Unit] =
    outcome.fold(
      canceled = jobHook.canceled(job),
      // Outcome.Errored should be impossible because job effects are wrapped in attempt
      errored = ex => F.raiseError(shouldNeverHappenException(ex)),
      completed = _.flatMap(js => updatePanel.run(js.record) *> jobHook.completed(js))
    )

  private class JobExecutor[F[_]: Temporal, A](
    mode: BatchMode,
    jobHook: JobHook[F, A],
    scope: MetricScope,
    batchId: BatchId,
    batchPanel: BatchMetrics[F],
    predicate: Reader[A, Boolean]) {

    private def batchJob(jni: JobNameIndex[F, A], kind: BatchKind) =
      Job(jni.name, jni.index, scope, mode, kind, batchId)

    def runValue(jni: JobNameIndex[F, A]): F[JobValue[A]] = {
      val job: Job = batchJob(jni, BatchKind.Value)
      jobHook.kickoff(job) *>
        jni.fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
          val result: Either[Throwable, A] =
            eoa.flatMap { a =>
              if (predicate.run(a))
                Right(a)
              else
                Left(PostConditionUnsatisfied(Some(job)))
            }
          JobState(CompletedJob(job, fd.toJava, result.isRight), result)
        }.guaranteeCase(handleOutcome(job, jobHook, batchPanel.updatePanel))
          .flatMap(js =>
            js.result match {
              case Left(ex)     => ex.raiseError[F, JobValue[A]]
              case Right(value) => JobValue(js.record, value).pure[F]
            })
    }

    def runQuasi(jni: JobNameIndex[F, A]): F[JobState[A]] = {
      val job: Job = batchJob(jni, BatchKind.Quasi)
      jobHook.kickoff(job) *>
        jni.fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
          val result: Either[Throwable, A] =
            eoa.flatMap { a =>
              if (predicate.run(a))
                Right(a)
              else
                Left(PostConditionUnsatisfied(Some(job)))
            }

          JobState(CompletedJob(job, fd.toJava, result.isRight), result)
        }.guaranteeCase(handleOutcome(job, jobHook, batchPanel.updatePanel))
    }
  }

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
    def quasiBatch(jobHook: JobHook[F, A]): Resource[F, QuasiBatch[A]]

    /** Exceptions from individual jobs are propagated, causing the batch operation to fail immediately, and a
      * post-condition failure is reported as `PostConditionUnsatisfied`.
      */
    def valueBatch(jobHook: JobHook[F, A]): Resource[F, ValueBatch[A]]
  }

  /*
   * Parallel
   */
  final class Parallel[F[_]: Async, A] private[Batch] (
    predicate: Reader[A, Boolean],
    metrics: MetricsHub[F],
    parallelism: Int,
    jobs: List[JobNameIndex[F, A]],
    batchIdGenerator: AtomicLong)
      extends BatchRunner[F, A] {
    override protected val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasiBatch(jobHook: JobHook[F, A]): Resource[F, QuasiBatch[A]] = {

      def exec(batchPanel: BatchMetrics[F], batchId: BatchId): F[(FiniteDuration, List[JobState[A]])] =
        jobs
          .parTraverseN(parallelism) {
            JobExecutor(mode, jobHook, metrics.scope, batchId, batchPanel, predicate).runQuasi
          }
          .timed
          .guarantee(batchPanel.activeGauge.deactivate)

      val batchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())
      createPanel(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(bp => exec(bp, batchId)).map {
        case (fd: FiniteDuration, jobs: List[JobState[A]]) =>
          QuasiBatch(scope = metrics.scope, spent = fd.toJava, mode = mode, batchId = batchId, jobs = jobs)
      }
    }

    override def valueBatch(jobHook: JobHook[F, A]): Resource[F, ValueBatch[A]] = {

      def exec(batchPanel: BatchMetrics[F], batchId: BatchId): F[(FiniteDuration, List[JobValue[A]])] =
        jobs
          .parTraverseN(parallelism) {
            JobExecutor(mode, jobHook, metrics.scope, batchId, batchPanel, predicate).runValue
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
      new Parallel[F, A](predicate = Reader(f), metrics, parallelism, jobs, batchIdGenerator)
  }

  /*
   * Sequential
   */

  final class Sequential[F[_]: Async, A] private[Batch] (
    predicate: Reader[A, Boolean],
    metrics: MetricsHub[F],
    jobs: List[JobNameIndex[F, A]],
    batchIdGenerator: AtomicLong)
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Sequential

    override def quasiBatch(jobHook: JobHook[F, A]): Resource[F, QuasiBatch[A]] = {
      def exec(batchPanel: BatchMetrics[F], batchId: BatchId): F[List[JobState[A]]] =
        jobs.traverse {
          JobExecutor(mode, jobHook, metrics.scope, batchId, batchPanel, predicate).runQuasi
        }.guarantee(batchPanel.activeGauge.deactivate)

      val batchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())
      createPanel(metrics, jobs.size, BatchKind.Quasi, mode)
        .evalMap(bp => exec(bp, batchId))
        .map(jobs =>
          QuasiBatch(
            scope = metrics.scope,
            spent = jobs.map(_.record.took).foldLeft(Duration.ZERO)(_.plus(_)),
            mode = mode,
            batchId = batchId,
            jobs = jobs))
    }

    override def valueBatch(jobHook: JobHook[F, A]): Resource[F, ValueBatch[A]] = {

      def exec(batchPanel: BatchMetrics[F], batchId: BatchId): F[List[JobValue[A]]] =
        jobs
          .traverse(
            JobExecutor(
              mode = mode,
              jobHook = jobHook,
              scope = metrics.scope,
              batchId = batchId,
              batchPanel = batchPanel,
              predicate = predicate
            ).runValue
          ).guarantee(batchPanel.activeGauge.deactivate)

      val batchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())
      createPanel(metrics, jobs.size, BatchKind.Value, mode).evalMap(bp => exec(bp, batchId)).map { jobs =>
        ValueBatch(
          scope = metrics.scope,
          spent = jobs.map(_.record.took).foldLeft(Duration.ZERO)(_.plus(_)),
          mode = mode,
          batchId = batchId,
          jobs = jobs)
      }
    }

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Batch.Sequential[F, A](predicate = Reader(f), metrics, jobs, batchIdGenerator)
  }

  /*
   * Monadic
   */

  final private case class Context[F[_]](
    updatePanel: UpdatePanel[F],
    jobHook: JobHook[F, Json],
    batchId: BatchId)

  /** Builder for monadic batches whose jobs are composed with `map` and `flatMap`. */
  final class JobBuilder[F[_]: Async] private[Batch] (metrics: MetricsHub[F], batchIdGenerator: AtomicLong):

    private val mode: BatchMode = BatchMode.Monadic

    final class Monadic[A] private[Batch] (
      private val kleisli: Kleisli[StateT[Resource[F, *], Int, *], Context[F], ExecutionState[A]]):

      /** Sequence a dependent monadic job when the previous job succeeds. */
      def flatMap[B](f: A => Monadic[B]): Monadic[B] = {
        val runB: Kleisli[StateT[Resource[F, *], Int, *], Context[F], ExecutionState[B]] =
          kleisli.tapWithF { (ctx: Context[F], execState: ExecutionState[A]) =>
            execState.eoa match {
              case Left(ex) => StateT((idx: Int) => (idx -> execState.update[B](ex)).pure)
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
                  val err = PostConditionUnsatisfied(history.headOption.map(_.job))
                  ExecutionState[A](Left(err), history)
                }
            }
          }
        )

      /** Execute the monadic batch with lifecycle hooks and JSON job reporting. */
      def monadicBatch(jobHook: JobHook[F, Json]): Resource[F, MonadicBatch[A]] = {
        val batchId: BatchId = BatchId(batchIdGenerator.getAndIncrement())
        createMonadicPanel[F](metrics).flatMap { case BatchMetrics(updatePanel, activeGauge) =>
          kleisli
            .run(Context[F](updatePanel, jobHook, batchId))
            .run(1)
            .guarantee(Resource.eval(activeGauge.deactivate))
        }.map { case (_, ExecutionState(eoa, history)) =>
          MonadicBatch(
            scope = metrics.scope,
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
        StateT(idx => (idx -> ExecutionState(Right(a), Nil)).pure)
      })

    /** Lift an effectful value into the monadic batch without creating a job.
      *
      * The effect is not tracked, timed, or reported. If it fails, the exception propagates uncaught and
      * crashes the batch.
      */
    def lift[A](fa: F[A]): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(idx => Resource.eval(fa).map(a => idx -> ExecutionState(Right(a), Nil)))
      })

    /** Lift a resource into the monadic batch without creating a job.
      *
      * The resource is acquired when this step runs and released when the batch's resource scope closes. It
      * is not tracked, timed, or reported. If acquisition fails, the exception propagates uncaught and
      * crashes the batch.
      */
    def lift[A](ra: Resource[F, A]): Monadic[A] =
      new Monadic[A](Kleisli { _ =>
        StateT(idx => ra.map(a => idx -> ExecutionState(Right(a), Nil)))
      })

    private def handleOutcome[A](
      job: Job,
      jobHook: JobHook[F, Json],
      updatePanel: UpdatePanel[F],
      translate: A => Json)(outcome: Outcome[Resource[F, *], Throwable, JobState[A]]): Resource[F, Unit] =
      outcome match {
        case Outcome.Succeeded(rfa) =>
          rfa.evalMap(js => updatePanel.run(js.record) *> jobHook.completed(js.map(translate)))
        // Outcome.Errored should be impossible because job effects are wrapped in attempt
        case Outcome.Errored(ex) =>
          Resource.raiseError[F, Unit, Throwable](shouldNeverHappenException(ex))
        case Outcome.Canceled() => Resource.eval(jobHook.canceled(job))
      }

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
    def apply[A: Encoder](name: String, rfa: Resource[F, A]): Monadic[A] =
      new Monadic[A](
        Kleisli { case Context(updatePanel, jobHook, batchId) =>
          StateT { (index: Int) =>
            val job: Job =
              Job(
                name = name,
                index = index,
                scope = metrics.scope,
                mode = mode,
                kind = BatchKind.Value,
                batchId = batchId)

            rfa
              .preAllocate(jobHook.kickoff(job))
              .attempt
              .timed
              .map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                JobState(CompletedJob(job, fd.toJava, eoa.isRight), eoa)
              }
              .guaranteeCase(handleOutcome(job, jobHook, updatePanel, Encoder[A].apply))
              .map { js =>
                index + 1 -> ExecutionState(js.result, List(js.record))
              }
          }
        }
      )

    /** Add a named effect-backed value job. */
    def apply[A: Encoder](name: String, fa: F[A]): Monadic[A] =
      apply[A](name, Resource.eval(fa))

    /** Add a resource-backed boolean job whose failure or false result is retained as a quasi failure.
      *
      * Exceptions from the job are converted into a failed Boolean result and recorded as false, allowing the
      * remainder of the monadic chain to continue.
      *
      * @param name
      *   the name of the job
      * @param rfa
      *   the job
      * @return
      *   true only when the job succeeds and evaluates to true; otherwise false
      */
    def failSafe(name: String, rfa: Resource[F, Boolean]): Monadic[Boolean] =
      new Monadic[Boolean](
        Kleisli { case Context(updatePanel, jobHook, batchId) =>
          StateT { (index: Int) =>
            val job: Job =
              Job(
                name = name,
                index = index,
                scope = metrics.scope,
                mode = mode,
                kind = BatchKind.Quasi,
                batchId = batchId)

            rfa
              .preAllocate(jobHook.kickoff(job))
              .attempt
              .timed
              .map { case (fd: FiniteDuration, eoa: Either[Throwable, Boolean]) =>
                val succeeded = eoa.fold(_ => false, identity)
                JobState(CompletedJob(job, fd.toJava, succeeded), eoa) // make throwable visible
              }
              .guaranteeCase(handleOutcome(job, jobHook, updatePanel, Json.fromBoolean))
              .map { js =>
                index + 1 -> ExecutionState(Right(js.record.succeeded), List(js.record))
              }
          }
        }
      )

    /** Add an effect-backed boolean job whose failure or false result is retained as a quasi failure. */
    def failSafe(name: String, fa: F[Boolean]): Monadic[Boolean] =
      failSafe(name, Resource.eval(fa))

  end JobBuilder
end Batch

/** Metrics-backed façade for long-running or stateful work.
  *
  * Use `sequential` or `parallel` for independent jobs, and `monadic` when later jobs depend on earlier
  * results. Acquire `quasiBatch` or `valueBatch` with `.use`; both execution styles report progress and
  * lifecycle events.
  */
final class Batch[F[_]: Async] private[guard] (metrics: MetricsHub[F], batchIdGenerator: AtomicLong) {

  /** Create a sequential batch from named effects; jobs run in input order.
    */
  def sequential[A](fas: (String, F[A])*): Batch.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new Batch.Sequential[F, A](
      predicate = Reader(_ => true),
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
    val builder = new Batch.JobBuilder[F](metrics, batchIdGenerator)
    f(builder)
  }
}
