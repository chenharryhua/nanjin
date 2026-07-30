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
import cats.{Applicative, Endo, MonadThrow}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.event.MetricLabel
import com.github.chenharryhua.nanjin.guard.metrics.MetricsHub
import com.github.chenharryhua.nanjin.guard.metrics.gauges.ActiveGauge
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.Monocle.focus

import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** Primary API for structured batch execution with lifecycle hooks, metrics, and observable progress. */
object Batch {
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
          BigDecimal(a * 100.0 / b).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
        Json.fromString(s"$rounded% ($expression)")
      }
  }

  private def toJson(results: List[CompletedJob]): Json =
    if (results.isEmpty) Json.Null
    else {
      val pairs: List[(String, Json)] = results.sortBy(_.job.index).map { (cj: CompletedJob) =>
        val took: String = defaultFormatter.format(cj.took)
        val result: String = if (cj.done) took else s"$took (failed)"
        cj.job.displayName -> result.asJson
      }
      Json.obj(pairs*)
    }

  private type UpdatePanel[F[_]] = Kleisli[F, CompletedJob, Unit]

  final private case class BatchMetrics[F[_]](updatePanel: UpdatePanel[F], activeGauge: ActiveGauge[F])

  private def createPanel[F[_]](mtx: MetricsHub[F], size: Int, kind: BatchKind, mode: BatchMode)(using
    F: Async[F]): Resource[F, BatchMetrics[F]] =
    for {
      active <- mtx.activeGauge("active")
      percentile <- mtx
        .percentile(show"$mode $kind completion", _.withTranslator(translator))
        .evalTap(_.incDenominator(size.toLong))
      progress <- Resource.eval(F.ref[List[CompletedJob]](Nil))
      _ <- mtx.gauge("completed jobs", _.register(progress.get.map(toJson)))
    } yield BatchMetrics(
      Kleisli { (cj: CompletedJob) =>
        F.uncancelable(_ => percentile.incNumerator(1) *> progress.update(_.appended(cj)))
      },
      active)

  private def createPanel[F[_]](mtx: MetricsHub[F])(using F: Async[F]): Resource[F, BatchMetrics[F]] =
    for {
      active <- mtx.activeGauge("active")
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
      completed = _.flatMap(js => updatePanel.run(js.completed) *> jobHook.completed(js))
    )

  private class JobExecutor[F[_]: Temporal, A](
    mode: BatchMode,
    jobHook: JobHook[F, A],
    metricLabel: MetricLabel,
    batchId: UUID,
    batchPanel: BatchMetrics[F],
    predicate: Reader[A, Boolean]) {

    private def batchJob(jni: JobNameIndex[F, A], kind: BatchKind) =
      Job(jni.name, jni.index, metricLabel, mode, kind, batchId)

    def runValue(jni: JobNameIndex[F, A]): F[JobValue[A]] = {
      val job: Job = batchJob(jni, BatchKind.Value)
      jobHook.kickoff(job) *>
        jni.fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
          val result: Either[Throwable, A] =
            eoa.flatMap(a => if predicate.run(a) then Right(a) else Left(PostConditionUnsatisfied(job)))
          JobState(CompletedJob(job, fd.toJava, result.isRight), result)
        }.guaranteeCase(handleOutcome(job, jobHook, batchPanel.updatePanel))
          .flatMap(js =>
            js.result match {
              case Left(ex)     => ex.raiseError[F, JobValue[A]]
              case Right(value) => JobValue(js.completed, value).pure[F]
            })
    }

    def runQuasi(jni: JobNameIndex[F, A]): F[JobState[A]] = {
      val job: Job = batchJob(jni, BatchKind.Quasi)
      jobHook.kickoff(job) *>
        jni.fa.attempt.timed.map { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
          val result: Either[Throwable, A] =
            eoa.flatMap(a => if predicate.run(a) then Right(a) else Left(PostConditionUnsatisfied(job)))

          JobState(CompletedJob(job, fd.toJava, result.isRight), result)
        }.guaranteeCase(handleOutcome(job, jobHook, batchPanel.updatePanel))
    }
  }

  /*
   * Runners
   */

  sealed abstract protected class BatchRunner[F[_], A] { outer =>

    /** rename the job names by apply f
      */
    def withJobRename(f: Endo[String]): BatchRunner[F, A]

    def withPostCondition(f: A => Boolean): BatchRunner[F, A]

    protected def mode: BatchMode

    /** Exceptions from individual jobs are captured as failed job results, allowing the overall batch to
      * complete and report per-job outcomes.
      *
      * @return
      *   a batch result where each job is marked as done only when it succeeds and satisfies the
      *   post-condition; otherwise it is marked as failed.
      */
    def quasiBatch(jobHook: JobHook[F, A]): Resource[F, QuasiBatch[A]]

    /** Exceptions from individual jobs are propagated, causing the batch operation to fail immediately, and a
      * post-condition failure is reported as `PostConditionUnsatisfied`.
      */
    def batchValue(jobHook: JobHook[F, A]): Resource[F, BatchValue[A]]
  }

  /*
   * Parallel
   */
  final class Parallel[F[_]: Async, A] private[Batch] (
    predicate: Reader[A, Boolean],
    metrics: MetricsHub[F],
    parallelism: Int,
    jobs: List[JobNameIndex[F, A]],
    uuidGenerator: F[UUID])
      extends BatchRunner[F, A] {
    override protected val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasiBatch(jobHook: JobHook[F, A]): Resource[F, QuasiBatch[A]] = {

      def exec(batchPanel: BatchMetrics[F], batchId: UUID): F[(FiniteDuration, List[JobState[A]])] =
        jobs
          .parTraverseN(parallelism) {
            JobExecutor(mode, jobHook, metrics.metricLabel, batchId, batchPanel, predicate).runQuasi
          }
          .timed
          .guarantee(batchPanel.activeGauge.deactivate)

      Resource.eval(uuidGenerator).flatMap { batchId =>
        createPanel(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(bp => exec(bp, batchId)).map {
          case (fd: FiniteDuration, jobs: List[JobState[A]]) =>
            QuasiBatch(
              label = metrics.metricLabel,
              spent = fd.toJava,
              mode = mode,
              batchId = batchId,
              jobs = jobs)
        }
      }
    }

    override def batchValue(jobHook: JobHook[F, A]): Resource[F, BatchValue[A]] = {

      def exec(batchPanel: BatchMetrics[F], batchId: UUID): F[(FiniteDuration, List[JobValue[A]])] =
        jobs
          .parTraverseN(parallelism) {
            JobExecutor(mode, jobHook, metrics.metricLabel, batchId, batchPanel, predicate).runValue
          }
          .timed
          .guarantee(batchPanel.activeGauge.deactivate)

      Resource.eval(uuidGenerator).flatMap { batchId =>
        createPanel(metrics, jobs.size, BatchKind.Value, mode).evalMap(bp => exec(bp, batchId)).map {
          case (fd: FiniteDuration, jobs: List[JobValue[A]]) =>
            BatchValue(
              label = metrics.metricLabel,
              spent = fd.toJava,
              mode = mode,
              batchId = batchId,
              jobs = jobs)
        }
      }
    }

    override def withJobRename(f: String => String): Parallel[F, A] =
      new Batch.Parallel[F, A](
        predicate,
        metrics,
        parallelism,
        jobs.map(_.focus(_.name).modify(f)),
        uuidGenerator)

    override def withPostCondition(f: A => Boolean): Parallel[F, A] =
      new Parallel[F, A](predicate = Reader(f), metrics, parallelism, jobs, uuidGenerator)
  }

  /*
   * Sequential
   */

  final class Sequential[F[_]: Async, A] private[Batch] (
    predicate: Reader[A, Boolean],
    metrics: MetricsHub[F],
    jobs: List[JobNameIndex[F, A]],
    uuidGenerator: F[UUID])
      extends BatchRunner[F, A] {

    override protected val mode: BatchMode = BatchMode.Sequential

    override def quasiBatch(jobHook: JobHook[F, A]): Resource[F, QuasiBatch[A]] = {
      def exec(batchPanel: BatchMetrics[F], batchId: UUID): F[List[JobState[A]]] =
        jobs.traverse {
          JobExecutor(mode, jobHook, metrics.metricLabel, batchId, batchPanel, predicate).runQuasi
        }.guarantee(batchPanel.activeGauge.deactivate)

      Resource.eval(uuidGenerator).flatMap { batchId =>
        createPanel(metrics, jobs.size, BatchKind.Quasi, mode)
          .evalMap(bp => exec(bp, batchId))
          .map(jobs =>
            QuasiBatch(
              label = metrics.metricLabel,
              spent = jobs.map(_.completed.took).foldLeft(Duration.ZERO)(_.plus(_)),
              mode = mode,
              batchId = batchId,
              jobs = jobs
            ))
      }
    }

    override def batchValue(jobHook: JobHook[F, A]): Resource[F, BatchValue[A]] = {

      def exec(batchPanel: BatchMetrics[F], batchId: UUID): F[List[JobValue[A]]] =
        jobs
          .traverse(
            JobExecutor(
              mode = mode,
              jobHook = jobHook,
              metricLabel = metrics.metricLabel,
              batchId = batchId,
              batchPanel = batchPanel,
              predicate = predicate
            ).runValue
          ).guarantee(batchPanel.activeGauge.deactivate)

      Resource.eval(uuidGenerator).flatMap { batchId =>
        createPanel(metrics, jobs.size, BatchKind.Value, mode).evalMap(bp => exec(bp, batchId)).map { jobs =>
          BatchValue(
            label = metrics.metricLabel,
            spent = jobs.map(_.completed.took).foldLeft(Duration.ZERO)(_.plus(_)),
            mode = mode,
            batchId = batchId,
            jobs = jobs)
        }
      }
    }

    override def withJobRename(f: String => String): Sequential[F, A] =
      new Batch.Sequential[F, A](predicate, metrics, jobs.map(_.focus(_.name).modify(f)), uuidGenerator)

    override def withPostCondition(f: A => Boolean): Sequential[F, A] =
      new Batch.Sequential[F, A](predicate = Reader(f), metrics, jobs, uuidGenerator)
  }

  /*
   * Monadic
   */

  final private case class Callbacks[F[_]](
    updatePanel: UpdatePanel[F],
    jobHook: JobHook[F, Json],
    renameJob: Option[Endo[String]],
    batchId: UUID)

  final class JobBuilder[F[_]] private[Batch] (metrics: MetricsHub[F], uuidGenerator: F[UUID])(using
    F: Async[F]):

    private val mode: BatchMode = BatchMode.Monadic

    final class Monadic[A] private[Batch] (
      private val kleisli: Kleisli[StateT[Resource[F, *], Int, *], Callbacks[F], MonadicExecutionState[A]],
      renameJob: Option[Endo[String]],
      uuidGenerator: F[UUID]):
      def withJobRename(f: String => String): Monadic[A] =
        new Monadic[A](kleisli, renameJob = Some(f), uuidGenerator)

      def flatMap[B](f: A => Monadic[B]): Monadic[B] = {
        val runB: Kleisli[StateT[Resource[F, *], Int, *], Callbacks[F], MonadicExecutionState[B]] =
          kleisli.tapWithF { (callbacks: Callbacks[F], jobState: MonadicExecutionState[A]) =>
            jobState.eoa match {
              case Left(ex) =>
                StateT(idx =>
                  Resource.pure[F, MonadicExecutionState[B]](jobState.update[B](ex)).map((idx, _)))
              case Right(a) => f(a).kleisli.run(callbacks).map(jobState.prependHistory[B])
            }
          }
        new Monadic[B](kleisli = runB, renameJob, uuidGenerator)
      }

      def map[B](f: A => B): Monadic[B] = new Monadic[B](kleisli.map(_.map(f)), renameJob, uuidGenerator)

      def withFilter(f: A => Boolean): Monadic[A] =
        new Monadic[A](
          kleisli = kleisli.map { case unchange @ MonadicExecutionState(eoa, history) =>
            eoa match {
              case Left(_)      => unchange
              case Right(value) =>
                if (f(value))
                  unchange
                else
                  MonadicExecutionState[A](Left(PostConditionUnsatisfied(history.head.job)), history)
            }
          },
          renameJob = renameJob,
          uuidGenerator = uuidGenerator
        )

      def monadicBatch(jobHook: JobHook[F, Json]): Resource[F, MonadicBatch[A]] =
        Resource.eval(uuidGenerator).flatMap { batchId =>
          createPanel[F](metrics).flatMap { case BatchMetrics(updatePanel, activeGauge) =>
            kleisli
              .run(Callbacks[F](updatePanel, jobHook, renameJob, batchId))
              .run(1)
              .guarantee(Resource.eval(activeGauge.deactivate))
          }.map { case (_, MonadicExecutionState(eoa, history)) =>
            MonadicBatch(
              label = metrics.metricLabel,
              spent = history.map(_.took).foldLeft(Duration.ZERO)(_.plus(_)),
              batchId = batchId,
              jobs = history,
              result = eoa
            )
          }
        }
    end Monadic

    private def pureMonadic[A](a: A): Monadic[A] =
      new Monadic[A](
        kleisli = Kleisli { _ =>
          StateT(index => Resource.pure(index -> MonadicExecutionState(Right(a), Nil)))
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

    private def handleOutcome[A](
      job: Job,
      jobHook: JobHook[F, Json],
      updatePanel: UpdatePanel[F],
      translate: A => Json)(outcome: Outcome[Resource[F, *], Throwable, JobState[A]]): Resource[F, Unit] =
      outcome match {
        case Outcome.Succeeded(rfa) =>
          rfa.evalMap(js => updatePanel.run(js.completed) *> jobHook.completed(js.map(translate)))
        // Outcome.Errored should be impossible because job effects are wrapped in attempt
        case Outcome.Errored(ex) =>
          Resource.raiseError[F, Unit, Throwable](shouldNeverHappenException(ex))
        case Outcome.Canceled() => Resource.eval(jobHook.canceled(job))
      }

    /** Exceptions from individual jobs are propagated through the monadic result, causing the remainder of
      * the monadic chain to stop at the first failure.
      *
      * @param name
      *   name of the job
      * @param rfa
      *   the job
      */
    def apply[A: Encoder](name: String, rfa: Resource[F, A]): Monadic[A] =
      new Monadic[A](
        kleisli = Kleisli { case Callbacks(updatePanel, jobHook, renameJob, batchId) =>
          StateT { (index: Int) =>
            val job: Job =
              Job(
                name = renameJob.fold(name)(_.apply(name)),
                index = index,
                label = metrics.metricLabel,
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
                (index + 1, MonadicExecutionState(eoa = js.result, history = List(js.completed)))
              }
          }
        },
        renameJob = None,
        uuidGenerator = uuidGenerator
      )

    def apply[A: Encoder](name: String, fa: F[A]): Monadic[A] =
      apply[A](name, Resource.eval(fa))

    /** Exceptions from the job are converted into a failed Boolean result and recorded as false, allowing the
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
        kleisli = Kleisli { case Callbacks(updatePanel, jobHook, renameJob, batchId) =>
          StateT { (index: Int) =>
            val job: Job =
              Job(
                name = renameJob.fold(name)(_.apply(name)),
                index = index,
                label = metrics.metricLabel,
                mode = mode,
                kind = BatchKind.Quasi,
                batchId = batchId)

            rfa
              .preAllocate(jobHook.kickoff(job))
              .attempt
              .timed
              .map { case (fd: FiniteDuration, eoa: Either[Throwable, Boolean]) =>
                val done = eoa.fold(_ => false, identity)
                JobState(CompletedJob(job, fd.toJava, done), eoa) // make throwable visible
              }
              .guaranteeCase(handleOutcome(job, jobHook, updatePanel, Json.fromBoolean))
              .map { js =>
                (
                  index + 1,
                  MonadicExecutionState(eoa = Right(js.completed.done), history = List(js.completed)))
              }
          }
        },
        renameJob = None,
        uuidGenerator = uuidGenerator
      )

    def failSafe(name: String, fa: F[Boolean]): Monadic[Boolean] =
      failSafe(name, Resource.eval(fa))

  end JobBuilder
}

/** Batch is intended for long-running or stateful work where callers want to observe progress, lifecycle
  * events, and richer execution state while jobs are still in flight.
  */
final class Batch[F[_]: Async] private[guard] (metrics: MetricsHub[F], uuidGenerator: F[UUID]) {

  /** Creates a sequential batch from a list of named effects. Jobs run one after another and preserve order.
    */
  def sequential[A](fas: (String, F[A])*): Batch.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) =>
      JobNameIndex[F, A](name, idx + 1, fa)
    }
    new Batch.Sequential[F, A](
      predicate = Reader(_ => true),
      metrics = metrics,
      jobs = jobs,
      uuidGenerator = uuidGenerator)
  }

  /** Creates a parallel batch from a list of named effects using the given parallelism. */
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
      uuidGenerator = uuidGenerator)
  }

  /** Creates a parallel batch with parallelism inferred from the number of jobs. */
  def parallel[A](fas: (String, F[A])*): Batch.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

  /** Builds a monadic batch using a fluent job builder that can sequence values and conditional steps. */
  def monadic[A](f: Batch.JobBuilder[F] => A): A = {
    val builder = new Batch.JobBuilder[F](metrics, uuidGenerator)
    f(builder)
  }
}
