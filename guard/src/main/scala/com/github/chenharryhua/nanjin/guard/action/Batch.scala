package com.github.chenharryhua.nanjin.guard.action
import cats.data.*
import cats.effect.implicits.monadCancelOps
import cats.effect.kernel.{Async, Outcome, Resource}
import cats.implicits.{
  catsSyntaxApplyOps,
  catsSyntaxEq,
  catsSyntaxMonadErrorRethrow,
  showInterpolator,
  toFlatMapOps,
  toFunctorOps,
  toTraverseOps
}
import cats.{Endo, MonadError}
import com.github.chenharryhua.nanjin.guard.config.Domain
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import io.circe.Json
import io.circe.syntax.EncoderOps
import monocle.Monocle.toAppliedFocusOps

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object Batch {

  /*
   * methods
   */

  private def getJobName(job: BatchJob): String =
    s"job-${job.index} (${job.name})"

  private def activeJob[F[_]](mtx: Metrics[F], job: BatchJob): Resource[F, Unit] =
    mtx.activeGauge(s"running ${getJobName(job)}")

  private val translator: Ior[Long, Long] => Json = {
    case Ior.Left(a)  => Json.fromString(s"$a/0")
    case Ior.Right(b) => Json.fromString(s"0/$b")
    case Ior.Both(a, b) =>
      val expression = s"$a/$b"
      if (b === 0) { Json.fromString(expression) }
      else {
        val rounded: Float =
          BigDecimal(a * 100.0 / b).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
        Json.fromString(s"$rounded% ($expression)")
      }
  }

  private def toJson(results: List[JobResult]): Json =
    if (results.isEmpty) Json.Null
    else {
      val pairs: List[(String, Json)] = results.map { detail =>
        val took   = durationFormatter.format(detail.took)
        val result = if (detail.done) took else s"$took (failed)"
        getJobName(detail.job) -> result.asJson
      }
      Json.obj(pairs*)
    }

  private type DoMeasurement[F[_]] = Kleisli[F, JobResult, Unit]

  private def createMeasure[F[_]](mtx: Metrics[F], size: Int, kind: BatchKind, mode: BatchMode)(implicit
    F: Async[F]): Resource[F, DoMeasurement[F]] =
    for {
      _ <- mtx.activeGauge("elapsed")
      percentile <- mtx
        .percentile(show"$mode completion", _.withTranslator(translator))
        .evalTap(_.incDenominator(size.toLong))
      progress <- Resource.eval(F.ref[List[JobResult]](Nil))
      _ <- mtx.gauge(show"$kind completed").register(progress.get.map(toJson))
    } yield Kleisli { (detail: JobResult) =>
      F.uncancelable(_ => percentile.incNumerator(1) *> progress.update(_.appended(detail)))
    }

  private def createMeasure[F[_]](mtx: Metrics[F], kind: BatchKind)(implicit
    F: Async[F]): Resource[F, DoMeasurement[F]] =
    for {
      _ <- mtx.activeGauge("elapsed")
      progress <- Resource.eval(F.ref[List[JobResult]](Nil))
      _ <- mtx.gauge(show"${BatchMode.Sequential} $kind completed").register(progress.get.map(toJson))
    } yield Kleisli((jr: JobResult) => F.uncancelable(_ => progress.update(_.appended(jr))))

  final private case class SingleJobOutcome[A](result: JobResult, eoa: Either[Throwable, A]) {
    def embed: Either[Throwable, (JobResult, A)] = eoa.map((result, _))
    def map[B](f: A => B): SingleJobOutcome[B]   = copy(eoa = eoa.map(f))
  }

  private def handleOutcome[F[_], A](jobId: BatchJobID, handler: HandleJobLifecycle[F, A])(
    outcome: Outcome[F, Throwable, SingleJobOutcome[A]])(implicit F: MonadError[F, Throwable]): F[Unit] =
    outcome.fold(
      canceled = handler.canceled(jobId),
      errored = e => F.raiseError(new Exception("Should never happen!!!", e)),
      completed = _.flatMap { case SingleJobOutcome(result, eoa) =>
        val joc = JobOutcome(jobId, result.took, result.done)
        eoa.fold(handler.errored(joc, _), handler.completed(joc, _))
      }
    )

  /*
   * Runners
   */

  sealed abstract protected class Runner[F[_]: Async, A] { outer =>
    protected val F: Async[F] = Async[F]

    def map[B](f: A => B): Runner[F, B]

    /** rename the job names by apply f
      */
    def renameJobs(f: Endo[String]): Runner[F, A]

    /** Exceptions thrown by individual jobs in the batch are suppressed, allowing the overall execution to
      * continue.
      *
      * @param isSucc:
      *   evaluate job's return value
      * @return
      *   BatchResult a job is
      *
      * done: when the job returns a value of A and isSucc(a) returns true
      *
      * otherwise fail
      */
    def quasiTrace(handler: HandleJobLifecycle[F, A])(isSucc: A => Boolean): Resource[F, BatchResult]

    /** Exceptions thrown by individual jobs in the batch are suppressed, allowing the overall execution to
      * continue.
      */
    final def quasiRun(f: A => Boolean): Resource[F, BatchResult] =
      quasiTrace(HandleJobLifecycle[F, A])(f)

    /** Exceptions thrown by individual jobs in the batch are propagated, causing the process to halt at the
      * point of failure
      */
    def fullyTrace(handler: HandleJobLifecycle[F, A]): Resource[F, (BatchResult, List[A])]

    /** Exceptions thrown by individual jobs in the batch are propagated, causing the process to halt at the
      * point of failure
      */
    final def fullyRun: Resource[F, (BatchResult, List[A])] = fullyTrace(HandleJobLifecycle[F, A])
  }

  /*
   * Parallel
   */
  final class Parallel[F[_], A] private[Batch] (
    metrics: Metrics[F],
    parallelism: Int,
    jobs: List[(BatchJob, F[A])])(implicit F: Async[F])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasiTrace(handler: HandleJobLifecycle[F, A])(
      isSucc: A => Boolean): Resource[F, BatchResult] = {

      def exec(meas: DoMeasurement[F]): F[(FiniteDuration, List[JobResult])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode, BatchKind.Quasi)
          handler.kickoff(jobId) *>
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = JobResult(job, fd.toJava, eoa.fold(_ => false, isSucc(_)))
                meas.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase(handleOutcome(jobId, handler))
              .map(_.result)
        })

      createMeasure(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(exec).map { case (fd, results) =>
        BatchResult(metrics.metricLabel, fd.toJava, mode, BatchKind.Quasi, results.sortBy(_.job.index))
      }
    }

    override def fullyTrace(handler: HandleJobLifecycle[F, A]): Resource[F, (BatchResult, List[A])] = {

      def exec(meas: DoMeasurement[F]): F[(FiniteDuration, List[(JobResult, A)])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode, BatchKind.Fully)
          handler.kickoff(jobId) *>
            F.timed(F.attempt(fa))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = JobResult(job, fd.toJava, done = eoa.isRight)
                meas.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase(handleOutcome(jobId, handler))
              .map(_.embed)
              .rethrow
        })

      createMeasure(metrics, jobs.size, BatchKind.Fully, mode).evalMap(exec).map { case (fd, results) =>
        val sorted = results.sortBy(_._1.job.index)
        val br     = BatchResult(metrics.metricLabel, fd.toJava, mode, BatchKind.Fully, sorted.map(_._1))
        (br, sorted.map(_._2))
      }
    }

    override def map[B](f: A => B): Parallel[F, B] =
      new Parallel[F, B](metrics, parallelism, jobs.map { case (job, fa) => (job, fa.map(f)) })

    override def renameJobs(f: String => String): Parallel[F, A] =
      new Parallel[F, A](metrics, parallelism, jobs.map(_.focus(_._1.name).modify(f)))
  }

  /*
   * Sequential
   */

  final class Sequential[F[_]: Async, A] private[Batch] (metrics: Metrics[F], jobs: List[(BatchJob, F[A])])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Sequential

    private def batchResult(kind: BatchKind)(results: List[JobResult]): BatchResult =
      BatchResult(
        label = metrics.metricLabel,
        spent = results.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
        mode = mode,
        kind = kind,
        results = results
      )

    override def quasiTrace(handler: HandleJobLifecycle[F, A])(
      isSucc: A => Boolean): Resource[F, BatchResult] = {

      def exec(meas: DoMeasurement[F]): F[List[JobResult]] =
        jobs.traverse { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode, BatchKind.Quasi)
          handler.kickoff(jobId) *>
            activeJob(metrics, job)
              .surround(F.timed(F.attempt(fa)))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = JobResult(job, fd.toJava, eoa.fold(_ => false, isSucc(_)))
                meas.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase(handleOutcome(jobId, handler))
              .map(_.result)
        }

      createMeasure(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(exec).map(batchResult(BatchKind.Quasi))
    }

    override def fullyTrace(handler: HandleJobLifecycle[F, A]): Resource[F, (BatchResult, List[A])] = {

      def exec(meas: DoMeasurement[F]): F[List[(JobResult, A)]] =
        jobs.traverse { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode, BatchKind.Fully)

          handler.kickoff(jobId) *>
            activeJob(metrics, job)
              .surround(F.timed(F.attempt(fa)))
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = JobResult(job, fd.toJava, done = eoa.isRight)
                meas.run(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase(handleOutcome(jobId, handler))
              .map(_.embed)
              .rethrow
        }

      createMeasure(metrics, jobs.size, BatchKind.Fully, mode).evalMap(exec).map { results =>
        val br = batchResult(BatchKind.Fully)(results.map(_._1))
        val as = results.map(_._2)
        (br, as)
      }
    }

    override def map[B](f: A => B): Sequential[F, B] =
      new Sequential[F, B](metrics, jobs.map { case (job, fa) => (job, fa.map(f)) })

    override def renameJobs(f: String => String): Sequential[F, A] =
      new Sequential[F, A](metrics, jobs.map(_.focus(_._1.name).modify(f)))
  }

  /*
   * Monadic
   */

  final private case class JobState[A](eoa: Either[Throwable, A], results: NonEmptyList[JobResult]) {
    def update[B](ex: Throwable): JobState[B] = copy(eoa = Left(ex))
    // reversed order
    def update[B](rb: JobState[B]): JobState[B] =
      JobState[B](rb.eoa, rb.results ::: results)

    def map[B](f: A => B): JobState[B] = copy(eoa = eoa.map(f))
  }

  final case class PostConditionUnsatisfied(job: BatchJob, batch: String, domain: Domain) extends Exception(
        s"${getJobName(job)} of batch($batch) in domain(${domain.value}) run successfully but failed post-condition check")

  final private case class Callbacks[F[_]](
    doMeasure: DoMeasurement[F],
    handler: HandleJobLifecycle[F, Unit],
    kind: BatchKind)

  final class JobBuilder[F[_]] private[Batch] (metrics: Metrics[F])(implicit F: Async[F]) {

    /** Exceptions thrown by individual jobs in the batch are propagated, causing the process to halt at the
      * point of failure
      *
      * build a named job
      * @param name
      *   name of the job
      * @param fa
      *   the job
      */
    def apply[A](name: String, fa: F[A]): Monadic[A] =
      new Monadic[A](
        Kleisli { case Callbacks(doMeasure, handler, kind) =>
          StateT { (index: Int) =>
            val job   = BatchJob(name, index)
            val jobId = BatchJobID(job, metrics.metricLabel, BatchMode.Sequential, kind)

            handler.kickoff(jobId) *>
              activeJob(metrics, job)
                .surround(F.timed(F.attempt(fa)))
                .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                  val result = JobResult(job, fd.toJava, eoa.isRight)
                  doMeasure.run(result).as(SingleJobOutcome(result, eoa))
                }
                .guaranteeCase { (oc: Outcome[F, Throwable, SingleJobOutcome[A]]) =>
                  handleOutcome[F, Unit](jobId, handler)(
                    oc.fold(
                      canceled = Outcome.Canceled(),
                      errored = Outcome.Errored(_),
                      completed = sjr => Outcome.Succeeded(sjr.map(_.map(_ => ())))
                    ))
                }
                .map { case SingleJobOutcome(result, eoa) =>
                  (index + 1, JobState(eoa, NonEmptyList.one(result)))
                }
          }
        }
      )

    /** Exceptions thrown by individual jobs in the batch are propagated, causing the process to halt at the
      * point of failure
      *
      * build a named job
      * @param tup
      *   tuple with the first being the name, second the job
      */
    def apply[A](tup: (String, F[A])): Monadic[A] = apply(tup._1, tup._2)

    /** Exceptions thrown during the job are suppressed, and execution proceeds without interruption.
      * @param name
      *   the name of the job
      * @param fa
      *   the job
      * @return
      *   true if no exception occurs and fa is evaluated to be true, otherwise false
      */
    def invincible(name: String, fa: F[Boolean]): Monadic[Boolean] =
      new Monadic[Boolean](
        Kleisli { case Callbacks(doMeasure, handler, kind) =>
          StateT { (index: Int) =>
            val job   = BatchJob(name, index)
            val jobId = BatchJobID(job, metrics.metricLabel, BatchMode.Sequential, kind)

            handler.kickoff(jobId) *>
              activeJob(metrics, job)
                .surround(F.timed(F.attempt(fa)))
                .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, Boolean]) =>
                  val result = JobResult(job, fd.toJava, eoa.fold(_ => false, identity))
                  doMeasure.run(result).as(SingleJobOutcome(result, eoa))
                }
                .guaranteeCase { (oc: Outcome[F, Throwable, SingleJobOutcome[Boolean]]) =>
                  handleOutcome[F, Unit](jobId, handler)(
                    oc.fold(
                      canceled = Outcome.Canceled(),
                      errored = Outcome.Errored(_),
                      completed = sjr => Outcome.Succeeded(sjr.map(_.map(_ => ())))
                    ))
                }
                .map { case SingleJobOutcome(result, eoa) =>
                  (index + 1, JobState(Right(eoa.fold(_ => false, identity)), NonEmptyList.one(result)))
                }
          }
        }
      )

    /** Exceptions thrown during the job are suppressed, and execution proceeds without interruption.
      * @param tup
      *   tuple with the first being the name, second the job
      * @return
      *   true if no exception occurs and fa is evaluated to be true, otherwise false
      */
    def invincible(tup: (String, F[Boolean])): Monadic[Boolean] = invincible(tup._1, tup._2)

    /*
     * dependent type
     */
    final class Monadic[T](
      private val kleisli: Kleisli[StateT[F, Int, *], Callbacks[F], JobState[T]]
    ) {
      def flatMap[B](f: T => Monadic[B]): Monadic[B] = {
        val runB: Kleisli[StateT[F, Int, *], Callbacks[F], JobState[B]] =
          kleisli.tapWithF { (callbacks, ra) =>
            ra.eoa match {
              case Left(ex) => StateT(idx => F.pure(ra.update[B](ex)).map((idx, _)))
              case Right(a) => f(a).kleisli.run(callbacks).map(ra.update[B])
            }
          }
        new Monadic[B](kleisli = runB)
      }

      def map[B](f: T => B): Monadic[B] = new Monadic[B](kleisli.map(_.map(f)))

      def withFilter(f: T => Boolean): Monadic[T] =
        new Monadic[T](
          kleisli.map { case unchange @ JobState(eoa, results) =>
            eoa match {
              case Left(_) => unchange
              case Right(value) =>
                if (f(value)) unchange
                else {
                  val head = results.head.focus(_.done).replace(false)
                  JobState[T](
                    Left(
                      PostConditionUnsatisfied(
                        head.job,
                        metrics.metricLabel.label,
                        metrics.metricLabel.domain)),
                    NonEmptyList.of(head, results.tail*)
                  )
                }
            }
          }
        )

      private def batchResult(kind: BatchKind, nel: NonEmptyList[JobResult]) = {
        val results = nel.toList.reverse
        BatchResult(
          label = metrics.metricLabel,
          spent = results.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = BatchMode.Sequential,
          kind = kind,
          results = results
        )
      }

      def fullyTrace(handler: HandleJobLifecycle[F, Unit]): Resource[F, (BatchResult, T)] =
        createMeasure[F](metrics, BatchKind.Fully).evalMap { meas =>
          kleisli.run(Callbacks[F](meas, handler, BatchKind.Fully)).run(1)
        }.map { case (_, JobState(eoa, results)) =>
          eoa.map(a => (batchResult(BatchKind.Fully, results), a))
        }.rethrow

      def fullyRun: Resource[F, (BatchResult, T)] =
        fullyTrace(HandleJobLifecycle[F, Unit])

      def quasiTrace(handler: HandleJobLifecycle[F, Unit]): Resource[F, BatchResult] =
        createMeasure[F](metrics, BatchKind.Quasi).evalMap { meas =>
          kleisli.run(Callbacks[F](meas, handler, BatchKind.Quasi)).run(1)
        }.map { case (_, JobState(_, results)) => batchResult(BatchKind.Quasi, results) }

      def quasiRun: Resource[F, BatchResult] =
        quasiTrace(HandleJobLifecycle[F, Unit])
    }
  }
}

final class Batch[F[_]: Async] private[guard] (metrics: Metrics[F]) {

  def sequential[A](fas: (String, F[A])*): Batch.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) => BatchJob(name, idx + 1) -> fa }
    new Batch.Sequential[F, A](metrics, jobs)
  }

  def parallel[A](parallelism: Int)(fas: (String, F[A])*): Batch.Parallel[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) => BatchJob(name, idx + 1) -> fa }
    new Batch.Parallel[F, A](metrics, parallelism, jobs)
  }

  def parallel[A](fas: (String, F[A])*): Batch.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

  def monadic[A](f: Batch.JobBuilder[F] => A): A =
    f(new Batch.JobBuilder[F](metrics))
}
