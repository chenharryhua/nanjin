package com.github.chenharryhua.nanjin.guard.action
import cats.data.*
import cats.effect.implicits.{clockOps, monadCancelOps}
import cats.effect.kernel.{Async, Outcome, Resource}
import cats.syntax.all.*
import cats.{Endo, MonadError}
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
    F: Async[F]): Resource[F, (DoMeasurement[F], Metrics[F])] =
    for {
      _ <- mtx.activeGauge("elapsed")
      progress <- Resource.eval(F.ref[List[JobResult]](Nil))
      _ <- mtx.gauge(show"${BatchMode.Sequential} $kind completed").register(progress.get.map(toJson))
    } yield (
      Kleisli { (jr: JobResult) =>
        F.uncancelable(_ => progress.update(_.appended(jr)))
      },
      mtx)

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
        val joc = JobOutcome(
          job = jobId,
          took = durationFormatter.format(result.took),
          done = result.done
        )
        eoa.fold(
          handler.errored(joc, _),
          handler.completed(joc, _)
        )
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

    /** exceptions raised by jobs in the batch are swallowed.
      *
      * a job is either
      *
      * done: when the job returns a value of A and isSucc(a) returns true
      *
      * otherwise fail
      *
      * @param isSucc:
      *   evaluate job's return value
      * @return
      */
    def traceQuasi(handler: HandleJobLifecycle[F, A])(isSucc: A => Boolean): Resource[F, BatchResult]

    final def runQuasi(f: A => Boolean): Resource[F, BatchResult] =
      traceQuasi(HandleJobLifecycle[F, A])(f)

    /** @param handler:
      *   final action on job's return state
      * @return
      */
    def traceFully(handler: HandleJobLifecycle[F, A]): Resource[F, (BatchResult, List[A])]
    final def runFully: Resource[F, (BatchResult, List[A])] = traceFully(HandleJobLifecycle[F, A])
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

    override def traceQuasi(handler: HandleJobLifecycle[F, A])(
      isSucc: A => Boolean): Resource[F, BatchResult] = {

      def exec(meas: DoMeasurement[F]): F[(FiniteDuration, List[JobResult])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode)
          handler.kickoff(jobId) *> F
            .timed(fa.attempt)
            .flatMap { case (fd, eoa) =>
              val result = JobResult(job, fd.toJava, eoa.fold(_ => false, isSucc(_)))
              meas.run(result).as(SingleJobOutcome(result, eoa))
            }
            .guaranteeCase(handleOutcome(jobId, handler))
            .map(_.result)
        })

      createMeasure(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(exec).map { case (fd, results) =>
        BatchResult(metrics.metricLabel, fd.toJava, mode, results.sortBy(_.job.index))
      }
    }

    override def traceFully(handler: HandleJobLifecycle[F, A]): Resource[F, (BatchResult, List[A])] = {

      def exec(meas: DoMeasurement[F]): F[(FiniteDuration, List[(JobResult, A)])] =
        F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode)
          handler.kickoff(jobId) *> F
            .timed(fa.attempt)
            .flatMap { case (fd, eoa) =>
              val result = JobResult(job, fd.toJava, done = eoa.isRight)
              meas.run(result).as(SingleJobOutcome(result, eoa))
            }
            .guaranteeCase(handleOutcome(jobId, handler))
            .map(_.embed)
            .rethrow
        }.timed

      createMeasure(metrics, jobs.size, BatchKind.Fully, mode).evalMap(exec).map { case (fd, results) =>
        val sorted = results.sortBy(_._1.job.index)
        val br     = BatchResult(metrics.metricLabel, fd.toJava, mode, sorted.map(_._1))
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

    private def batchResult(results: List[JobResult]): BatchResult =
      BatchResult(
        label = metrics.metricLabel,
        spent = results.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
        mode = mode,
        results = results
      )

    override def traceQuasi(handler: HandleJobLifecycle[F, A])(
      isSucc: A => Boolean): Resource[F, BatchResult] = {

      def exec(meas: DoMeasurement[F]): F[List[JobResult]] =
        jobs.traverse { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode)
          handler.kickoff(jobId) *> F
            .timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(fa.attempt))
            .flatMap { case (fd, eoa) =>
              val result = JobResult(job, fd.toJava, eoa.fold(_ => false, isSucc(_)))
              meas.run(result).as(SingleJobOutcome(result, eoa))
            }
            .guaranteeCase(handleOutcome(jobId, handler))
            .map(_.result)
        }

      createMeasure(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(exec).map(batchResult)
    }

    override def traceFully(handler: HandleJobLifecycle[F, A]): Resource[F, (BatchResult, List[A])] = {

      def exec(meas: DoMeasurement[F]): F[List[(JobResult, A)]] =
        jobs.traverse { case (job, fa) =>
          val jobId = BatchJobID(job, metrics.metricLabel, mode)

          handler.kickoff(jobId) *> F
            .timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(fa.attempt))
            .flatMap { case (fd, eoa) =>
              val result = JobResult(job, fd.toJava, done = eoa.isRight)
              meas.run(result).as(SingleJobOutcome(result, eoa))
            }
            .guaranteeCase(handleOutcome(jobId, handler))
            .map(_.embed)
            .rethrow
        }

      createMeasure(metrics, jobs.size, BatchKind.Fully, mode).evalMap(exec).map { results =>
        val br = batchResult(results.map(_._1))
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
  }

  final private case class Callbacks[F[_]](
    doMeasure: DoMeasurement[F],
    handler: HandleJobLifecycle[F, Unit],
    rename: Endo[String],
    mtx: Metrics[F])

  final class JobBuilder[F[_]] private[Batch] (passby: Metrics[F])(implicit F: Async[F]) {
    private def build[A](name: String, fa: F[A]): Monadic[F, A] =
      new Monadic[F, A](
        Kleisli { (callbacks: Callbacks[F]) =>
          StateT { (index: Int) =>
            val job   = BatchJob(callbacks.rename(name), index)
            val jobId = BatchJobID(job, callbacks.mtx.metricLabel, BatchMode.Sequential)

            callbacks.handler.kickoff(jobId) *> callbacks.mtx
              .activeGauge(s"running ${getJobName(job)}")
              .surround(fa.attempt.timed)
              .flatMap { case (fd: FiniteDuration, eoa: Either[Throwable, A]) =>
                val result = JobResult(job, fd.toJava, eoa.isRight)
                callbacks.doMeasure(result).as(SingleJobOutcome(result, eoa))
              }
              .guaranteeCase { (oc: Outcome[F, Throwable, SingleJobOutcome[A]]) =>
                handleOutcome[F, Unit](jobId, callbacks.handler)(
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
        },
        passby,
        identity
      )

    /** build a named job
      * @param name
      *   name of the job
      * @param fa
      *   the job
      */
    def apply[A](name: String, fa: F[A]): Monadic[F, A] = build[A](name, fa)

    /** build a named job
      * @param tup
      *   a tuple with the first being the name, second the job
      */
    def apply[A](tup: (String, F[A])): Monadic[F, A] = build(tup._1, tup._2)
  }

  final case class PostConditionUnsatisfied(job: BatchJob)
      extends Exception(s"${getJobName(job)} run successfully but failed post-condition check")

  final class Monadic[F[_]: Async, A] private[Batch] (
    private[Batch] val kleisli: Kleisli[StateT[F, Int, *], Callbacks[F], JobState[A]],
    metrics: Metrics[F],
    rename: Endo[String]
  ) {
    def renameJobs(f: String => String): Monadic[F, A] =
      new Monadic[F, A](kleisli, metrics, rename = f)

    def flatMap[B](f: A => Monadic[F, B]): Monadic[F, B] = {
      val runB: Kleisli[StateT[F, Int, *], Callbacks[F], JobState[B]] =
        kleisli.tapWithF { (callbacks, ra) =>
          ra.eoa match {
            case Left(ex) => StateT(idx => ra.update[B](ex).pure[F].map((idx, _)))
            case Right(a) => f(a).kleisli.run(callbacks).map(ra.update[B])
          }
        }
      new Monadic[F, B](kleisli = runB, metrics, rename)
    }

    def map[B](f: A => B): Monadic[F, B] =
      new Monadic[F, B](kleisli.map(js => js.copy(eoa = js.eoa.map(f))), metrics, rename)

    def withFilter(f: A => Boolean): Monadic[F, A] =
      new Monadic[F, A](
        kleisli.map { (js: JobState[A]) =>
          js.eoa match {
            case Left(_) => js
            case Right(value) =>
              if (f(value)) js
              else {
                val head = js.results.head.focus(_.done).replace(false)
                JobState[A](
                  Left(PostConditionUnsatisfied(head.job)),
                  NonEmptyList.of(head, js.results.tail*)
                )
              }
          }
        },
        metrics,
        rename
      )

    private def batchResult(nel: NonEmptyList[JobResult]) = {
      val results = nel.toList.reverse
      BatchResult(
        label = metrics.metricLabel,
        spent = results.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
        mode = BatchMode.Sequential,
        results = results
      )
    }

    def traceFully(handler: HandleJobLifecycle[F, Unit]): Resource[F, (BatchResult, A)] =
      createMeasure(metrics, BatchKind.Fully).evalMap { case (meas, mtx) =>
        kleisli.run(Callbacks(meas, handler, rename, mtx)).run(1)
      }.map { case (_, js) =>
        js.eoa.map(a => (batchResult(js.results), a))
      }.rethrow

    def runFully: Resource[F, (BatchResult, A)] =
      traceFully(HandleJobLifecycle[F, Unit])

    def traceQuasi(handler: HandleJobLifecycle[F, Unit]): Resource[F, BatchResult] =
      createMeasure(metrics, BatchKind.Quasi).evalMap { case (meas, mtx) =>
        kleisli.run(Callbacks(meas, handler, rename, mtx)).run(1)
      }.map { case (_, js) =>
        batchResult(js.results)
      }

    def runQuasi: Resource[F, BatchResult] =
      traceQuasi(HandleJobLifecycle[F, Unit])
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
