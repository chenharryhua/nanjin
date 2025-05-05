package com.github.chenharryhua.nanjin.guard.action
import cats.MonadError
import cats.data.{Ior, Kleisli, NonEmptyList, StateT}
import cats.effect.implicits.{clockOps, monadCancelOps}
import cats.effect.kernel.{Async, Outcome, Resource}
import cats.syntax.all.*
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

  private def getJobName(job: BatchJob): String = {
    val lead = s"job-${job.index}"
    job.name.fold(lead)(n => s"$lead ($n)")
  }

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

  private def toJson(details: List[Detail]): Json =
    if (details.isEmpty) Json.Null
    else {
      val pairs: List[(String, Json)] = details.map { detail =>
        val took   = durationFormatter.format(detail.took)
        val result = if (detail.done) took else s"$took (failed)"
        getJobName(detail.job) -> result.asJson
      }
      Json.obj(pairs*)
    }

  private type DoMeasurement[F[_]] = Kleisli[F, Detail, Unit]

  private def createMeasure[F[_]](mtx: Metrics[F], size: Int, kind: BatchKind, mode: BatchMode)(implicit
    F: Async[F]): Resource[F, DoMeasurement[F]] =
    for {
      _ <- mtx.activeGauge("elapsed")
      percentile <- mtx
        .percentile(show"$mode completion", _.withTranslator(translator))
        .evalTap(_.incDenominator(size.toLong))
      progress <- Resource.eval(F.ref[List[Detail]](Nil))
      _ <- mtx.gauge(show"$kind completed").register(progress.get.map(toJson))
    } yield Kleisli { (detail: Detail) =>
      F.uncancelable(_ => percentile.incNumerator(1) *> progress.update(_.appended(detail)))
    }

  private def createMeasure[F[_]](mtx: Metrics[F], kind: BatchKind)(implicit
    F: Async[F]): Resource[F, (DoMeasurement[F], Metrics[F])] =
    for {
      _ <- mtx.activeGauge("elapsed")
      progress <- Resource.eval(F.ref[List[Detail]](Nil))
      _ <- mtx.gauge(show"${BatchMode.Sequential} $kind completed").register(progress.get.map(toJson))
    } yield (
      Kleisli { (detail: Detail) =>
        F.uncancelable(_ => progress.update(_.appended(detail)))
      },
      mtx)

  private def handleOutcome[F[_], A](
    job: BatchJob,
    handler: HandleJobOutcome[F, A],
    postcondition: A => Boolean)(oc: Outcome[F, Throwable, (FiniteDuration, Either[Throwable, A])])(implicit
    F: MonadError[F, Throwable]): F[Unit] =
    oc.fold(
      canceled = handler.canceled(job),
      errored = e => F.raiseError(new Exception("Should never happen!!!", e)),
      completed = _.flatMap { case (fd, eoa) =>
        eoa match {
          case Left(ex) => handler.errored(JobTenure(job, fd.toJava, done = false), ex)
          case Right(a) => handler.succeeded(JobTenure(job, fd.toJava, done = postcondition(a)), a)
        }
      }
    )

  /*
   * Runners
   */

  sealed abstract class Runner[F[_]: Async, A] { outer =>
    protected val F: Async[F] = Async[F]

    /** batch always success but jobs may fail
      * @return
      */
    def traceQuasi(handler: HandleJobOutcome[F, A])(f: A => Boolean): Resource[F, BatchResult]
    final def runQuasi(f: A => Boolean): Resource[F, BatchResult] =
      traceQuasi(HandleJobOutcome.noop[F, A])(f)

    /** any job's failure will cause whole batch's failure
      * @return
      */
    def traceFully(handler: HandleJobOutcome[F, A]): Resource[F, (BatchResult, List[A])]
    final def runFully: Resource[F, (BatchResult, List[A])] = traceFully(HandleJobOutcome.noop[F, A])
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

    override def traceQuasi(handler: HandleJobOutcome[F, A])(f: A => Boolean): Resource[F, BatchResult] = {

      def exec(meas: DoMeasurement[F]): F[(FiniteDuration, List[Detail])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          F.timed(fa.attempt).guaranteeCase(handleOutcome(job, handler, postcondition = f)).flatMap {
            case (fd, eoa) =>
              val detail = Detail(job, fd.toJava, eoa.fold(_ => false, f(_)))
              meas.run(detail).as(detail)
          }
        })

      createMeasure(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(exec).map { case (fd, details) =>
        BatchResult(metrics.metricLabel, fd.toJava, mode, details.sortBy(_.job.index))
      }
    }

    override def traceFully(handler: HandleJobOutcome[F, A]): Resource[F, (BatchResult, List[A])] = {

      def exec(meas: DoMeasurement[F]): F[(FiniteDuration, List[(Detail, A)])] =
        F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          F.timed(fa.attempt)
            .guaranteeCase(handleOutcome(job, handler, postcondition = _ => true))
            .flatMap { case (fd, eoa) =>
              val detail = Detail(job, fd.toJava, done = eoa.isRight)
              meas.run(detail).as(eoa.map((detail, _)))
            }
            .rethrow
        }.timed

      createMeasure(metrics, jobs.size, BatchKind.Fully, mode).evalMap(exec).map { case (fd, result) =>
        val sorted = result.sortBy(_._1.job.index)
        val br     = BatchResult(metrics.metricLabel, fd.toJava, mode, sorted.map(_._1))
        (br, sorted.map(_._2))
      }
    }
  }

  /*
   * Sequential
   */

  final class Sequential[F[_]: Async, A] private[Batch] (metrics: Metrics[F], jobs: List[(BatchJob, F[A])])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Sequential

    private def batchResult(details: List[Detail]): BatchResult =
      BatchResult(
        label = metrics.metricLabel,
        spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
        mode = mode,
        details = details
      )

    override def traceQuasi(handler: HandleJobOutcome[F, A])(f: A => Boolean): Resource[F, BatchResult] = {

      def exec(meas: DoMeasurement[F]): F[List[Detail]] =
        jobs.traverse { case (job, fa) =>
          F.timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(fa.attempt))
            .guaranteeCase(handleOutcome(job, handler, postcondition = f))
            .flatMap { case (fd, eoa) =>
              val detail = Detail(job, fd.toJava, eoa.fold(_ => false, f(_)))
              meas.run(detail).as(detail)
            }
        }

      createMeasure(metrics, jobs.size, BatchKind.Quasi, mode).evalMap(exec).map(batchResult)
    }

    override def traceFully(handler: HandleJobOutcome[F, A]): Resource[F, (BatchResult, List[A])] = {

      def exec(meas: DoMeasurement[F]): F[List[(Detail, A)]] =
        jobs.traverse { case (job, fa) =>
          F.timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(fa.attempt))
            .guaranteeCase(handleOutcome(job, handler, postcondition = _ => true))
            .flatMap { case (fd, eoa) =>
              val detail = Detail(job, fd.toJava, done = eoa.isRight)
              meas.run(detail).as(eoa.map((detail, _)))
            }
            .rethrow
        }

      createMeasure(metrics, jobs.size, BatchKind.Fully, mode).evalMap(exec).map { result =>
        val br = batchResult(result.map(_._1))
        val a  = result.map(_._2)
        (br, a)
      }
    }
  }

  /*
   * Monadic
   */

  final private case class JobState[A](result: Either[Throwable, A], details: NonEmptyList[Detail]) {
    def update[B](ex: Throwable): JobState[B] = copy(result = Left(ex))
    // reversed order
    def update[B](rb: JobState[B]): JobState[B] =
      JobState[B](rb.result, rb.details ::: details)
  }

  final class JobBuilder[F[_]] private[Batch] (passby: Metrics[F])(implicit F: Async[F]) {
    private def build[A](name: Option[String], fa: F[A]): Monadic[F, A] =
      new Monadic[F, A](
        Kleisli { case (measure: DoMeasurement[F], handler: HandleJobOutcome[F, Unit], mtx: Metrics[F]) =>
          StateT { (index: Int) =>
            val job = BatchJob(name, index)
            mtx
              .activeGauge(s"running ${getJobName(job)}")
              .surround(fa.attempt.timed)
              .guaranteeCase { (oc: Outcome[F, Throwable, (FiniteDuration, Either[Throwable, A])]) =>
                handleOutcome[F, Unit](job, handler, postcondition = _ => true)(oc.fold(
                  canceled = Outcome.Canceled(),
                  errored = Outcome.Errored(_),
                  completed = (ffe: F[(FiniteDuration, Either[Throwable, A])]) =>
                    Outcome.Succeeded(ffe.map { case (fd, eoa) =>
                      (fd, eoa.map(_ => ()))
                    })
                ))
              }
              .flatMap { case (fd: FiniteDuration, ea: Either[Throwable, A]) =>
                val detail = Detail(job, fd.toJava, ea.isRight)
                measure.run(detail).as(JobState(ea, NonEmptyList.one(detail)))
              }
              .map((index + 1, _))
          }
        },
        passby
      )

    /** build a nameless job
      * @param fa
      *   the job
      */
    def apply[A](fa: F[A]): Monadic[F, A] = build[A](None, fa)

    /** build a named job
      * @param name
      *   name of the job
      * @param fa
      *   the job
      */
    def apply[A](name: String, fa: F[A]): Monadic[F, A] = build[A](name.some, fa)

    /** build a named job
      * @param tup
      *   a tuple with the first being the name, second the job
      */
    def apply[A](tup: (String, F[A])): Monadic[F, A] = build(tup._1.some, tup._2)
  }

  final case class PostConditionUnsatisfied(job: BatchJob)
      extends Exception(s"${getJobName(job)} run successfully but failed post-condition check")

  final class Monadic[F[_]: Async, A] private[Batch] (
    private[Batch] val kleisli: Kleisli[
      StateT[F, Int, *],
      (DoMeasurement[F], HandleJobOutcome[F, Unit], Metrics[F]),
      JobState[A]],
    metrics: Metrics[F]
  ) {
    def flatMap[B](f: A => Monadic[F, B]): Monadic[F, B] = {
      val runB
        : Kleisli[StateT[F, Int, *], (DoMeasurement[F], HandleJobOutcome[F, Unit], Metrics[F]), JobState[B]] =
        kleisli.tapWithF {
          case (measure: (DoMeasurement[F], HandleJobOutcome[F, Unit], Metrics[F]), ra: JobState[A]) =>
            ra.result match {
              case Left(ex) => StateT(idx => ra.update[B](ex).pure[F].map((idx, _)))
              case Right(a) => f(a).kleisli.run(measure).map(ra.update[B])
            }
        }
      new Monadic[F, B](runB, metrics)
    }

    def map[B](f: A => B): Monadic[F, B] =
      new Monadic[F, B](kleisli.map(js => js.copy(result = js.result.map(f))), metrics)

    def withFilter(f: A => Boolean): Monadic[F, A] =
      new Monadic[F, A](
        kleisli.map { (js: JobState[A]) =>
          js.result match {
            case Left(_) => js
            case Right(value) =>
              if (f(value)) js
              else {
                val head = js.details.head.focus(_.done).replace(false)
                JobState[A](
                  Left(PostConditionUnsatisfied(head.job)),
                  NonEmptyList.of(head, js.details.tail*)
                )
              }
          }
        },
        metrics
      )

    private def batchResult(nel: NonEmptyList[Detail]) = {
      val details = nel.toList.reverse
      BatchResult(
        label = metrics.metricLabel,
        spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
        mode = BatchMode.Sequential,
        details = details
      )
    }

    def traceFully(handler: HandleJobOutcome[F, Unit]): Resource[F, (BatchResult, A)] =
      createMeasure(metrics, BatchKind.Fully).evalMap { case (meas, mtx) =>
        kleisli.run((meas, handler, mtx)).run(1)
      }.map { case (_, js) =>
        js.result.map(a => (batchResult(js.details), a))
      }.rethrow

    def runFully: Resource[F, (BatchResult, A)] =
      traceFully(HandleJobOutcome.noop[F, Unit])

    def traceQuasi(handler: HandleJobOutcome[F, Unit]): Resource[F, BatchResult] =
      createMeasure(metrics, BatchKind.Quasi).evalMap { case (meas, mtx) =>
        kleisli.run((meas, handler, mtx)).run(1)
      }.map { case (_, js) =>
        batchResult(js.details)
      }

    def runQuasi: Resource[F, BatchResult] =
      traceQuasi(HandleJobOutcome.noop[F, Unit])
  }
}

final class Batch[F[_]: Async] private[guard] (metrics: Metrics[F]) {
  def sequential[A](fas: F[A]*): Batch.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case (fa, idx) => BatchJob(none, idx + 1) -> fa }
    new Batch.Sequential[F, A](metrics, jobs)
  }

  def namedSequential[A](fas: (String, F[A])*): Batch.Sequential[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) => BatchJob(name.some, idx + 1) -> fa }
    new Batch.Sequential[F, A](metrics, jobs)
  }

  def parallel[A](parallelism: Int)(fas: F[A]*): Batch.Parallel[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case (fa, idx) => BatchJob(none, idx + 1) -> fa }
    new Batch.Parallel[F, A](metrics, parallelism, jobs)
  }

  def parallel[A](fas: F[A]*): Batch.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

  def namedParallel[A](parallelism: Int)(fas: (String, F[A])*): Batch.Parallel[F, A] = {
    val jobs = fas.toList.zipWithIndex.map { case ((name, fa), idx) => BatchJob(name.some, idx + 1) -> fa }
    new Batch.Parallel[F, A](metrics, parallelism, jobs)
  }

  def namedParallel[A](fas: (String, F[A])*): Batch.Parallel[F, A] =
    namedParallel[A](fas.size)(fas*)

  def monadic[A](f: Batch.JobBuilder[F] => A): A =
    f(new Batch.JobBuilder[F](metrics))
}
