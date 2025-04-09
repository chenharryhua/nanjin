package com.github.chenharryhua.nanjin.guard.action
import cats.data.{Ior, Kleisli}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import com.github.chenharryhua.nanjin.guard.translator.fmt
import io.circe.Json
import io.circe.syntax.EncoderOps

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object Batch {
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
        val took   = fmt.format(detail.took)
        val result = if (detail.done) took else s"$took (failed)"
        getJobName(detail.job) -> result.asJson
      }
      Json.obj(pairs*)
    }

  private def createMeasure[F[_]](mtx: Metrics[F], size: Int, kind: BatchKind, mode: BatchMode)(implicit
    F: Async[F]): Resource[F, Kleisli[F, Detail, Unit]] =
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

  sealed abstract class Runner[F[_]: Async, A] { outer =>
    protected val F: Async[F] = Async[F]

    protected type DoMeasurement = Kleisli[F, Detail, Unit]

    /** batch always success but jobs may fail
      * @return
      */
    def quasi: Resource[F, QuasiResult]

    /** any job's failure will cause whole batch's failure
      * @return
      */
    def fully: Resource[F, List[A]]

  }

  final class Parallel[F[_]: Async, A] private[action] (
    metrics: Metrics[F],
    parallelism: Int,
    jobs: List[(BatchJob, F[A])])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasi: Resource[F, QuasiResult] = {

      def exec(meas: DoMeasurement): F[(FiniteDuration, List[Detail])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          F.timed(fa.attempt).flatMap { case (fd, result) =>
            val detail = Detail(job, fd.toJava, result.isRight)
            meas.run(detail).as(detail)
          }
        })

      for {
        meas <- createMeasure(metrics, jobs.size, BatchKind.Quasi, mode)
        case (fd, details) <- Resource.eval(exec(meas))
      } yield QuasiResult(metrics.metricLabel, fd.toJava, mode, details.sortBy(_.job.index))
    }

    override def fully: Resource[F, List[A]] = {

      def exec(meas: DoMeasurement): F[List[A]] =
        F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          F.timed(fa).flatMap { case (fd, a) =>
            val detail = Detail(job, fd.toJava, done = true)
            meas.run(detail).as(a)
          }
        }

      createMeasure(metrics, jobs.size, BatchKind.Fully, mode).evalMap(exec)
    }
  }

  final class Sequential[F[_]: Async, A] private[action] (metrics: Metrics[F], jobs: List[(BatchJob, F[A])])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Sequential

    override def quasi: Resource[F, QuasiResult] = {

      def exec(meas: DoMeasurement): F[List[Detail]] =
        jobs.traverse { case (job, fa) =>
          F.timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(fa.attempt)).flatMap {
            case (fd, result) =>
              val detail = Detail(job, fd.toJava, result.isRight)
              meas.run(detail).as(detail)
          }
        }

      for {
        meas <- createMeasure(metrics, jobs.size, BatchKind.Quasi, mode)
        details <- Resource.eval(exec(meas))
      } yield QuasiResult(
        label = metrics.metricLabel,
        spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
        mode = mode,
        details = details.sortBy(_.job.index)
      )
    }

    override def fully: Resource[F, List[A]] = {

      def exec(meas: DoMeasurement): F[List[A]] =
        jobs.traverse { case (job, fa) =>
          F.timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(fa)).flatMap { case (fd, a) =>
            val detail = Detail(job, fd.toJava, done = true)
            meas.run(detail).as(a)
          }
        }

      createMeasure(metrics, jobs.size, BatchKind.Fully, mode).evalMap(exec)
    }
  }

  final class Single[F[_]: Async, A](
    metrics: Metrics[F],
    rfa: Kleisli[F, Kleisli[F, Detail, Unit], (Either[Throwable, A], List[Detail])],
    index: Int) {
    private val F = Async[F]

    private def build[B](name: Option[String])(f: A => F[B]): Single[F, B] = {
      val runB: Kleisli[F, Kleisli[F, Detail, Unit], (Either[Throwable, B], List[Detail])] =
        rfa.tapWithF[(Either[Throwable, B], List[Detail]), Kleisli[F, Detail, Unit]] {
          case (m, (ea: Either[Throwable, A], details: List[Detail])) =>
            val job = BatchJob(name, index)
            ea match {
              case Left(ex) => // escape the rest when job failed
                val detail = Detail(job, Duration.ZERO, done = false)
                m.run(detail).as((ex.asLeft[B], details.appended(detail)))
              case Right(a) => // previous job run smoothly
                F.timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(f(a).attempt)).flatMap {
                  case (fd: FiniteDuration, eb: Either[Throwable, B]) =>
                    eb match {
                      case ex @ Left(_) => // fail current job
                        val detail = Detail(job, fd.toJava, done = false)
                        m.run(detail).as((ex, details.appended(detail)))
                      case b @ Right(_) => // all good
                        val detail = Detail(job, fd.toJava, done = true)
                        m.run(detail).as((b, details.appended(detail)))
                    }
                }
            }
        }
      new Single[F, B](metrics, runB, index + 1)
    }

    def nextJob[B](f: A => F[B]): Single[F, B]               = build[B](None)(f)
    def nextJob[B](name: String, f: A => F[B]): Single[F, B] = build[B](Some(name))(f)
    def nextJob[B](fb: F[B]): Single[F, B]                   = build[B](None)(_ => fb)
    def nextJob[B](name: String, fb: F[B]): Single[F, B]     = build[B](Some(name))(_ => fb)
    def nextJob[B](tup: (String, F[B])): Single[F, B]        = build[B](Some(tup._1))(_ => tup._2)

    def fully: Resource[F, A] =
      createMeasure(metrics, index - 1, BatchKind.Fully, BatchMode.Sequential)
        .evalMap(rfa.run(_).map(_._1).rethrow)

    def quasi: Resource[F, QuasiResult] =
      createMeasure(metrics, index - 1, BatchKind.Quasi, BatchMode.Sequential).evalMap(rfa.run(_).map { res =>
        val details: List[Detail] = res._2
        QuasiResult(
          label = metrics.metricLabel,
          spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = BatchMode.Sequential,
          details = details
        )
      })
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

  /** @param initial
    *   initial value of the sequence.
    *
    * measured with the closest nextJob
    */
  def single[A](initial: F[A]): Batch.Single[F, A] =
    new Batch.Single[F, A](metrics, Kleisli(_ => initial.map(fa => (Right(fa), List.empty))), 1)
  val single: Batch.Single[F, Unit] = single[Unit](Async[F].unit)
}
