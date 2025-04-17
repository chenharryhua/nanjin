package com.github.chenharryhua.nanjin.guard.action
import cats.data.{Ior, Kleisli, NonEmptyList, StateT}
import cats.effect.implicits.clockOps
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import com.github.chenharryhua.nanjin.guard.translator.fmt
import io.circe.Json
import io.circe.syntax.EncoderOps
import monocle.Monocle.toAppliedFocusOps

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

  sealed abstract class Runner[F[_]: Async, A] { outer =>
    protected val F: Async[F] = Async[F]

    /** batch always success but jobs may fail
      * @return
      */
    def runQuasi(implicit ev: A =:= Boolean): Resource[F, QuasiResult]

    /** any job's failure will cause whole batch's failure
      * @return
      */
    def runFully: Resource[F, List[A]]

  }

  final class Parallel[F[_]: Async, A] private[Batch] (
    metrics: Metrics[F],
    parallelism: Int,
    jobs: List[(BatchJob, F[A])])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def runQuasi(implicit ev: A =:= Boolean): Resource[F, QuasiResult] = {

      def exec(meas: DoMeasurement[F]): F[(FiniteDuration, List[Detail])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          F.timed(fa.attempt).flatMap { case (fd, result) =>
            val detail = Detail(job, fd.toJava, result.fold(_ => false, ev(_)))
            meas.run(detail).as(detail)
          }
        })

      for {
        meas <- createMeasure(metrics, jobs.size, BatchKind.Quasi, mode)
        case (fd, details) <- Resource.eval(exec(meas))
      } yield QuasiResult(metrics.metricLabel, fd.toJava, mode, details.sortBy(_.job.index))
    }

    override def runFully: Resource[F, List[A]] = {

      def exec(meas: DoMeasurement[F]): F[List[A]] =
        F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          F.timed(fa).flatMap { case (fd, a) =>
            val detail = Detail(job, fd.toJava, done = true)
            meas.run(detail).as(a)
          }
        }

      createMeasure(metrics, jobs.size, BatchKind.Fully, mode).evalMap(exec)
    }
  }

  final class Sequential[F[_]: Async, A] private[Batch] (metrics: Metrics[F], jobs: List[(BatchJob, F[A])])
      extends Runner[F, A] {

    private val mode: BatchMode = BatchMode.Sequential

    override def runQuasi(implicit ev: A =:= Boolean): Resource[F, QuasiResult] = {

      def exec(meas: DoMeasurement[F]): F[List[Detail]] =
        jobs.traverse { case (job, fa) =>
          F.timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(fa.attempt)).flatMap {
            case (fd, result) =>
              val detail = Detail(job, fd.toJava, result.fold(_ => false, ev(_)))
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

    override def runFully: Resource[F, List[A]] = {

      def exec(meas: DoMeasurement[F]): F[List[A]] =
        jobs.traverse { case (job, fa) =>
          F.timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(fa)).flatMap { case (fd, a) =>
            val detail = Detail(job, fd.toJava, done = true)
            meas.run(detail).as(a)
          }
        }

      createMeasure(metrics, jobs.size, BatchKind.Fully, mode).evalMap(exec)
    }
  }

  final private case class JobState[A](result: Either[Throwable, A], details: NonEmptyList[Detail]) {
    def update[B](ex: Throwable): JobState[B] = copy(result = Left(ex))
    // reversed order
    def update[B](rb: JobState[B]): JobState[B] =
      JobState[B](rb.result, rb.details ::: details)
  }

  final class JobBuilder[F[_]] private[Batch] (passby: Metrics[F])(implicit F: Async[F]) {
    private def build[A](name: Option[String], fa: F[A]): Monadic[F, A] =
      new Monadic[F, A](
        Kleisli { case (measure: DoMeasurement[F], mtx: Metrics[F]) =>
          StateT { (index: Int) =>
            val job = BatchJob(name, index)
            mtx
              .activeGauge(s"running ${getJobName(job)}")
              .surround(fa.attempt.timed)
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
    def apply[A](name: String, fa: F[A]): Monadic[F, A] = build[A](Some(name), fa)

    /** build a named job
      * @param tup
      *   a tuple with the first being the name, second the job
      */
    def apply[A](tup: (String, F[A])): Monadic[F, A] = build(Some(tup._1), tup._2)
  }

  final class Monadic[F[_]: Async, A] private[Batch] (
    private[Batch] val kleisli: Kleisli[StateT[F, Int, *], (DoMeasurement[F], Metrics[F]), JobState[A]],
    metrics: Metrics[F]
  ) {
    def flatMap[B](f: A => Monadic[F, B]): Monadic[F, B] = {
      val runB: Kleisli[StateT[F, Int, *], (DoMeasurement[F], Metrics[F]), JobState[B]] =
        kleisli.tapWithF { case (measure: (DoMeasurement[F], Metrics[F]), ra: JobState[A]) =>
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
                  Left(new Exception(s"Post-Condition Unsatisfied: ${getJobName(head.job)}")),
                  NonEmptyList.of(head, js.details.tail*)
                )
              }
          }
        },
        metrics
      )

    def runFully: Resource[F, A] =
      createMeasure(metrics, BatchKind.Fully).evalMap {
        kleisli.run(_).run(1).map(_._2.result).rethrow
      }

    def runQuasi: Resource[F, QuasiResult] =
      createMeasure(metrics, BatchKind.Quasi)
        .evalMap(kleisli.run(_).run(1))
        .map(_._2.details.toList.reverse)
        .map { details =>
          QuasiResult(
            label = metrics.metricLabel,
            spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
            mode = BatchMode.Sequential,
            details = details
          )
        }
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
