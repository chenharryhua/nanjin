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
  sealed abstract class Runner[F[_]: Async, A](mtx: Metrics[F]) { outer =>
    protected val F: Async[F] = Async[F]

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

    protected type DoMeasurement = Kleisli[F, Detail, Unit]

    protected def getJobName(job: BatchJob): String = {
      val lead = s"job-${job.index}"
      job.name.fold(lead)(n => s"$lead ($n)")
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

    protected def measure(size: Int, kind: BatchKind, mode: BatchMode): Resource[F, DoMeasurement] =
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

    /** batch always success but jobs may fail
      * @return
      */
    def quasi(implicit ev: A =:= Boolean): Resource[F, List[QuasiResult]]

    /** any job's failure will cause whole batch failure
      * @return
      */
    def fully: Resource[F, List[A]]

    // sequential
    final def seqCombine(that: Runner[F, A]): Runner[F, A] =
      new Runner[F, A](mtx) {
        override def quasi(implicit ev: A =:= Boolean): Resource[F, List[QuasiResult]] =
          for {
            a <- outer.quasi
            b <- that.quasi
          } yield a ::: b

        override val fully: Resource[F, List[A]] =
          for {
            a <- outer.fully
            b <- that.fully
          } yield a ::: b
      }

    // parallel
    final def parCombine(that: Runner[F, A]): Runner[F, A] =
      new Runner[F, A](mtx) {

        override def quasi(implicit ev: A =:= Boolean): Resource[F, List[QuasiResult]] =
          outer.quasi.both(that.quasi).map { case (a, b) => a ::: b }

        override val fully: Resource[F, List[A]] =
          outer.fully.both(that.fully).map { case (a, b) => a ::: b }
      }
  }

  final class Parallel[F[_]: Async, A] private[action] (
    metrics: Metrics[F],
    parallelism: Int,
    jobs: List[(BatchJob, F[A])])
      extends Runner[F, A](metrics) {

    private val mode: BatchMode = BatchMode.Parallel(parallelism)

    override def quasi(implicit ev: A =:= Boolean): Resource[F, List[QuasiResult]] = {

      def exec(meas: DoMeasurement): F[(FiniteDuration, List[Detail])] =
        F.timed(F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          F.timed(fa.attempt).flatMap { case (fd, result) =>
            val detail = Detail(job, fd.toJava, result.fold(_ => false, ev(_)))
            meas.run(detail).as(detail)
          }
        })

      for {
        meas <- measure(jobs.size, BatchKind.Quasi, mode)
        case (fd, details) <- Resource.eval(exec(meas))
      } yield List(QuasiResult(metrics.metricLabel, fd.toJava, mode, details.sortBy(_.job.index)))
    }

    override val fully: Resource[F, List[A]] = {

      def exec(meas: DoMeasurement): F[List[A]] =
        F.parTraverseN(parallelism)(jobs) { case (job, fa) =>
          F.timed(fa).flatMap { case (fd, a) =>
            val detail = Detail(job, fd.toJava, done = true)
            meas.run(detail).as(a)
          }
        }

      measure(jobs.size, BatchKind.Fully, mode).evalMap(exec)
    }
  }

  final class Sequential[F[_]: Async, A] private[action] (metrics: Metrics[F], jobs: List[(BatchJob, F[A])])
      extends Runner[F, A](metrics) {

    private val mode: BatchMode = BatchMode.Sequential

    override def quasi(implicit ev: A =:= Boolean): Resource[F, List[QuasiResult]] = {

      def exec(meas: DoMeasurement): F[List[Detail]] =
        jobs.traverse { case (job, fa) =>
          F.timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(fa.attempt)).flatMap {
            case (fd, result) =>
              val detail = Detail(job, fd.toJava, result.fold(_ => false, ev(_)))
              meas.run(detail).as(detail)
          }
        }

      for {
        meas <- measure(jobs.size, BatchKind.Quasi, mode)
        details <- Resource.eval(exec(meas))
      } yield List(
        QuasiResult(
          label = metrics.metricLabel,
          spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = mode,
          details = details.sortBy(_.job.index)
        ))
    }

    override val fully: Resource[F, List[A]] = {

      def exec(meas: DoMeasurement): F[List[A]] =
        jobs.traverse { case (job, fa) =>
          F.timed(metrics.activeGauge(s"running ${getJobName(job)}").surround(fa)).flatMap { case (fd, a) =>
            val detail = Detail(job, fd.toJava, done = true)
            meas.run(detail).as(a)
          }
        }

      measure(jobs.size, BatchKind.Fully, mode).evalMap(exec)
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
}
