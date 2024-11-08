package com.github.chenharryhua.nanjin.guard.action
import cats.data.{Ior, Kleisli}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.codahale.metrics.SlidingWindowReservoir
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import io.circe.Json

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

    protected type DoMeasurement = Kleisli[F, (BatchJob, FiniteDuration), Unit]

    protected def measure(size: Int, mode: String): Resource[F, DoMeasurement] =
      for {
        ratio <- mtx
          .ratio(s"($mode) completion", _.withTranslator(translator))
          .evalTap(_.incDenominator(size.toLong))
        _ <- mtx.activeGauge("elapsed")
        progress <- Resource.eval(F.ref[List[String]](Nil))
        _ <- mtx.gauge("completed").register(progress.get.map(p => s"[${p.mkString(", ")}]"))
        timer <- mtx.histogram(
          "timer",
          _.withUnit(_.NANOSECONDS).withReservoir(new SlidingWindowReservoir(size)))
      } yield Kleisli { case (job: BatchJob, fd: FiniteDuration) =>
        val jobName = job.name.fold(s"job-${job.index + 1}")(_.value)
        timer.update(fd.toNanos) *>
          ratio.incNumerator(1) *>
          progress.update(_.appended(jobName))
      }

    def quasi: Resource[F, List[QuasiResult]]
    def run: Resource[F, List[A]]

    // sequential
    final def seqCombine(that: Runner[F, A]): Runner[F, A] =
      new Runner[F, A](mtx) {
        override val quasi: Resource[F, List[QuasiResult]] =
          for {
            a <- outer.quasi
            b <- that.quasi
          } yield a ::: b

        override val run: Resource[F, List[A]] =
          for {
            a <- outer.run
            b <- that.run
          } yield a ::: b
      }

    // parallel
    final def parCombine(that: Runner[F, A]): Runner[F, A] =
      new Runner[F, A](mtx) {

        override def quasi: Resource[F, List[QuasiResult]] =
          outer.quasi.both(that.quasi).map { case (a, b) => a ::: b }

        override def run: Resource[F, List[A]] =
          outer.run.both(that.run).map { case (a, b) => a ::: b }
      }
  }

  final class Parallel[F[_]: Async, A] private[action] (
    metrics: Metrics[F],
    parallelism: Int,
    jobs: List[(Option[BatchJobName], F[A])])
      extends Runner[F, A](metrics) {

    override val quasi: Resource[F, List[QuasiResult]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Parallel(parallelism), name, idx, jobs.size) -> fa
      }

      def exec(meas: DoMeasurement): F[(FiniteDuration, List[Detail])] =
        F.timed(F.parTraverseN(parallelism)(batchJobs) { case (job, fa) =>
          F.timed(fa.attempt).flatMap { case (fd, result) =>
            meas.run((job, fd)).as(Detail(job, fd.toJava, result.isRight))
          }
        })

      for {
        meas <- measure(batchJobs.size, s"parallel-$parallelism quasi")
        case (fd, details) <- Resource.eval(exec(meas))
      } yield List(
        QuasiResult(
          spent = fd.toJava,
          mode = BatchMode.Parallel(parallelism),
          details = details.sortBy(_.job.index)))
    }

    override val run: Resource[F, List[A]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Parallel(parallelism), name, idx, jobs.size) -> fa
      }

      def exec(meas: DoMeasurement): F[List[A]] =
        F.parTraverseN(parallelism)(batchJobs) { case (job, fa) =>
          F.timed(fa).flatMap { case (fd, a) => meas.run((job, fd)).as(a) }
        }

      measure(batchJobs.size, s"parallel-$parallelism run").evalMap(exec)
    }
  }

  final class Sequential[F[_]: Async, A] private[action] (
    metrics: Metrics[F],
    jobs: List[(Option[BatchJobName], F[A])])
      extends Runner[F, A](metrics) {

    private def getName(job: BatchJob): String = {
      val lead = s"running job-${job.index + 1}"
      job.name.fold(lead)(n => s"$lead (${n.value})")
    }

    override val quasi: Resource[F, List[QuasiResult]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Sequential, name, idx, jobs.size) -> fa
      }

      def exec(meas: DoMeasurement): F[List[Detail]] =
        batchJobs.traverse { case (job, fa) =>
          F.timed(metrics.activeGauge(getName(job)).surround(fa.attempt)).flatMap { case (fd, result) =>
            meas.run((job, fd)).as(Detail(job, fd.toJava, result.isRight))
          }
        }

      for {
        meas <- measure(batchJobs.size, "sequential quasi")
        details <- Resource.eval(exec(meas))
      } yield List(
        QuasiResult(
          spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = BatchMode.Sequential,
          details = details.sortBy(_.job.index)
        ))
    }

    override val run: Resource[F, List[A]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Sequential, name, idx, jobs.size) -> fa
      }

      def exec(meas: DoMeasurement): F[List[A]] =
        batchJobs.traverse { case (job, fa) =>
          F.timed(metrics.activeGauge(getName(job)).surround(fa)).flatMap { case (fd, a) =>
            meas.run((job, fd)).as(a)
          }
        }

      measure(batchJobs.size, "sequential run").evalMap(exec)
    }
  }
}

final class Batch[F[_]: Async] private[guard] (metrics: Metrics[F]) {
  def sequential[A](fas: F[A]*): Batch.Sequential[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map(none -> _)
    new Batch.Sequential[F, A](metrics, jobs)
  }

  def namedSequential[A](fas: (String, F[A])*): Batch.Sequential[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map { case (name, fa) =>
      BatchJobName(name).some -> fa
    }
    new Batch.Sequential[F, A](metrics, jobs)
  }

  def parallel[A](parallelism: Int)(fas: F[A]*): Batch.Parallel[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map(none -> _)
    new Batch.Parallel[F, A](metrics, parallelism, jobs)
  }

  def parallel[A](fas: F[A]*): Batch.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

  def namedParallel[A](parallelism: Int)(fas: (String, F[A])*): Batch.Parallel[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map { case (name, fa) =>
      BatchJobName(name).some -> fa
    }
    new Batch.Parallel[F, A](metrics, parallelism, jobs)
  }

  def namedParallel[A](fas: (String, F[A])*): Batch.Parallel[F, A] =
    namedParallel[A](fas.size)(fas*)
}
