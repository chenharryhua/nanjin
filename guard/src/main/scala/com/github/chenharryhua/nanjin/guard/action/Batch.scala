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

object BatchRunner {
  sealed abstract class Runner[F[_]: Async, A](metrics: Metrics[F]) {
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

    protected def measure(size: Int): Resource[F, Kleisli[F, FiniteDuration, Unit]] =
      for {
        _ <- metrics.activeGauge("batch_elapsed")
        c <- metrics
          .ratio("batch_completion", _.withTranslator(translator))
          .evalTap(_.incDenominator(size.toLong))
        t <- metrics.histogram(
          "batch_timer",
          _.withUnit(_.NANOSECONDS).withReservoir(new SlidingWindowReservoir(size)))
      } yield Kleisli((fd: FiniteDuration) => t.update(fd.toNanos) *> c.incNumerator(1))

    protected def jobTag(job: BatchJob): String = {
      val lead = s"job-${job.index + 1}"
      job.name.fold(lead)(n => s"$lead (${n.value})")
    }
  }

  final class Parallel[F[_]: Async, A] private[action] (
    metrics: Metrics[F],
    parallelism: Int,
    jobs: List[(Option[BatchJobName], F[A])])
      extends Runner[F, A](metrics) {

    val quasi: Resource[F, F[QuasiResult]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Parallel(parallelism), name, idx, jobs.size) -> fa
      }
      for {
        rat <- measure(batchJobs.size)
      } yield F
        .timed(F.parTraverseN(parallelism)(batchJobs) { case (job, fa) =>
          F.timed(fa.attempt).flatMap { case (fd, result) =>
            rat.run(fd).as(Detail(job, fd.toJava, result.isRight))
          }
        })
        .map { case (fd, details) =>
          QuasiResult(
            metricName = metrics.metricName,
            spent = fd.toJava,
            mode = BatchMode.Parallel(parallelism),
            details = details.sortBy(_.job.index))
        }
    }

    val run: Resource[F, F[List[A]]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Parallel(parallelism), name, idx, jobs.size) -> fa
      }

      for {
        rat <- measure(batchJobs.size)
      } yield F.parTraverseN(parallelism)(batchJobs) { case (_, fa) =>
        F.timed(fa).flatMap { case (fd, a) => rat.run(fd).as(a) }
      }
    }
  }

  final class Sequential[F[_]: Async, A] private[action] (
    metrics: Metrics[F],
    jobs: List[(Option[BatchJobName], F[A])])
      extends Runner[F, A](metrics) {

    val quasi: Resource[F, F[QuasiResult]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Sequential, name, idx, jobs.size) -> fa
      }

      for {
        rat <- measure(batchJobs.size)
      } yield batchJobs.traverse { case (job, fa) =>
        metrics
          .activeGauge(jobTag(job))
          .surround(F.timed(fa.attempt).flatMap { case (fd, result) =>
            rat.run(fd).as(Detail(job, fd.toJava, result.isRight))
          })
      }.map(details =>
        QuasiResult(
          metricName = metrics.metricName,
          spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = BatchMode.Sequential,
          details = details.sortBy(_.job.index)
        ))
    }

    val run: Resource[F, F[List[A]]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Sequential, name, idx, jobs.size) -> fa
      }

      for {
        rat <- measure(batchJobs.size)
      } yield batchJobs.traverse { case (job, fa) =>
        metrics.activeGauge(jobTag(job)).surround(F.timed(fa)).flatMap { case (fd, a) => rat.run(fd).as(a) }
      }
    }
  }
}

final class Batch[F[_]: Async] private[guard] (metrics: Metrics[F]) {
  def sequential[A](fas: F[A]*): BatchRunner.Sequential[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map(none -> _)
    new BatchRunner.Sequential[F, A](metrics, jobs)
  }

  def namedSequential[A](fas: (String, F[A])*): BatchRunner.Sequential[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map { case (name, fa) =>
      BatchJobName(name).some -> fa
    }
    new BatchRunner.Sequential[F, A](metrics, jobs)
  }

  def parallel[A](parallelism: Int)(fas: F[A]*): BatchRunner.Parallel[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map(none -> _)
    new BatchRunner.Parallel[F, A](metrics, parallelism, jobs)
  }

  def parallel[A](fas: F[A]*): BatchRunner.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

  def namedParallel[A](parallelism: Int)(fas: (String, F[A])*): BatchRunner.Parallel[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map { case (name, fa) =>
      BatchJobName(name).some -> fa
    }
    new BatchRunner.Parallel[F, A](metrics, parallelism, jobs)
  }

  def namedParallel[A](fas: (String, F[A])*): BatchRunner.Parallel[F, A] =
    namedParallel[A](fas.size)(fas*)
}
