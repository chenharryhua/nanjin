package com.github.chenharryhua.nanjin.guard.action
import cats.data.Ior
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.metrics.{NJMetrics, NJRatio}
import io.circe.Json

import java.time.Duration
import scala.jdk.DurationConverters.ScalaDurationOps

object BatchRunner {
  sealed abstract class Runner[F[_]: Async, A](metrics: NJMetrics[F]) {
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

    protected val ratioGauge: Resource[F, NJRatio[F]] =
      metrics.activeGauge("elapsed") >> metrics.ratio("completion", _.withTranslator(translator))

    protected def jobTag(job: BatchJob): String = {
      val lead = s"job-${job.index + 1}"
      job.name.fold(lead)(n => s"$lead (${n.value})")
    }
  }

  final class Parallel[F[_]: Async, A] private[action] (
    metrics: NJMetrics[F],
    parallelism: Int,
    jobs: List[(Option[BatchJobName], F[A])])
      extends Runner[F, A](metrics) {

    val quasi: F[QuasiResult] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Parallel(parallelism), name, idx, jobs.size) -> fa
      }
      val exec: Resource[F, F[QuasiResult]] = for {
        rat <- ratioGauge.evalTap(_.incDenominator(batchJobs.size.toLong))
      } yield F
        .timed(F.parTraverseN(parallelism)(batchJobs) { case (job, fa) =>
          F.timed(fa.attempt)
            .map { case (fd, result) => Detail(job, fd.toJava, result.isRight) }
            .flatTap(_ => rat.incNumerator(1))
        })
        .map { case (fd, details) =>
          QuasiResult(
            spent = fd.toJava,
            mode = BatchMode.Parallel(parallelism),
            details = details.sortBy(_.job.index))
        }

      exec.use(identity)
    }

    val run: F[List[A]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Parallel(parallelism), name, idx, jobs.size) -> fa
      }

      val exec: Resource[F, F[List[A]]] = for {
        rat <- ratioGauge.evalTap(_.incDenominator(batchJobs.size.toLong))
      } yield F.parTraverseN(parallelism)(batchJobs) { case (_, fa) =>
        fa.flatTap(_ => rat.incNumerator(1))
      }

      exec.use(identity)
    }

  }

  final class Sequential[F[_]: Async, A] private[action] (
    metrics: NJMetrics[F],
    jobs: List[(Option[BatchJobName], F[A])])
      extends Runner[F, A](metrics) {

    val quasi: F[QuasiResult] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Sequential, name, idx, jobs.size) -> fa
      }

      val exec: Resource[F, F[QuasiResult]] = for {
        rat <- ratioGauge.evalTap(_.incDenominator(batchJobs.size.toLong))
      } yield batchJobs.traverse { case (job, fa) =>
        metrics
          .activeGauge(jobTag(job))
          .surround(
            F.timed(fa.attempt)
              .map { case (fd, result) => Detail(job, fd.toJava, result.isRight) }
              .flatTap(_ => rat.incNumerator(1)))
      }.map(details =>
        QuasiResult(
          spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = BatchMode.Sequential,
          details = details.sortBy(_.job.index)
        ))

      exec.use(identity)
    }

    val run: F[List[A]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Sequential, name, idx, jobs.size) -> fa
      }

      val exec: Resource[F, F[List[A]]] = for {
        rat <- ratioGauge.evalTap(_.incDenominator(batchJobs.size.toLong))
      } yield batchJobs.traverse { case (job, fa) =>
        metrics.activeGauge(jobTag(job)).surround(fa).flatTap(_ => rat.incNumerator(1))
      }

      exec.use(identity)
    }
  }
}

final class NJBatch[F[_]: Async] private[guard] (metrics: NJMetrics[F]) {
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
