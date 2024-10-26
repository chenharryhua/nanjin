package com.github.chenharryhua.nanjin.guard.action
import cats.Endo
import cats.data.Ior
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.metrics.{NJMetrics, NJRatio}
import io.circe.Json
import io.circe.syntax.EncoderOps

import java.time.Duration
import scala.jdk.DurationConverters.ScalaDurationOps

object BatchRunner {
  sealed abstract class Runner[F[_]: Async, A](
    action: NJAction[F],
    metrics: NJMetrics[F]
  ) {
    protected[this] val F: Async[F] = Async[F]

    protected[this] val nullTransform: A => Json = _ => Json.Null

    protected[this] def tap(f: A => Json): Endo[BuildWith.Builder[F, (BatchJob, F[A]), A]] =
      _.tapInput { case (job, _) =>
        job.asJson
      }.tapOutput { case ((job, _), out) =>
        job.asJson.deepMerge(Json.obj("result" -> f(out)))
      }.tapError { case ((job, _), _) =>
        job.asJson
      }

    private[this] val translator: Ior[Long, Long] => Json = {
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

    protected[this] val ratioGauge: Resource[F, NJRatio[F]] =
      metrics.activeGauge("elapsed", _.enable(action.actionParams.isEnabled)) >>
        metrics.ratio("completion", _.enable(action.actionParams.isEnabled).withTranslator(translator))

    protected def jobTag(job: BatchJob): String = {
      val lead = s"job-${job.index + 1}"
      job.name.fold(lead)(n => s"$lead (${n.value})")
    }
  }

  final class Parallel[F[_]: Async, A] private[action] (
    action: NJAction[F],
    metrics: NJMetrics[F],
    parallelism: Int,
    jobs: List[(Option[BatchJobName], F[A])])
      extends Runner[F, A](action, metrics) {

    def quasi(f: A => Json): F[QuasiResult] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Parallel(parallelism), name, idx, jobs.size) -> fa
      }
      val exec: Resource[F, F[QuasiResult]] = for {
        rat <- ratioGauge.evalTap(_.incDenominator(batchJobs.size.toLong))
        act <- action.retry((_: BatchJob, fa: F[A]) => fa).buildWith(tap(f))
      } yield F
        .timed(F.parTraverseN(parallelism)(batchJobs) { case (job, fa) =>
          F.timed(act.run((job, fa)).attempt)
            .map { case (fd, result) => Detail(job, fd.toJava, result.isRight) }
            .flatTap(_ => rat.incNumerator(1))
        })
        .map { case (fd, details) =>
          QuasiResult(
            name = action.actionParams.actionName,
            spent = fd.toJava,
            mode = BatchMode.Parallel(parallelism),
            details = details.sortBy(_.job.index))
        }

      exec.use(identity)
    }

    val quasi: F[QuasiResult] = quasi(nullTransform)

    def run(f: A => Json): F[List[A]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Parallel(parallelism), name, idx, jobs.size) -> fa
      }

      val exec: Resource[F, F[List[A]]] = for {
        rat <- ratioGauge.evalTap(_.incDenominator(batchJobs.size.toLong))
        act <- action.retry((_: BatchJob, fa: F[A]) => fa).buildWith(tap(f))
      } yield F.parTraverseN(parallelism)(batchJobs) { case (job, fa) =>
        act.run((job, fa)).flatTap(_ => rat.incNumerator(1))
      }

      exec.use(identity)
    }

    val run: F[List[A]] = run(nullTransform)
  }

  final class Sequential[F[_]: Async, A] private[action] (
    action: NJAction[F],
    metrics: NJMetrics[F],
    jobs: List[(Option[BatchJobName], F[A])])
      extends Runner[F, A](action, metrics) {

    def quasi(f: A => Json): F[QuasiResult] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Sequential, name, idx, jobs.size) -> fa
      }

      val exec: Resource[F, F[QuasiResult]] = for {
        rat <- ratioGauge.evalTap(_.incDenominator(batchJobs.size.toLong))
        act <- action.retry((_: BatchJob, fa: F[A]) => fa).buildWith(tap(f))
      } yield batchJobs.traverse { case (job, fa) =>
        metrics
          .activeGauge(jobTag(job))
          .surround(
            F.timed(act.run((job, fa)).attempt)
              .map { case (fd, result) => Detail(job, fd.toJava, result.isRight) }
              .flatTap(_ => rat.incNumerator(1)))
      }.map(details =>
        QuasiResult(
          name = action.actionParams.actionName,
          spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = BatchMode.Sequential,
          details = details.sortBy(_.job.index)
        ))

      exec.use(identity)
    }

    val quasi: F[QuasiResult] = quasi(nullTransform)

    def run(f: A => Json): F[List[A]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Sequential, name, idx, jobs.size) -> fa
      }

      val exec: Resource[F, F[List[A]]] = for {
        rat <- ratioGauge.evalTap(_.incDenominator(batchJobs.size.toLong))
        act <- action.retry((_: BatchJob, fa: F[A]) => fa).buildWith(tap(f))
      } yield batchJobs.traverse { case (job, fa) =>
        metrics.activeGauge(jobTag(job)).surround(act.run((job, fa)).flatTap(_ => rat.incNumerator(1)))
      }

      exec.use(identity)
    }

    val run: F[List[A]] = run(nullTransform)
  }
}

final class NJBatch[F[_]: Async] private[guard] (
  action: NJAction[F],
  metrics: NJMetrics[F]
) {
  def sequential[A](fas: F[A]*): BatchRunner.Sequential[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map(none -> _)
    new BatchRunner.Sequential[F, A](action, metrics, jobs)
  }

  def namedSequential[A](fas: (String, F[A])*): BatchRunner.Sequential[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map { case (name, fa) =>
      BatchJobName(name).some -> fa
    }
    new BatchRunner.Sequential[F, A](action, metrics, jobs)
  }

  def parallel[A](parallelism: Int)(fas: F[A]*): BatchRunner.Parallel[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map(none -> _)
    new BatchRunner.Parallel[F, A](action, metrics, parallelism, jobs)
  }

  def parallel[A](fas: F[A]*): BatchRunner.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

  def namedParallel[A](parallelism: Int)(fas: (String, F[A])*): BatchRunner.Parallel[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map { case (name, fa) =>
      BatchJobName(name).some -> fa
    }
    new BatchRunner.Parallel[F, A](action, metrics, parallelism, jobs)
  }

  def namedParallel[A](fas: (String, F[A])*): BatchRunner.Parallel[F, A] =
    namedParallel[A](fas.size)(fas*)
}
