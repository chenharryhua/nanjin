package com.github.chenharryhua.nanjin.guard.action
import cats.Endo
import cats.effect.kernel.{Async, Resource, Unique}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import io.circe.Json
import io.circe.syntax.EncoderOps

import java.time.Duration
import scala.jdk.DurationConverters.ScalaDurationOps

object BatchRunner {
  sealed abstract class Runner[F[_]: Async, A](
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    action: NJAction[F],
    ratioBuilder: NJRatio.Builder,
    gaugeBuilder: NJGauge.Builder
  ) {
    protected[this] val F: Async[F] = Async[F]

    protected[this] val BatchIdTag: String = QuasiResult.BatchIdTag
    private[this] val ResultTag: String    = "result"

    protected[this] val nullTransform: A => Json = _ => Json.Null

    protected[this] def tap(f: A => Json): Endo[BuildWith.Builder[F, (BatchJob, Unique.Token, F[A]), A]] =
      _.tapInput { case (job, token, _) =>
        job.asJson.deepMerge(Json.obj(BatchIdTag -> token.hash.asJson))
      }.tapOutput { case ((job, token, _), out) =>
        job.asJson.deepMerge(Json.obj(BatchIdTag -> token.hash.asJson, ResultTag -> f(out)).dropNullValues)
      }.tapError { case ((job, token, _), _) =>
        job.asJson.deepMerge(Json.obj(BatchIdTag -> token.hash.asJson))
      }

    protected[this] val ratio: Resource[F, NJRatio[F]] =
      for {
        _ <- gaugeBuilder
          .withMeasurement(action.actionParams.measurement.value)
          .build[F](action.actionParams.actionName.value, metricRegistry, serviceParams)
          .timed
        rat <- ratioBuilder
          .withMeasurement(action.actionParams.measurement.value)
          .build(action.actionParams.actionName.value, metricRegistry, serviceParams)
      } yield rat
  }

  final class Parallel[F[_]: Async, A] private[action] (
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    action: NJAction[F],
    ratioBuilder: NJRatio.Builder,
    gaugeBuilder: NJGauge.Builder,
    parallelism: Int,
    jobs: List[(Option[BatchJobName], F[A])])
      extends Runner[F, A](serviceParams, metricRegistry, action, ratioBuilder, gaugeBuilder) {

    def quasi(f: A => Json): F[QuasiResult] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Parallel(parallelism), name, idx, jobs.size) -> fa
      }
      val exec: Resource[F, F[QuasiResult]] = for {
        token <- Resource.eval(F.unique)
        rat <- ratio.evalTap(_.incDenominator(batchJobs.size.toLong))
        act <- action.retry((_: BatchJob, _: Unique.Token, fa: F[A]) => fa).buildWith(tap(f))
      } yield F
        .timed(F.parTraverseN(parallelism)(batchJobs) { case (job, fa) =>
          F.timed(act.run((job, token, fa)).attempt)
            .map { case (fd, result) => Detail(job, fd.toJava, result.isRight) }
            .flatTap(_ => rat.incNumerator(1))
        })
        .map { case (fd, details) =>
          QuasiResult(token, fd.toJava, BatchMode.Parallel(parallelism), details.sortBy(_.job.index))
        }

      exec.use(identity)
    }

    val quasi: F[QuasiResult] = quasi(nullTransform)

    def run(f: A => Json): F[List[A]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Parallel(parallelism), name, idx, jobs.size) -> fa
      }

      val exec: Resource[F, F[List[A]]] = for {
        token <- Resource.eval(F.unique)
        rat <- ratio.evalTap(_.incDenominator(batchJobs.size.toLong))
        act <- action.retry((_: BatchJob, _: Unique.Token, fa: F[A]) => fa).buildWith(tap(f))
      } yield F.parTraverseN(parallelism)(batchJobs) { case (job, fa) =>
        act.run((job, token, fa)).flatTap(_ => rat.incNumerator(1))
      }

      exec.use(identity)
    }

    val run: F[List[A]] = run(nullTransform)
  }

  final class Sequential[F[_]: Async, A] private[action] (
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    action: NJAction[F],
    ratioBuilder: NJRatio.Builder,
    gaugeBuilder: NJGauge.Builder,
    jobs: List[(Option[BatchJobName], F[A])])
      extends Runner[F, A](serviceParams, metricRegistry, action, ratioBuilder, gaugeBuilder) {

    def quasi(f: A => Json): F[QuasiResult] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Sequential, name, idx, jobs.size) -> fa
      }

      val exec: Resource[F, F[QuasiResult]] = for {
        token <- Resource.eval(F.unique)
        rat <- ratio.evalTap(_.incDenominator(batchJobs.size.toLong))
        act <- action.retry((_: BatchJob, _: Unique.Token, fa: F[A]) => fa).buildWith(tap(f))
      } yield batchJobs.traverse { case (job, fa) =>
        F.timed(act.run((job, token, fa)).attempt)
          .map { case (fd, result) => Detail(job, fd.toJava, result.isRight) }
          .flatTap(_ => rat.incNumerator(1))
      }.map(details =>
        QuasiResult(
          token = token,
          spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = BatchMode.Sequential,
          details = details.sortBy(_.job.index)))

      exec.use(identity)
    }

    val quasi: F[QuasiResult] = quasi(nullTransform)

    def run(f: A => Json): F[List[A]] = {
      val batchJobs: List[(BatchJob, F[A])] = jobs.zipWithIndex.map { case ((name, fa), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Sequential, name, idx, jobs.size) -> fa
      }

      val exec: Resource[F, F[List[A]]] = for {
        token <- Resource.eval(F.unique)
        rat <- ratio.evalTap(_.incDenominator(batchJobs.size.toLong))
        act <- action.retry((_: BatchJob, _: Unique.Token, fa: F[A]) => fa).buildWith(tap(f))
      } yield batchJobs.traverse { case (job, fa) =>
        act.run((job, token, fa)).flatTap(_ => rat.incNumerator(1))
      }

      exec.use(identity)
    }

    val run: F[List[A]] = run(nullTransform)
  }
}

final class NJBatch[F[_]: Async] private[guard] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  action: NJAction[F],
  ratioBuilder: NJRatio.Builder,
  gaugeBuilder: NJGauge.Builder
) {
  def sequential[A](fas: F[A]*): BatchRunner.Sequential[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map(none -> _)
    new BatchRunner.Sequential[F, A](serviceParams, metricRegistry, action, ratioBuilder, gaugeBuilder, jobs)
  }

  def namedSequential[A](fas: (String, F[A])*): BatchRunner.Sequential[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map { case (name, fa) =>
      BatchJobName(name).some -> fa
    }
    new BatchRunner.Sequential[F, A](serviceParams, metricRegistry, action, ratioBuilder, gaugeBuilder, jobs)
  }

  def parallel[A](parallelism: Int)(fas: F[A]*): BatchRunner.Parallel[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map(none -> _)
    new BatchRunner.Parallel[F, A](
      serviceParams,
      metricRegistry,
      action,
      ratioBuilder,
      gaugeBuilder,
      parallelism,
      jobs)
  }

  def parallel[A](fas: F[A]*): BatchRunner.Parallel[F, A] =
    parallel[A](fas.size)(fas*)

  def namedParallel[A](parallelism: Int)(fas: (String, F[A])*): BatchRunner.Parallel[F, A] = {
    val jobs: List[(Option[BatchJobName], F[A])] = fas.toList.map { case (name, fa) =>
      BatchJobName(name).some -> fa
    }
    new BatchRunner.Parallel[F, A](
      serviceParams,
      metricRegistry,
      action,
      ratioBuilder,
      gaugeBuilder,
      parallelism,
      jobs)
  }

  def namedParallel[A](fas: (String, F[A])*): BatchRunner.Parallel[F, A] =
    namedParallel[A](fas.size)(fas*)
}
