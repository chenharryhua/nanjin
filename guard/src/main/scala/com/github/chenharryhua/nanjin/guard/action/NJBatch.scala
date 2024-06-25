package com.github.chenharryhua.nanjin.guard.action
import cats.effect.kernel.{Async, Resource, Unique}
import cats.syntax.all.*
import cats.{Endo, Show}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.translator.fmt
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

import java.time.Duration
import scala.jdk.DurationConverters.ScalaDurationOps

sealed private trait BatchKind
private object BatchKind {
  case object Quasi extends BatchKind
  case object Batch extends BatchKind
  implicit val showBatchKind: Show[BatchKind] = {
    case Quasi => "quasi"
    case Batch => "batch"
  }
}

sealed trait BatchMode
private object BatchMode {
  final case class Parallel(size: Int) extends BatchMode
  case object Sequential extends BatchMode

  implicit val showBatchMode: Show[BatchMode] = {
    case Parallel(size) => s"parallel-$size"
    case Sequential     => "sequential"
  }

  implicit val encoderBatchMode: Encoder[BatchMode] =
    (a: BatchMode) => Json.fromString(a.show)
}

final private case class BatchJob(
  kind: BatchKind,
  mode: BatchMode,
  name: Option[String],
  index: Int,
  jobs: Int)

private object BatchJob {
  implicit val encodeBatchJob: Encoder[BatchJob] = (a: BatchJob) =>
    Json
      .obj(
        show"${a.kind}" -> Json.obj(
          "mode" -> a.mode.asJson,
          "name" -> a.name.asJson,
          "index" -> (a.index + 1).asJson,
          "jobs" -> a.jobs.asJson))
      .deepDropNullValues
}

final case class Detail(name: Option[String], index: Int, took: Duration, done: Boolean)
final case class QuasiResult(token: Unique.Token, mode: BatchMode, spent: Duration, details: List[Detail])
object QuasiResult {
  val BatchID: String = "id"
  implicit val encoderQuasiResult: Encoder[QuasiResult] =
    (a: QuasiResult) =>
      Json.obj(
        BatchID -> a.token.hash.asJson,
        "mode" -> a.mode.asJson,
        "spent" -> fmt.format(a.spent).asJson,
        "details" -> a.details
          .map(d =>
            Json
              .obj(
                "name" -> d.name.asJson,
                "index" -> d.index.asJson,
                "took" -> fmt.format(d.took).asJson,
                "done" -> d.done.asJson)
              .dropNullValues)
          .asJson
      )
}

final class NJBatch[F[_]: Async] private[guard] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  action: NJAction[F],
  ratioBuilder: NJRatio.Builder,
  gaugeBuilder: NJGauge.Builder) {
  private val F = Async[F]

  private val BatchID: String = QuasiResult.BatchID

  private val ratio: Resource[F, NJRatio[F]] =
    for {
      _ <- gaugeBuilder
        .withMeasurement(action.actionParams.measurement.value)
        .build[F](action.actionParams.actionName.value, metricRegistry, serviceParams)
        .timed
      rat <- ratioBuilder
        .withMeasurement(action.actionParams.measurement.value)
        .build(action.actionParams.actionName.value, metricRegistry, serviceParams)
    } yield rat

  private def tap[Z]: Endo[BuildWith.Builder[F, (BatchJob, Unique.Token, F[Z]), Z]] =
    _.tapInput { case (job, token, _) =>
      job.asJson.deepMerge(Json.obj(BatchID -> token.hash.asJson))
    }.tapOutput { case ((job, token, _), _) =>
      job.asJson.deepMerge(Json.obj(BatchID -> token.hash.asJson))
    }.tapError { case ((job, token, _), _) =>
      job.asJson.deepMerge(Json.obj(BatchID -> token.hash.asJson))
    }

  object quasi {

    private def sequential_run[Z](gfz: List[(BatchJob, F[Z])]): F[QuasiResult] = {
      val run: Resource[F, F[QuasiResult]] = for {
        token <- Resource.eval(F.unique)
        rat <- ratio.evalTap(_.incDenominator(gfz.size.toLong))
        act <- action.retry((_: BatchJob, _: Unique.Token, fz: F[Z]) => fz).buildWith(tap)
      } yield gfz.traverse { case (job, fz) =>
        F.timed(act.run((job, token, fz)).attempt)
          .map { case (fd, result) => Detail(job.name, job.index, fd.toJava, result.isRight) }
          .flatTap(_ => rat.incNumerator(1))
      }.map(details =>
        QuasiResult(
          token = token,
          spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = BatchMode.Sequential,
          details = details.sortBy(_.index)))

      run.use(identity)
    }

    def sequential[Z](fzs: F[Z]*): F[QuasiResult] =
      sequential_run[Z](fzs.toList.zipWithIndex.map { case (fz, idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Sequential, None, idx, fzs.size) -> fz
      })

    def namedSequential[Z](fzs: (String, F[Z])*): F[QuasiResult] =
      sequential_run[Z](fzs.toList.zipWithIndex.map { case ((name, fz), idx) =>
        BatchJob(BatchKind.Quasi, BatchMode.Sequential, name.some, idx, fzs.size) -> fz
      })

    private def parallel_run[Z](parallelism: Int, gfz: List[(BatchJob, F[Z])]): F[QuasiResult] = {
      val run: Resource[F, F[QuasiResult]] = for {
        token <- Resource.eval(F.unique)
        rat <- ratio.evalTap(_.incDenominator(gfz.size.toLong))
        act <- action.retry((_: BatchJob, _: Unique.Token, fz: F[Z]) => fz).buildWith(tap)
      } yield F
        .parTraverseN(parallelism)(gfz) { case (job, fz) =>
          F.timed(act.run((job, token, fz)).attempt)
            .map { case (fd, result) => Detail(job.name, job.index, fd.toJava, result.isRight) }
            .flatTap(_ => rat.incNumerator(1))
        }
        .map(details =>
          QuasiResult(
            token = token,
            spent = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
            mode = BatchMode.Parallel(parallelism),
            details = details.sortBy(_.index)))

      run.use(identity)
    }

    def parallel[Z](parallelism: Int)(fzs: F[Z]*): F[QuasiResult] =
      parallel_run[Z](
        parallelism,
        fzs.toList.zipWithIndex.map { case (fz, idx) =>
          BatchJob(BatchKind.Quasi, BatchMode.Parallel(parallelism), None, idx, fzs.size) -> fz
        })

    def parallel[Z](fzs: F[Z]*): F[QuasiResult] = parallel[Z](fzs.size)(fzs*)

    def namedParallel[Z](parallelism: Int)(fzs: (String, F[Z])*): F[QuasiResult] =
      parallel_run[Z](
        parallelism,
        fzs.toList.zipWithIndex.map { case ((name, fz), idx) =>
          BatchJob(BatchKind.Quasi, BatchMode.Parallel(parallelism), name.some, idx, fzs.size) -> fz
        })

    def namedParallel[Z](fzs: (String, F[Z])*): F[QuasiResult] = namedParallel[Z](fzs.size)(fzs*)
  }

  // batch

  private def sequential_run[Z](gfz: List[(BatchJob, F[Z])]): F[List[Z]] = {
    val run: Resource[F, F[List[Z]]] = for {
      token <- Resource.eval(F.unique)
      rat <- ratio.evalTap(_.incDenominator(gfz.size.toLong))
      act <- action.retry((_: BatchJob, _: Unique.Token, fz: F[Z]) => fz).buildWith(tap)
    } yield gfz.traverse { case (job, fz) =>
      act.run((job, token, fz)).flatTap(_ => rat.incNumerator(1))
    }

    run.use(identity)
  }

  def sequential[Z](fzs: F[Z]*): F[List[Z]] =
    sequential_run[Z](fzs.toList.zipWithIndex.map { case (fz, idx) =>
      BatchJob(BatchKind.Batch, BatchMode.Sequential, None, idx, fzs.size) -> fz
    })

  def namedSequential[Z](fzs: (String, F[Z])*): F[List[Z]] =
    sequential_run[Z](fzs.toList.zipWithIndex.map { case ((name, fz), idx) =>
      BatchJob(BatchKind.Batch, BatchMode.Sequential, name.some, idx, fzs.size) -> fz
    })

  private def parallel_run[Z](parallelism: Int, gfz: List[(BatchJob, F[Z])]): F[List[Z]] = {
    val run: Resource[F, F[List[Z]]] = for {
      token <- Resource.eval(F.unique)
      rat <- ratio.evalTap(_.incDenominator(gfz.size.toLong))
      act <- action.retry((_: BatchJob, _: Unique.Token, fz: F[Z]) => fz).buildWith(tap)
    } yield F.parTraverseN(parallelism)(gfz) { case (job, fz) =>
      act.run((job, token, fz)).flatTap(_ => rat.incNumerator(1))
    }

    run.use(identity)
  }

  def parallel[Z](parallelism: Int)(fzs: F[Z]*): F[List[Z]] =
    parallel_run[Z](
      parallelism,
      fzs.toList.zipWithIndex.map { case (fz, idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Parallel(parallelism), None, idx, fzs.size) -> fz
      })

  def parallel[Z](fzs: F[Z]*): F[List[Z]] =
    parallel[Z](fzs.size)(fzs*)

  def namedParallel[Z](parallelism: Int)(fzs: (String, F[Z])*): F[List[Z]] =
    parallel_run[Z](
      parallelism,
      fzs.toList.zipWithIndex.map { case ((name, fz), idx) =>
        BatchJob(BatchKind.Batch, BatchMode.Parallel(parallelism), name.some, idx, fzs.size) -> fz
      })

  def namedParallel[Z](fzs: (String, F[Z])*): F[List[Z]] =
    namedParallel[Z](fzs.size)(fzs*)
}
