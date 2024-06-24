package com.github.chenharryhua.nanjin.guard.action
import cats.effect.kernel.{Async, Resource, Unique}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

import java.time.Duration
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class Detail(name: String, took: Duration, is_done: Boolean)
@JsonCodec
final case class QuasiResult(id: String, total: Duration, mode: String, details: List[Detail])

final class NJBatch[F[_]: Async] private[guard] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  action: NJAction[F],
  ratioBuilder: NJRatio.Builder,
  gaugeBuilder: NJGauge.Builder) {
  private val F = Async[F]

  private def mode(par: Option[Int]): (String, Json) =
    "mode" -> Json.fromString(par.fold("sequential")(p => s"parallel-$p"))
  private def jobs(size: Long): (String, Json)              = "jobs" -> Json.fromLong(size)
  private def start(name: String): (String, Json)           = "start" -> name.asJson
  private def finish(name: String): (String, Json)          = "done" -> name.asJson
  private def failed(name: String): (String, Json)          = "failed" -> name.asJson
  private def batch_id(token: Unique.Token): (String, Json) = "id" -> Json.fromInt(token.hash)

  private def jobName(idx: Int): String = s"job-${idx + 1}"

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

  object quasi {

    private def sequential_run[Z](gfz: List[(String, F[Z])]): F[QuasiResult] = {
      val size: Long = gfz.size.toLong
      val run: Resource[F, F[QuasiResult]] = for {
        token <- Resource.eval(F.unique)
        rat <- ratio.evalTap(_.incDenominator(size))
        act <- action
          .retry((_: String, fz: F[Z]) => fz)
          .buildWith(
            _.tapInput { case (name, _) =>
              Json.obj("quasi" -> Json.obj(batch_id(token), start(name), jobs(size), mode(None)))
            }.tapOutput { case ((name, _), _) =>
              Json.obj("quasi" -> Json.obj(batch_id(token), finish(name), jobs(size), mode(None)))
            }.tapError { case ((name, _), _) =>
              Json.obj("quasi" -> Json.obj(batch_id(token), failed(name), jobs(size), mode(None)))
            }
          )
      } yield gfz.traverse { case (name, fz) =>
        F.timed(act.run((name, fz)).attempt).flatTap(_ => rat.incNumerator(1)).map { case (fd, result) =>
          Detail(name = name, took = fd.toJava, is_done = result.isRight)
        }
      }.map(details =>
        QuasiResult(
          id = token.hash.show,
          total = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
          mode = "sequential",
          details = details))

      run.use(identity)
    }

    def sequential[Z](fzs: F[Z]*): F[QuasiResult] =
      sequential_run[Z](fzs.toList.zipWithIndex.map { case (fz, idx) => jobName(idx) -> fz })

    def namedSequential[Z](fzs: (String, F[Z])*): F[QuasiResult] =
      sequential_run[Z](fzs.toList)

    private def parallel_run[Z](parallelism: Int, gfz: List[(String, F[Z])]): F[QuasiResult] = {
      val size: Long = gfz.size.toLong
      val run: Resource[F, F[QuasiResult]] = for {
        token <- Resource.eval(F.unique)
        rat <- ratio.evalTap(_.incDenominator(size))
        act <- action
          .retry((_: String, fz: F[Z]) => fz)
          .buildWith(
            _.tapInput { case (name, _) =>
              Json.obj("quasi" -> Json.obj(batch_id(token), start(name), jobs(size), mode(Some(parallelism))))
            }.tapOutput { case ((name, _), _) =>
              Json.obj(
                "quasi" -> Json.obj(batch_id(token), finish(name), jobs(size), mode(Some(parallelism))))
            }.tapError { case ((name, _), _) =>
              Json.obj(
                "quasi" -> Json.obj(batch_id(token), failed(name), jobs(size), mode(Some(parallelism))))
            }
          )
      } yield F
        .parTraverseN(parallelism)(gfz) { case (name, fz) =>
          F.timed(act.run((name, fz)).attempt).flatTap(_ => rat.incNumerator(1)).map { case (fd, result) =>
            Detail(name = name, took = fd.toJava, is_done = result.isRight)
          }
        }
        .map(details =>
          QuasiResult(
            id = token.hash.show,
            total = details.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
            mode = s"parallel-$parallelism",
            details = details))

      run.use(identity)
    }

    def parallel[Z](parallelism: Int)(fzs: F[Z]*): F[QuasiResult] =
      parallel_run[Z](parallelism, fzs.toList.zipWithIndex.map { case (fz, idx) => jobName(idx) -> fz })

    def namedParallel[Z](parallelism: Int)(fzs: (String, F[Z])*): F[QuasiResult] =
      parallel_run[Z](parallelism, fzs.toList)

    def parallel[Z](fzs: F[Z]*): F[QuasiResult] =
      parallel_run[Z](fzs.size, fzs.toList.zipWithIndex.map { case (fz, idx) => jobName(idx) -> fz })

    def namedParallel[Z](fzs: (String, F[Z])*): F[QuasiResult] =
      parallel_run[Z](fzs.size, fzs.toList)

  }

  // batch

  private def sequential_run[Z: Encoder](gfz: List[(String, F[Z])]): F[List[Z]] = {
    val size: Long = gfz.size.toLong
    val run: Resource[F, F[List[Z]]] = for {
      token <- Resource.eval(F.unique)
      rat <- ratio.evalTap(_.incDenominator(size))
      act <- action
        .retry((_: String, fz: F[Z]) => fz)
        .buildWith(
          _.tapInput { case (name, _) =>
            Json.obj("batch" -> Json.obj(batch_id(token), start(name), jobs(size), mode(None)))
          }.tapOutput { case ((name, _), out) =>
            Json.obj(
              "batch" -> Json.obj(batch_id(token), finish(name), jobs(size), mode(None), "out" -> out.asJson))
          }.tapError { case ((name, _), _) =>
            Json.obj("batch" -> Json.obj(batch_id(token), failed(name), jobs(size), mode(None)))
          }
        )
    } yield gfz.traverse { case (name, fz) =>
      act.run((name, fz)).flatTap(_ => rat.incNumerator(1))
    }

    run.use(identity)
  }

  def sequential[Z: Encoder](fzs: F[Z]*): F[List[Z]] =
    sequential_run[Z](fzs.toList.zipWithIndex.map { case (fz, idx) => jobName(idx) -> fz })

  def namedSequential[Z: Encoder](fzs: (String, F[Z])*): F[List[Z]] =
    sequential_run[Z](fzs.toList)

  private def parallel_run[Z: Encoder](parallelism: Int, gfz: List[(String, F[Z])]): F[List[Z]] = {
    val size: Long = gfz.size.toLong
    val run: Resource[F, F[List[Z]]] = for {
      token <- Resource.eval(F.unique)
      rat <- ratio.evalTap(_.incDenominator(size))
      act <- action
        .retry((_: String, fz: F[Z]) => fz)
        .buildWith(
          _.tapInput { case (name, _) =>
            Json.obj("batch" -> Json.obj(batch_id(token), start(name), jobs(size), mode(Some(parallelism))))
          }.tapOutput { case ((name, _), out) =>
            Json.obj("batch" -> Json
              .obj(batch_id(token), finish(name), jobs(size), mode(Some(parallelism)), "out" -> out.asJson))
          }.tapError { case ((name, _), _) =>
            Json.obj("batch" -> Json.obj(batch_id(token), failed(name), jobs(size), mode(Some(parallelism))))
          }
        )
    } yield F.parTraverseN(parallelism)(gfz) { case (name, fz) =>
      act.run((name, fz)).flatTap(_ => rat.incNumerator(1))
    }

    run.use(identity)
  }

  def parallel[Z: Encoder](parallelism: Int)(fzs: F[Z]*): F[List[Z]] =
    parallel_run[Z](parallelism, fzs.toList.zipWithIndex.map { case (fz, idx) => jobName(idx) -> fz })

  def namedParallel[Z: Encoder](parallelism: Int)(fzs: (String, F[Z])*): F[List[Z]] =
    parallel_run[Z](parallelism, fzs.toList)

  def parallel[Z: Encoder](fzs: F[Z]*): F[List[Z]] =
    parallel_run[Z](fzs.size, fzs.toList.zipWithIndex.map { case (fz, idx) => jobName(idx) -> fz })

  def namedParallel[Z: Encoder](fzs: (String, F[Z])*): F[List[Z]] =
    parallel_run[Z](fzs.size, fzs.toList)
}
