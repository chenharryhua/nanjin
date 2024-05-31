package com.github.chenharryhua.nanjin.guard.action
import cats.Traverse
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
final case class Detail(nth_job: Int, took: Duration, is_done: Boolean)
@JsonCodec
final case class QuasiResult(token: Int, mode: String, details: List[Detail])

final class NJBatch[F[_]: Async] private[guard] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  action: NJAction[F],
  ratioBuilder: NJRatio.Builder,
  gaugeBuilder: NJGauge.Builder) {
  private val F = Async[F]

  private def mode(par: Option[Int]): (String, Json) =
    "mode" -> Json.fromString(par.fold("sequential")(p => s"parallel-$p"))
  private def jobs(size: Long): (String, Json)              = "total_jobs" -> Json.fromLong(size)
  private def start(idx: Int): (String, Json)               = "started_at" -> idx.asJson
  private def finish(idx: Int): (String, Json)              = "done_at" -> idx.asJson
  private def failed(idx: Int): (String, Json)              = "failed_at" -> idx.asJson
  private def batch_id(token: Unique.Token): (String, Json) = "token" -> Json.fromInt(token.hash)

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

    def sequential[G[_]: Traverse, Z](gfz: G[F[Z]]): F[QuasiResult] = {
      val size: Long = gfz.size
      val run: Resource[F, F[QuasiResult]] = for {
        token <- Resource.eval(F.unique)
        rat <- ratio.evalTap(_.incDenominator(size))
        act <- action
          .retry((_: Int, fz: F[Z]) => fz)
          .buildWith(
            _.tapInput { case (idx, _) =>
              Json.obj("quasi" -> Json.obj(batch_id(token), start(idx), jobs(size), mode(None)))
            }.tapOutput { case ((idx, _), _) =>
              Json.obj("quasi" -> Json.obj(batch_id(token), finish(idx), jobs(size), mode(None)))
            }.tapError { case (idx, _) =>
              Json.obj("quasi" -> Json.obj(batch_id(token), failed(idx), jobs(size), mode(None)))
            }
          )
      } yield gfz.zipWithIndex.traverse { case (fz, idx) =>
        val oneBase = idx + 1
        F.timed(act.run((oneBase, fz)).attempt).flatTap(_ => rat.incNumerator(1)).map { case (fd, result) =>
          Detail(nth_job = oneBase, took = fd.toJava, is_done = result.isRight)
        }
      }.map(details => QuasiResult(token.hash, "sequential", details.toList))

      run.use(identity)
    }

    def sequential[Z](fzs: F[Z]*): F[QuasiResult] =
      sequential[List, Z](fzs.toList)

    private def parallel_run[G[_]: Traverse, Z](parallelism: Int, gfz: G[F[Z]]): F[QuasiResult] = {
      val size: Long = gfz.size
      val run: Resource[F, F[QuasiResult]] = for {
        token <- Resource.eval(F.unique)
        rat <- ratio.evalTap(_.incDenominator(size))
        act <- action
          .retry((_: Int, fz: F[Z]) => fz)
          .buildWith(
            _.tapInput { case (idx, _) =>
              Json.obj("quasi" -> Json.obj(batch_id(token), start(idx), jobs(size), mode(Some(parallelism))))
            }.tapOutput { case ((idx, _), _) =>
              Json.obj("quasi" -> Json.obj(batch_id(token), finish(idx), jobs(size), mode(Some(parallelism))))
            }.tapError { case (idx, _) =>
              Json.obj("quasi" -> Json.obj(batch_id(token), failed(idx), jobs(size), mode(Some(parallelism))))
            }
          )
      } yield F
        .parTraverseN(parallelism)(gfz.zipWithIndex) { case (fz, idx) =>
          val oneBase = idx + 1
          F.timed(act.run((oneBase, fz)).attempt).flatTap(_ => rat.incNumerator(1)).map { case (fd, result) =>
            Detail(nth_job = oneBase, took = fd.toJava, is_done = result.isRight)
          }
        }
        .map(details => QuasiResult(token.hash, s"parallel-$parallelism", details.toList.sortBy(_.nth_job)))

      run.use(identity)
    }

    def parallel[Z](parallelism: Int)(fzs: F[Z]*): F[QuasiResult] =
      parallel_run[List, Z](parallelism, fzs.toList)

    def parallel[Z](fzs: F[Z]*): F[QuasiResult] =
      parallel_run[List, Z](fzs.size, fzs.toList)
  }

  // batch

  def sequential[G[_]: Traverse, Z: Encoder](gfz: G[F[Z]]): F[G[Z]] = {
    val size: Long = gfz.size
    val run: Resource[F, F[G[Z]]] = for {
      token <- Resource.eval(F.unique)
      rat <- ratio.evalTap(_.incDenominator(size))
      act <- action
        .retry((_: Int, fz: F[Z]) => fz)
        .buildWith(
          _.tapInput { case (idx, _) =>
            Json.obj("batch" -> Json.obj(batch_id(token), start(idx), jobs(size), mode(None)))
          }.tapOutput { case ((idx, _), out) =>
            Json.obj(
              "batch" -> Json.obj(batch_id(token), finish(idx), jobs(size), mode(None), "out" -> out.asJson))
          }.tapError { case (idx, _) =>
            Json.obj("batch" -> Json.obj(batch_id(token), failed(idx), jobs(size), mode(None)))
          }
        )
    } yield gfz.zipWithIndex.traverse { case (fz, idx) =>
      act.run((idx + 1, fz)).flatTap(_ => rat.incNumerator(1))
    }

    run.use(identity)
  }

  def sequential[Z: Encoder](fzs: F[Z]*): F[List[Z]] =
    sequential[List, Z](fzs.toList)

  private def parallel_run[G[_]: Traverse, Z: Encoder](parallelism: Int, gfz: G[F[Z]]): F[G[Z]] = {
    val size: Long = gfz.size
    val run: Resource[F, F[G[Z]]] = for {
      token <- Resource.eval(F.unique)
      rat <- ratio.evalTap(_.incDenominator(size))
      act <- action
        .retry((_: Int, fz: F[Z]) => fz)
        .buildWith(
          _.tapInput { case (idx, _) =>
            Json.obj("batch" -> Json.obj(batch_id(token), start(idx), jobs(size), mode(Some(parallelism))))
          }.tapOutput { case ((idx, _), out) =>
            Json.obj("batch" -> Json
              .obj(batch_id(token), finish(idx), jobs(size), mode(Some(parallelism)), "out" -> out.asJson))
          }.tapError { case (idx, _) =>
            Json.obj("batch" -> Json.obj(batch_id(token), failed(idx), jobs(size), mode(Some(parallelism))))
          }
        )
    } yield F.parTraverseN(parallelism)(gfz.zipWithIndex) { case (fz, idx) =>
      act.run((idx + 1, fz)).flatTap(_ => rat.incNumerator(1))
    }

    run.use(identity)
  }

  def parallel[Z: Encoder](parallelism: Int)(fzs: F[Z]*): F[List[Z]] =
    parallel_run[List, Z](parallelism, fzs.toList)

  def parallel[Z: Encoder](fzs: F[Z]*): F[List[Z]] =
    parallel_run[List, Z](fzs.size, fzs.toList)
}
