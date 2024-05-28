package com.github.chenharryhua.nanjin.guard.action
import cats.Traverse
import cats.effect.kernel.{Async, Resource, Unique}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import io.circe.Json
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps

import java.time.Duration
import java.util.UUID
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class Detail(nth_job: Int, took: Duration, is_done: Boolean)
@JsonCodec
final case class QuasiResult(uuid: UUID, mode: String, details: List[Detail])

final class NJBatch[F[_]: Async] private[guard] (
  action: NJAction[F],
  ratio: Resource[F, NJRatio[F]],
  gauge: NJGauge[F]) {
  private val F = Async[F]

  private def mode(par: Option[Int]): (String, Json) =
    "mode" -> Json.fromString(par.fold("sequential")(p => s"parallel-$p"))
  private def jobs(size: Long): (String, Json)              = "total_jobs" -> Json.fromLong(size)
  private def start(idx: Int): (String, Json)               = "started_at" -> idx.asJson
  private def finish(idx: Int): (String, Json)              = "done_at" -> idx.asJson
  private def failed(idx: Int): (String, Json)              = "failed_at" -> idx.asJson
  private def batch_id(uuid: UUID): (String, Json)          = "uuid" -> uuid.asJson
  private def batch_id(token: Unique.Token): (String, Json) = "token" -> Json.fromInt(token.hash)

  object quasi {

    def sequential[G[_]: Traverse, Z](gfz: G[F[Z]]): F[QuasiResult] = {
      val size: Long = gfz.size
      val run: Resource[F, F[QuasiResult]] = for {
        _ <- gauge.timed
        rat <- ratio.evalTap(_.incDenominator(size))
        act <- action
          .retry((_: Int, _: UUID, fz: F[Z]) => fz)
          .buildWith(_.tapInput { case (idx, uuid, _) =>
            Json.obj("quasi" -> Json.obj(batch_id(uuid), start(idx), jobs(size), mode(None)))
          }.tapOutput { case ((idx, uuid, _), _) =>
            Json.obj("quasi" -> Json.obj(batch_id(uuid), finish(idx), jobs(size), mode(None)))
          }.tapError { case (idx, uuid, _) =>
            Json.obj("quasi" -> Json.obj(batch_id(uuid), failed(idx), jobs(size), mode(None)))
          })
      } yield UUIDGen[F].randomUUID.flatMap { uuid =>
        gfz.zipWithIndex.traverse { case (fz, idx) =>
          val oneBase = idx + 1
          F.timed(act.run((oneBase, uuid, fz)).attempt).flatTap(_ => rat.incNumerator(1)).map {
            case (fd, result) =>
              Detail(nth_job = oneBase, took = fd.toJava, is_done = result.isRight)
          }
        }.map(details => QuasiResult(uuid, "sequential", details.toList))
      }
      run.use(identity)
    }

    def sequential[Z](fzs: F[Z]*): F[QuasiResult] =
      sequential[List, Z](fzs.toList)

    private def parallel_run[G[_]: Traverse, Z](parallelism: Int, gfz: G[F[Z]]): F[QuasiResult] = {
      val size: Long = gfz.size
      val run: Resource[F, F[QuasiResult]] = for {
        _ <- gauge.timed
        rat <- ratio.evalTap(_.incDenominator(size))
        act <- action
          .retry((_: Int, _: UUID, fz: F[Z]) => fz)
          .buildWith(_.tapInput { case (idx, uuid, _) =>
            Json.obj("quasi" -> Json.obj(batch_id(uuid), start(idx), jobs(size), mode(Some(parallelism))))
          }.tapOutput { case ((idx, uuid, _), _) =>
            Json.obj("quasi" -> Json.obj(batch_id(uuid), finish(idx), jobs(size), mode(Some(parallelism))))
          }.tapError { case (idx, uuid, _) =>
            Json.obj("quasi" -> Json.obj(batch_id(uuid), failed(idx), jobs(size), mode(Some(parallelism))))
          })
      } yield UUIDGen[F].randomUUID.flatMap { uuid =>
        F.parTraverseN(parallelism)(gfz.zipWithIndex) { case (fz, idx) =>
          val oneBase = idx + 1
          F.timed(act.run((oneBase, uuid, fz)).attempt).flatTap(_ => rat.incNumerator(1)).map {
            case (fd, result) =>
              Detail(nth_job = oneBase, took = fd.toJava, is_done = result.isRight)
          }
        }.map(details => QuasiResult(uuid, s"parallel-$parallelism", details.toList.sortBy(_.nth_job)))
      }
      run.use(identity)
    }

    def parallel[Z](parallelism: Int)(fzs: F[Z]*): F[QuasiResult] =
      parallel_run[List, Z](parallelism, fzs.toList)

    def parallel[Z](fzs: F[Z]*): F[QuasiResult] =
      parallel_run[List, Z](fzs.size, fzs.toList)
  }

  // batch

  def sequential[G[_]: Traverse, Z](gfz: G[F[Z]]): F[G[Z]] = {
    val size: Long = gfz.size
    val run: Resource[F, F[G[Z]]] = for {
      _ <- gauge.timed
      rat <- ratio.evalTap(_.incDenominator(size))
      act <- action
        .retry((_: Int, _: Unique.Token, fz: F[Z]) => fz)
        .buildWith(_.tapInput { case (idx, token, _) =>
          Json.obj("batch" -> Json.obj(batch_id(token), start(idx), jobs(size), mode(None)))
        }.tapOutput { case ((idx, token, _), _) =>
          Json.obj("batch" -> Json.obj(batch_id(token), finish(idx), jobs(size), mode(None)))
        }.tapError { case (idx, token, _) =>
          Json.obj("batch" -> Json.obj(batch_id(token), failed(idx), jobs(size), mode(None)))
        })
    } yield Unique[F].unique.flatMap { token =>
      gfz.zipWithIndex.traverse { case (fz, idx) =>
        act.run((idx + 1, token, fz)).flatTap(_ => rat.incNumerator(1))
      }
    }
    run.use(identity)
  }

  def sequential[Z](fzs: F[Z]*): F[List[Z]] =
    sequential[List, Z](fzs.toList)

  private def parallel_run[G[_]: Traverse, Z](parallelism: Int, gfz: G[F[Z]]): F[G[Z]] = {
    val size: Long = gfz.size
    val run: Resource[F, F[G[Z]]] = for {
      _ <- gauge.timed
      rat <- ratio.evalTap(_.incDenominator(size))
      act <- action
        .retry((_: Int, _: Unique.Token, fz: F[Z]) => fz)
        .buildWith(_.tapInput { case (idx, token, _) =>
          Json.obj("batch" -> Json.obj(batch_id(token), start(idx), jobs(size), mode(Some(parallelism))))
        }.tapOutput { case ((idx, token, _), _) =>
          Json.obj("batch" -> Json.obj(batch_id(token), finish(idx), jobs(size), mode(Some(parallelism))))
        }.tapError { case (idx, token, _) =>
          Json.obj("batch" -> Json.obj(batch_id(token), failed(idx), jobs(size), mode(Some(parallelism))))
        })
    } yield Unique[F].unique.flatMap { token =>
      F.parTraverseN(parallelism)(gfz.zipWithIndex) { case (fz, idx) =>
        act.run((idx + 1, token, fz)).flatTap(_ => rat.incNumerator(1))
      }
    }
    run.use(identity)
  }

  def parallel[Z](parallelism: Int)(fzs: F[Z]*): F[List[Z]] =
    parallel_run[List, Z](parallelism, fzs.toList)

  def parallel[Z](fzs: F[Z]*): F[List[Z]] =
    parallel_run[List, Z](fzs.size, fzs.toList)
}
