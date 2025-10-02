package com.github.chenharryhua.nanjin.http.client.middleware

import cats.effect.kernel.{Async, Resource, Temporal}
import cats.effect.std.Hotswap
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.{Policy, TickStatus}
import org.http4s.client.Client
import org.http4s.client.middleware.RetryPolicy
import org.http4s.headers.`Retry-After`
import org.http4s.{Request, Response}

import java.time.ZoneId
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.DurationConverters.JavaDurationOps

object retry {
  def apply[F[_]: Async](zoneId: ZoneId, policy: Policy): Client[F] => Client[F] =
    client => impl[F](zoneId, policy, RetryPolicy.defaultRetriable)(client)

  def reckless[F[_]: Async](zoneId: ZoneId, policy: Policy): Client[F] => Client[F] =
    client => impl[F](zoneId, policy, (_, ex) => RetryPolicy.recklesslyRetriable(ex))(client)

  private def impl[F[_]: Async](
    zoneId: ZoneId,
    policy: Policy,
    retriable: (Request[F], Either[Throwable, Response[F]]) => Boolean)(client: Client[F]): Client[F] = {
    def nextAttempt(
      req: Request[F],
      tickStatus: TickStatus,
      retryHeader: Option[`Retry-After`],
      hotswap: Hotswap[F, Either[Throwable, Response[F]]]
    ): F[Response[F]] = {
      val header_duration: F[Option[FiniteDuration]] =
        retryHeader.traverse { h =>
          h.retry match {
            case Left(date)  => Async[F].realTime.map(date.toDuration - _)
            case Right(secs) => secs.seconds.pure[F]
          }
        }
      val sleep_duration = header_duration.map {
        case Some(value) => value.max(tickStatus.tick.snooze.toScala)
        case None        => tickStatus.tick.snooze.toScala
      }
      sleep_duration.flatMap(Temporal[F].sleep(_)) >> retryLoop(req, tickStatus, hotswap)
    }

    def retryLoop(
      req: Request[F],
      tickStatus: TickStatus,
      hotswap: Hotswap[F, Either[Throwable, Response[F]]]
    ): F[Response[F]] =
      hotswap.clear >>
        hotswap.swap(client.run(req).attempt).flatMap {
          case Left(ex) =>
            Temporal[F].realTimeInstant.flatMap(now =>
              tickStatus.next(now) match {
                case Some(ts) => nextAttempt(req, ts, None, hotswap)
                case None     => ex.raiseError
              })
          case Right(response) =>
            if (retriable(req, Right(response))) {
              Temporal[F].realTimeInstant.flatMap(now =>
                tickStatus.next(now) match {
                  case Some(ts) => nextAttempt(req, ts, response.headers.get[`Retry-After`], hotswap)
                  case None     => response.pure[F]
                })
            } else response.pure[F]
        }

    Client[F] { (req: Request[F]) =>
      Hotswap.create[F, Either[Throwable, Response[F]]].flatMap { hotswap =>
        Resource.eval(TickStatus.zeroth[F](zoneId, policy).flatMap(ts => retryLoop(req, ts, hotswap)))
      }
    }
  }
}
