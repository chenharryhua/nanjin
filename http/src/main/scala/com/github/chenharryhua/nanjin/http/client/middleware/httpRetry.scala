package com.github.chenharryhua.nanjin.http.client.middleware

import cats.effect.kernel.{Async, Clock, Resource, Temporal}
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

object httpRetry {

  private type Retriable[F[_]] = (Request[F], Either[Throwable, Response[F]]) => Boolean

  def apply[F[_]: Async](
    zoneId: ZoneId,
    policy: Policy,
    retriable: Retriable[F] = RetryPolicy.defaultRetriable[F](_, _))(client: Client[F]): Client[F] =
    impl[F](zoneId, policy, retriable)(client)

  def reckless[F[_]: Async](zoneId: ZoneId, policy: Policy)(client: Client[F]): Client[F] =
    apply[F](
      zoneId,
      policy,
      (_: Request[F], ex: Either[Throwable, Response[F]]) => RetryPolicy.recklesslyRetriable[F](ex))(client)

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
      val effective_delay: F[FiniteDuration] =
        retryHeader.traverse { h =>
          h.retry match {
            case Left(date)  => Clock[F].realTime.map(n => (date.toDuration - n).max(0.seconds))
            case Right(secs) => secs.seconds.pure[F]
          }
        }.map {
          case Some(after) => after.max(tickStatus.tick.snooze.toScala)
          case None        => tickStatus.tick.snooze.toScala
        }
      effective_delay.flatMap(Temporal[F].sleep(_)) >> retryLoop(req, tickStatus, hotswap)
    }

    def retryLoop(
      req: Request[F],
      tickStatus: TickStatus,
      hotswap: Hotswap[F, Either[Throwable, Response[F]]]
    ): F[Response[F]] =
      hotswap.clear >>
        hotswap.swap(client.run(req).attempt).flatMap {
          case Left(ex) =>
            Clock[F].realTimeInstant.flatMap(now =>
              tickStatus.next(now) match {
                case Some(ts) => nextAttempt(req, ts, None, hotswap)
                case None     => ex.raiseError
              })
          case Right(response) =>
            if (retriable(req, Right(response))) {
              Clock[F].realTimeInstant.flatMap(now =>
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
