package com.github.chenharryhua.nanjin.http.client.middleware

import cats.effect.kernel.{Async, Clock, Resource, Temporal}
import cats.effect.std.NonEmptyHotswap
import cats.syntax.applicative.given
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.chrono.{Policy, PolicyTick}
import monocle.Monocle.focus
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
    f: Policy.type => Policy,
    retriable: Retriable[F] = RetryPolicy.defaultRetriable[F])(client: Client[F]): Client[F] =
    impl[F](zoneId, f(Policy), retriable)(client)

  def reckless[F[_]: Async](zoneId: ZoneId, f: Policy.type => Policy)(client: Client[F]): Client[F] = {
    val g = (_: Request[F], ex: Either[Throwable, Response[F]]) => RetryPolicy.recklesslyRetriable[F](ex)
    apply[F](zoneId, f, g)(client)
  }

  final private case class RetryAttempt[F[_]](
    request: Request[F],
    policyTick: PolicyTick[F],
    hotswap: NonEmptyHotswap[F, Either[Throwable, Response[F]]],
    retryAfter: Option[`Retry-After`]
  )

  private def impl[F[_]: Async](
    zoneId: ZoneId,
    policy: Policy,
    retriable: (Request[F], Either[Throwable, Response[F]]) => Boolean)(client: Client[F]): Client[F] = {

    def nextAttempt(ra: RetryAttempt[F]): F[Response[F]] = {
      val effectiveDelay: F[FiniteDuration] =
        ra.retryAfter.traverse { h =>
          h.retry match {
            case Left(date)  => Clock[F].realTime.map(n => (date.toDuration - n).max(0.seconds))
            case Right(secs) => secs.seconds.pure[F]
          }
        }.map {
          case Some(after) => after.max(ra.policyTick.tick.snooze.toScala)
          case None        => ra.policyTick.tick.snooze.toScala
        }

      effectiveDelay.flatMap(Temporal[F].sleep(_)) >>
        ra.hotswap.swap(client.run(ra.request).attempt) >>
        retryLoop(ra)
    }

    def retryLoop(ra: RetryAttempt[F]): F[Response[F]] =
      ra.hotswap.get.use {
        case Left(ex) =>
          ra.policyTick.advance.flatMap {
            case Some(ts) => Right(ra.focus(_.policyTick).replace(ts)).pure[F]
            case None     => ex.raiseError
          }
        case Right(response) =>
          if (retriable(ra.request, Right(response))) {
            ra.policyTick.advance.map {
              case Some(ts) =>
                val next = RetryAttempt(ra.request, ts, ra.hotswap, response.headers.get[`Retry-After`])
                Right(next)
              case None => Left(response)
            }
          } else Left(response).pure[F]
      } // consume the response before next attempt
        .flatMap {
          case Left(response) => response.pure[F]
          case Right(next)    => nextAttempt(next)
        }

    Client[F] { (req: Request[F]) =>
      NonEmptyHotswap(client.run(req).attempt).flatMap { hotswap =>
        Resource.eval(PolicyTick.zeroth[F](zoneId, policy).flatMap(ts =>
          retryLoop(RetryAttempt[F](req, ts, hotswap, None))))
      }
    }
  }
}
