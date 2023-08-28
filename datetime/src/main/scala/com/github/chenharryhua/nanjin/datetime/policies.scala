package com.github.chenharryhua.nanjin.datetime

import cats.effect.kernel.{Clock, Sync}
import cats.effect.std.Random
import cats.syntax.all.*
import cats.{Applicative, Functor}
import DurationFormatter.defaultFormatter
import cron4s.expr.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import org.typelevel.cats.time.instances.zoneid
import retry.PolicyDecision.{DelayAndRetry, GiveUp}
import retry.{RetryPolicies, RetryPolicy}

import java.time.{Duration, ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

object policies extends zoneid {
  type Policy[F[_]] = RetryPolicy[F]

  def jitterBackoff[F[_]: Sync](min: FiniteDuration, max: FiniteDuration): Policy[F] = {
    require(min < max, s"$min should be strictly smaller than $max")
    RetryPolicy.withShow(
      _ =>
        Random
          .scalaUtilRandom[F]
          .flatMap(_.betweenLong(min.toNanos, max.toNanos).map(d =>
            DelayAndRetry(FiniteDuration(d, TimeUnit.NANOSECONDS)))),
      pretty =
        show"jitterBackoff(minDelay=${defaultFormatter.format(min)}, maxDelay=${defaultFormatter.format(max)})"
    )
  }

  def cronBackoff[F[_]: Clock: Functor](cronExpr: CronExpr, zoneId: ZoneId): Policy[F] =
    RetryPolicy.withShow(
      _ =>
        Clock[F].realTimeInstant.map { ts =>
          val now: ZonedDateTime = ts.atZone(zoneId)
          cronExpr.next(now) match {
            case Some(next) => DelayAndRetry(Duration.between(now, next).toScala)
            case None       => GiveUp
          }
        },
      pretty = show"cronBackoff(cron=${cronExpr.show}, zoneId=${zoneId.show})"
    )

  // delegate to RetryPolicies
  def alwaysGiveUp[M[_]: Applicative]: Policy[M] = RetryPolicies.alwaysGiveUp[M]
  def constantDelay[M[_]: Applicative](delay: FiniteDuration): Policy[M] =
    RetryPolicies.constantDelay[M](delay)

  def exponentialBackoff[M[_]: Applicative](baseDelay: FiniteDuration): Policy[M] =
    RetryPolicies.exponentialBackoff[M](baseDelay)

  def limitRetries[M[_]: Applicative](maxRetries: Int): Policy[M] =
    RetryPolicies.limitRetries[M](maxRetries)

  def fibonacciBackoff[M[_]: Applicative](baseDelay: FiniteDuration): Policy[M] =
    RetryPolicies.fibonacciBackoff[M](baseDelay)

  def fullJitter[M[_]: Applicative](baseDelay: FiniteDuration): Policy[M] =
    RetryPolicies.fullJitter[M](baseDelay)

  def capDelay[M[_]: Applicative](cap: FiniteDuration, policy: Policy[M]): Policy[M] =
    RetryPolicies.capDelay[M](cap, policy)

  def limitRetriesByDelay[M[_]: Applicative](threshold: FiniteDuration, policy: Policy[M]): Policy[M] =
    RetryPolicies.limitRetriesByDelay[M](threshold, policy)

  def limitRetriesByCumulativeDelay[M[_]: Applicative](
    threshold: FiniteDuration,
    policy: Policy[M]
  ): Policy[M] =
    RetryPolicies.limitRetriesByCumulativeDelay[M](threshold, policy)
}
