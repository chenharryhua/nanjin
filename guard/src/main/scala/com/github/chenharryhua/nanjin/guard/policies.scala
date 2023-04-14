package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Clock
import cats.syntax.all.*
import cats.{Applicative, Functor}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import cron4s.expr.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import org.typelevel.cats.time.instances.zoneid
import retry.PolicyDecision.{DelayAndRetry, GiveUp}
import retry.RetryPolicy

import java.time.{Duration, ZoneId, ZonedDateTime}
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.FiniteDuration

object policies extends zoneid {

  def jitterBackoff[F[_]: Applicative](min: FiniteDuration, max: FiniteDuration): RetryPolicy[F] = {
    require(min < max, s"$min should be strictly smaller than $max")
    RetryPolicy.liftWithShow(
      _ =>
        DelayAndRetry(
          FiniteDuration(
            ThreadLocalRandom.current().nextLong(min.toNanos, max.toNanos),
            TimeUnit.NANOSECONDS)),
      pretty =
        show"jitterBackoff(minDelay=${defaultFormatter.format(min)}, maxDelay=${defaultFormatter.format(max)})"
    )
  }

  def cronBackoff[F[_]: Clock: Functor](cronExpr: CronExpr, zoneId: ZoneId): RetryPolicy[F] =
    RetryPolicy.withShow(
      _ =>
        Clock[F].realTimeInstant.map { ts =>
          val now: ZonedDateTime = ts.atZone(zoneId)
          cronExpr.next(now) match {
            case Some(next) =>
              val nano: Long = Duration.between(now, next).toNanos
              DelayAndRetry(FiniteDuration(nano, TimeUnit.NANOSECONDS))
            case None => GiveUp
          }
        },
      pretty = show"cronBackoff(cron=${cronExpr.show}, zoneId=${zoneId.show})"
    )
}
