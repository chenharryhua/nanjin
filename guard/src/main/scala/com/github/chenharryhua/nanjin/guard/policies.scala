package com.github.chenharryhua.nanjin.guard

import cats.Functor
import cats.effect.kernel.{Clock, Sync}
import cats.effect.std.Random
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import cron4s.expr.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import org.typelevel.cats.time.instances.zoneid
import retry.PolicyDecision.{DelayAndRetry, GiveUp}
import retry.RetryPolicy

import java.time.{Duration, ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

object policies extends zoneid {

  def jitterBackoff[F[_]: Sync](min: FiniteDuration, max: FiniteDuration): RetryPolicy[F] = {
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

  def cronBackoff[F[_]: Clock: Functor](cronExpr: CronExpr, zoneId: ZoneId): RetryPolicy[F] =
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
}
