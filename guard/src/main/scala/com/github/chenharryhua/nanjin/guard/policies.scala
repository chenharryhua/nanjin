package com.github.chenharryhua.nanjin.guard

import cats.Applicative
import cats.implicits.{showInterpolator, toShow}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import cron4s.expr.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import retry.PolicyDecision.{DelayAndRetry, GiveUp}
import retry.RetryPolicy

import java.time.{Duration, Instant}
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

object policies {

  def jitterBackoff[F[_]: Applicative](min: FiniteDuration, max: FiniteDuration): RetryPolicy[F] =
    RetryPolicy.liftWithShow(
      _ =>
        DelayAndRetry(
          FiniteDuration(
            ThreadLocalRandom.current().nextLong(min.toNanos, max.toNanos),
            TimeUnit.NANOSECONDS)),
      show"jitterBackoff(minDelay=${defaultFormatter.format(min)}, maxDelay=${defaultFormatter.format(max)})"
    )

  def cronBackoff[F[_]: Applicative](cronExpr: CronExpr): RetryPolicy[F] =
    RetryPolicy.liftWithShow(
      _ => {
        val now = Instant.now()
        cronExpr.next(now) match {
          case Some(next) => DelayAndRetry(Duration.between(now, next).toScala)
          case None       => GiveUp
        }
      },
      show"cronBackoff(cron=${cronExpr.show})"
    )
}
