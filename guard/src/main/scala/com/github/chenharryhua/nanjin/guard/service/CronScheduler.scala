package com.github.chenharryhua.nanjin.guard.service

import cats.effect.Temporal
import cats.syntax.all.*
import cron4s.CronExpr
import cron4s.lib.javatime.javaTemporalInstance
import fs2.Pull
import fs2.Stream
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import java.time.{Duration, LocalDateTime, ZoneId}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

final private class CronScheduler(zoneId: ZoneId) {

  def awakeEvery[F[_]](cronExpr: CronExpr)(implicit F: Temporal[F]): Stream[F, Long] = {
    def go(wakeUpTime: LocalDateTime, idx: Long): Pull[F, Long, Unit] =
      Pull.eval(F.realTimeInstant.map(_.atZone(zoneId).toLocalDateTime)).flatMap { now =>
        if (now.isBefore(wakeUpTime)) { // sleep and do nothing
          val fd: FiniteDuration = Duration.between(now, wakeUpTime).toScala
          Pull.sleep(fd) >> go(wakeUpTime, idx)
        } else {
          cronExpr.next(now) match {
            case None => Pull.done
            case Some(next) =>
              val fd: FiniteDuration = Duration.between(now, next).toScala
              Pull.output1(idx) >> Pull.sleep(fd) >> go(next, idx + 1)
          }
        }
      }

    Stream.eval(F.realTimeInstant.map(now => cronExpr.next(now.atZone(zoneId).toLocalDateTime))).flatMap {
      case None         => Stream.empty
      case Some(wakeup) => go(wakeup, 0).stream
    }
  }

  def awakeEvery[F[_]](policy: RetryPolicy[F])(implicit F: Temporal[F]): Stream[F, Long] = {
    def go(status: RetryStatus, idx: Long): Pull[F, Long, Unit] =
      Pull.eval(policy.decideNextRetry(status)).flatMap {
        case PolicyDecision.GiveUp => Pull.done
        case PolicyDecision.DelayAndRetry(delay) =>
          Pull.output1(idx) >> Pull.sleep(delay) >> go(status.addRetry(delay), idx + 1)
      }
    go(RetryStatus.NoRetriesYet, 0).stream
  }
}
