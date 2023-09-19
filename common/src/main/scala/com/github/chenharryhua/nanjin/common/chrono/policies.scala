package com.github.chenharryhua.nanjin.common.chrono

import cron4s.CronExpr

import java.time.{Duration as JavaDuration, ZoneId}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object policies {
  def constant(baseDelay: JavaDuration): Policy   = Policy.Constant(baseDelay)
  def constant(baseDelay: FiniteDuration): Policy = constant(baseDelay.toJava)

  def exponential(baseDelay: JavaDuration): Policy   = Policy.Exponential(baseDelay)
  def exponential(baseDelay: FiniteDuration): Policy = exponential(baseDelay.toJava)

  def fixedPace(baseDelay: JavaDuration): Policy   = Policy.FixedPace(baseDelay)
  def fixedPace(baseDelay: FiniteDuration): Policy = fixedPace(baseDelay.toJava)

  def fibonacci(baseDelay: JavaDuration): Policy   = Policy.Fibonacci(baseDelay)
  def fibonacci(baseDelay: FiniteDuration): Policy = fibonacci(baseDelay.toJava)

  def crontab(cronExpr: CronExpr, zoneId: ZoneId): Policy = Policy.Crontab(cronExpr, zoneId)

  def jitter(min: JavaDuration, max: JavaDuration): Policy     = Policy.Jitter(min, max)
  def jitter(min: FiniteDuration, max: FiniteDuration): Policy = jitter(min.toJava, max.toJava)

  val giveUp: Policy = Policy.GiveUp
}
