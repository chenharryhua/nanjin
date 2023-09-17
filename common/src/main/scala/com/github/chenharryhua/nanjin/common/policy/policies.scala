package com.github.chenharryhua.nanjin.common.policy

import cron4s.CronExpr

import java.time.{Duration as JavaDuration, ZoneId}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object policies {
  def constant(baseDelay: JavaDuration): Policy.Constant   = Policy.Constant(baseDelay)
  def constant(baseDelay: FiniteDuration): Policy.Constant = constant(baseDelay.toJava)

  def exponential(baseDelay: JavaDuration): Policy.Exponential   = Policy.Exponential(baseDelay)
  def exponential(baseDelay: FiniteDuration): Policy.Exponential = exponential(baseDelay.toJava)

  def fixedPace(baseDelay: JavaDuration): Policy.FixedPace   = Policy.FixedPace(baseDelay)
  def fixedPace(baseDelay: FiniteDuration): Policy.FixedPace = fixedPace(baseDelay.toJava)

  def fibonacci(baseDelay: JavaDuration): Policy.Fibonacci   = Policy.Fibonacci(baseDelay)
  def fibonacci(baseDelay: FiniteDuration): Policy.Fibonacci = fibonacci(baseDelay.toJava)

  def crontab(cronExpr: CronExpr, zoneId: ZoneId): Policy.Crontab = Policy.Crontab(cronExpr, zoneId)

  def jitter(min: JavaDuration, max: JavaDuration): Policy.Jitter     = Policy.Jitter(min, max)
  def jitter(min: FiniteDuration, max: FiniteDuration): Policy.Jitter = jitter(min.toJava, max.toJava)

  val giveUp: Policy.GiveUp = Policy.GiveUp
}
