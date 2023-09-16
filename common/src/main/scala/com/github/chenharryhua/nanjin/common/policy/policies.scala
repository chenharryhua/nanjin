package com.github.chenharryhua.nanjin.common.policy

import cron4s.CronExpr

import java.time.{Duration, ZoneId}

object policies {
  def constant(baseDelay: Duration): Policy.Constant       = Policy.Constant(baseDelay)
  def exponential(baseDelay: Duration): Policy.Exponential = Policy.Exponential(baseDelay)
  def fixedPace(baseDelay: Duration): Policy.FixedPace     = Policy.FixedPace(baseDelay)
  def fibonacci(baseDelay: Duration): Policy.Fibonacci     = Policy.Fibonacci(baseDelay)

  def crontab(cronExpr: CronExpr, zoneId: ZoneId): Policy.Crontab = Policy.Crontab(cronExpr, zoneId)
}
