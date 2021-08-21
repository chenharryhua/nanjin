package com.github.chenharryhua.nanjin.datetime

import cron4s.Cron
import cron4s.expr.CronExpr

object crontabs {

  val hourly: CronExpr      = Cron.unsafeParse("0 0 */1 ? * *")
  val bihourly: CronExpr    = Cron.unsafeParse("0 0 */2 ? * *")
  val trihourly: CronExpr   = Cron.unsafeParse("0 0 */3 ? * *")
  val every4Hour: CronExpr  = Cron.unsafeParse("0 0 */4 ? * *")
  val every6Hour: CronExpr  = Cron.unsafeParse("0 0 */6 ? * *")
  val every8Hour: CronExpr  = Cron.unsafeParse("0 0 */8 ? * *")
  val every12Hour: CronExpr = Cron.unsafeParse("0 0 */12 ? * *")

  val minutely: CronExpr       = Cron.unsafeParse("0 */1 * ? * *")
  val biminutely: CronExpr     = Cron.unsafeParse("0 */2 * ? * *")
  val triminutely: CronExpr    = Cron.unsafeParse("0 */3 * ? * *")
  val every5Minutes: CronExpr  = Cron.unsafeParse("0 */5 * ? * *")
  val every10Minutes: CronExpr = Cron.unsafeParse("0 */10 * ? * *")
  val every15Minutes: CronExpr = Cron.unsafeParse("0 */15 * ? * *")

  val every5Seconds: CronExpr  = Cron.unsafeParse("*/5 * * ? * *")
  val every10Seconds: CronExpr = Cron.unsafeParse("*/10 * * ? * *")
  val every15Seconds: CronExpr = Cron.unsafeParse("*/15 * * ? * *")
}
