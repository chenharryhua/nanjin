package com.github.chenharryhua.nanjin.datetime

import cron4s.Cron
import cron4s.expr.CronExpr

object crontabs {

  val hourly: CronExpr       = Cron.unsafeParse("0 0 0-23 ? * *")
  val bihourly: CronExpr     = Cron.unsafeParse("0 0 */2 ? * *")
  val trihourly: CronExpr    = Cron.unsafeParse("0 0 */3 ? * *")
  val every4Hours: CronExpr  = Cron.unsafeParse("0 0 */4 ? * *")
  val every6Hours: CronExpr  = Cron.unsafeParse("0 0 */6 ? * *")
  val every8Hours: CronExpr  = Cron.unsafeParse("0 0 */8 ? * *")
  val every12Hours: CronExpr = Cron.unsafeParse("0 0 */12 ? * *")

  val minutely: CronExpr       = Cron.unsafeParse("0 0-59 * ? * *")
  val biminutely: CronExpr     = Cron.unsafeParse("0 */2 * ? * *")
  val triminutely: CronExpr    = Cron.unsafeParse("0 */3 * ? * *")
  val every5Minutes: CronExpr  = Cron.unsafeParse("0 */5 * ? * *")
  val every10Minutes: CronExpr = Cron.unsafeParse("0 */10 * ? * *")
  val every15Minutes: CronExpr = Cron.unsafeParse("0 */15 * ? * *")
  val every20Minutes: CronExpr = Cron.unsafeParse("0 */20 * ? * *")
  val every30Minutes: CronExpr = Cron.unsafeParse("0 */30 * ? * *")

  val secondly: CronExpr       = Cron.unsafeParse("0-59 * * ? * *")
  val bisecondly: CronExpr     = Cron.unsafeParse("*/2 * * ? * *")
  val trisecondly: CronExpr    = Cron.unsafeParse("*/3 * * ? * *")
  val every5Seconds: CronExpr  = Cron.unsafeParse("*/5 * * ? * *")
  val every10Seconds: CronExpr = Cron.unsafeParse("*/10 * * ? * *")
  val every15Seconds: CronExpr = Cron.unsafeParse("*/15 * * ? * *")
  val every20Seconds: CronExpr = Cron.unsafeParse("*/20 * * ? * *")
  val every30Seconds: CronExpr = Cron.unsafeParse("*/30 * * ? * *")

  val z9w5: CronExpr = Cron.unsafeParse("0 0 9,17 ? * 1,2,3,4,5")
  val c996: CronExpr = Cron.unsafeParse("0 0 9,21 ? * 1,2,3,4,5,6")
  val c997: CronExpr = Cron.unsafeParse("0 0 9,21 ? * *")

  val businessHour: CronExpr = Cron.unsafeParse("0 0 8,13,18 ? * *")
}
