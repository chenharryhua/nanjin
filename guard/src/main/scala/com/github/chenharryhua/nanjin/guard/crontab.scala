package com.github.chenharryhua.nanjin.guard

import cron4s.Cron
import cron4s.expr.CronExpr

object crontab {
  val midnightEveryday: CronExpr = Cron.unsafeParse("0 0 0 ? * *")

}
