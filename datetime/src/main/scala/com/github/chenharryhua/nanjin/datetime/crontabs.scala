package com.github.chenharryhua.nanjin.datetime

import cron4s.{Cron, CronExpr}

object crontabs {
  object daily {
    final val midnight: CronExpr = Cron.unsafeParse("0 0 0 ? * *")
    final val amOne: CronExpr    = Cron.unsafeParse("0 0 1 ? * *")
    final val amTwo: CronExpr    = Cron.unsafeParse("0 0 2 ? * *")
    final val amThree: CronExpr  = Cron.unsafeParse("0 0 3 ? * *")
    final val amFour: CronExpr   = Cron.unsafeParse("0 0 4 ? * *")
    final val amFive: CronExpr   = Cron.unsafeParse("0 0 5 ? * *")
    final val amSix: CronExpr    = Cron.unsafeParse("0 0 6 ? * *")
    final val amSeven: CronExpr  = Cron.unsafeParse("0 0 7 ? * *")
    final val amEight: CronExpr  = Cron.unsafeParse("0 0 8 ? * *")
    final val amNine: CronExpr   = Cron.unsafeParse("0 0 9 ? * *")
    final val amTen: CronExpr    = Cron.unsafeParse("0 0 10 ? * *")
    final val amEleven: CronExpr = Cron.unsafeParse("0 0 11 ? * *")
    final val noon: CronExpr     = Cron.unsafeParse("0 0 12 ? * *")
    final val pmOne: CronExpr    = Cron.unsafeParse("0 0 13 ? * *")
    final val pmTwo: CronExpr    = Cron.unsafeParse("0 0 14 ? * *")
    final val pmThree: CronExpr  = Cron.unsafeParse("0 0 15 ? * *")
    final val pmFour: CronExpr   = Cron.unsafeParse("0 0 16 ? * *")
    final val pmFive: CronExpr   = Cron.unsafeParse("0 0 17 ? * *")
    final val pmSix: CronExpr    = Cron.unsafeParse("0 0 18 ? * *")
    final val pmSeven: CronExpr  = Cron.unsafeParse("0 0 19 ? * *")
    final val pmEight: CronExpr  = Cron.unsafeParse("0 0 20 ? * *")
    final val pmNine: CronExpr   = Cron.unsafeParse("0 0 21 ? * *")
    final val pmTen: CronExpr    = Cron.unsafeParse("0 0 22 ? * *")
    final val pmEleven: CronExpr = Cron.unsafeParse("0 0 23 ? * *")
  }

  final val hourly: CronExpr       = Cron.unsafeParse("0 0 0-23 ? * *")
  final val bihourly: CronExpr     = Cron.unsafeParse("0 0 */2 ? * *")
  final val trihourly: CronExpr    = Cron.unsafeParse("0 0 */3 ? * *")
  final val everyHour: CronExpr    = hourly
  final val every2Hours: CronExpr  = bihourly
  final val every3Hours: CronExpr  = trihourly
  final val every4Hours: CronExpr  = Cron.unsafeParse("0 0 */4 ? * *")
  final val every6Hours: CronExpr  = Cron.unsafeParse("0 0 */6 ? * *")
  final val every8Hours: CronExpr  = Cron.unsafeParse("0 0 */8 ? * *")
  final val every12Hours: CronExpr = Cron.unsafeParse("0 0 */12 ? * *")

  final val minutely: CronExpr       = Cron.unsafeParse("0 0-59 * ? * *")
  final val biminutely: CronExpr     = Cron.unsafeParse("0 */2 * ? * *")
  final val triminutely: CronExpr    = Cron.unsafeParse("0 */3 * ? * *")
  final val everyMinute: CronExpr    = minutely
  final val every2Minutes: CronExpr  = biminutely
  final val every3Minutes: CronExpr  = triminutely
  final val every4Minutes: CronExpr  = Cron.unsafeParse("0 */4 * ? * *")
  final val every5Minutes: CronExpr  = Cron.unsafeParse("0 */5 * ? * *")
  final val every6Minutes: CronExpr  = Cron.unsafeParse("0 */6 * ? * *")
  final val every10Minutes: CronExpr = Cron.unsafeParse("0 */10 * ? * *")
  final val every12Minutes: CronExpr = Cron.unsafeParse("0 */12 * ? * *")
  final val every15Minutes: CronExpr = Cron.unsafeParse("0 */15 * ? * *")
  final val every20Minutes: CronExpr = Cron.unsafeParse("0 */20 * ? * *")
  final val every30Minutes: CronExpr = Cron.unsafeParse("0 */30 * ? * *")

  final val secondly: CronExpr       = Cron.unsafeParse("0-59 * * ? * *")
  final val bisecondly: CronExpr     = Cron.unsafeParse("*/2 * * ? * *")
  final val trisecondly: CronExpr    = Cron.unsafeParse("*/3 * * ? * *")
  final val everySecond: CronExpr    = secondly
  final val every2Seconds: CronExpr  = bisecondly
  final val every3Seconds: CronExpr  = trisecondly
  final val every4Seconds: CronExpr  = Cron.unsafeParse("*/4 * * ? * *")
  final val every5Seconds: CronExpr  = Cron.unsafeParse("*/5 * * ? * *")
  final val every6Seconds: CronExpr  = Cron.unsafeParse("*/6 * * ? * *")
  final val every10Seconds: CronExpr = Cron.unsafeParse("*/10 * * ? * *")
  final val every12Seconds: CronExpr = Cron.unsafeParse("*/12 * * ? * *")
  final val every15Seconds: CronExpr = Cron.unsafeParse("*/15 * * ? * *")
  final val every20Seconds: CronExpr = Cron.unsafeParse("*/20 * * ? * *")
  final val every30Seconds: CronExpr = Cron.unsafeParse("*/30 * * ? * *")

  final val z9w5: CronExpr = Cron.unsafeParse("0 0 9,17 ? * 1,2,3,4,5")
  final val c996: CronExpr = Cron.unsafeParse("0 0 9,21 ? * 1,2,3,4,5,6")
  final val c997: CronExpr = Cron.unsafeParse("0 0 9,21 ? * *")

  final val businessHour: CronExpr = Cron.unsafeParse("0 0 8,13,18 ? * *")
}
