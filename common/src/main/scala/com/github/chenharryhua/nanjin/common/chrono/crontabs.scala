package com.github.chenharryhua.nanjin.common.chrono

import cron4s.{Cron, CronExpr}

object crontabs {
  object yearly {
    final val january: CronExpr = Cron.unsafeParse("0 0 0 1 1 ?")
    final val february: CronExpr = Cron.unsafeParse("0 0 0 1 2 ?")
    final val march: CronExpr = Cron.unsafeParse("0 0 0 1 3 ?")
    final val april: CronExpr = Cron.unsafeParse("0 0 0 1 4 ?")
    final val may: CronExpr = Cron.unsafeParse("0 0 0 1 5 ?")
    final val june: CronExpr = Cron.unsafeParse("0 0 0 1 6 ?")
    final val july: CronExpr = Cron.unsafeParse("0 0 0 1 7 ?")
    final val august: CronExpr = Cron.unsafeParse("0 0 0 1 8 ?")
    final val september: CronExpr = Cron.unsafeParse("0 0 0 1 9 ?")
    final val october: CronExpr = Cron.unsafeParse("0 0 0 1 10 ?")
    final val november: CronExpr = Cron.unsafeParse("0 0 0 1 11 ?")
    final val december: CronExpr = Cron.unsafeParse("0 0 0 1 12 ?")
  }

  final val monthly: CronExpr = Cron.unsafeParse("0 0 0 1 * ?")

  object weekly { // notice the difference in https://crontab.guru, sunday is 0 0 * * 0
    final val monday: CronExpr = Cron.unsafeParse("0 0 0 ? * 0")
    final val tuesday: CronExpr = Cron.unsafeParse("0 0 0 ? * 1")
    final val wednesday: CronExpr = Cron.unsafeParse("0 0 0 ? * 2")
    final val thursday: CronExpr = Cron.unsafeParse("0 0 0 ? * 3")
    final val friday: CronExpr = Cron.unsafeParse("0 0 0 ? * 4")
    final val saturday: CronExpr = Cron.unsafeParse("0 0 0 ? * 5")
    final val sunday: CronExpr = Cron.unsafeParse("0 0 0 ? * 6")
  }

  object daily {
    final val midnight: CronExpr = Cron.unsafeParse("0 0 0 ? * *")
    final val oneAM: CronExpr = Cron.unsafeParse("0 0 1 ? * *")
    final val twoAM: CronExpr = Cron.unsafeParse("0 0 2 ? * *")
    final val threeAM: CronExpr = Cron.unsafeParse("0 0 3 ? * *")
    final val fourAM: CronExpr = Cron.unsafeParse("0 0 4 ? * *")
    final val fiveAM: CronExpr = Cron.unsafeParse("0 0 5 ? * *")
    final val sixAM: CronExpr = Cron.unsafeParse("0 0 6 ? * *")
    final val sevenAM: CronExpr = Cron.unsafeParse("0 0 7 ? * *")
    final val eightAM: CronExpr = Cron.unsafeParse("0 0 8 ? * *")
    final val nineAM: CronExpr = Cron.unsafeParse("0 0 9 ? * *")
    final val tenAM: CronExpr = Cron.unsafeParse("0 0 10 ? * *")
    final val elevenAM: CronExpr = Cron.unsafeParse("0 0 11 ? * *")
    final val noon: CronExpr = Cron.unsafeParse("0 0 12 ? * *")
    final val onePM: CronExpr = Cron.unsafeParse("0 0 13 ? * *")
    final val twoPM: CronExpr = Cron.unsafeParse("0 0 14 ? * *")
    final val threePM: CronExpr = Cron.unsafeParse("0 0 15 ? * *")
    final val fourPM: CronExpr = Cron.unsafeParse("0 0 16 ? * *")
    final val fivePM: CronExpr = Cron.unsafeParse("0 0 17 ? * *")
    final val sixPM: CronExpr = Cron.unsafeParse("0 0 18 ? * *")
    final val sevenPM: CronExpr = Cron.unsafeParse("0 0 19 ? * *")
    final val eightPM: CronExpr = Cron.unsafeParse("0 0 20 ? * *")
    final val ninePM: CronExpr = Cron.unsafeParse("0 0 21 ? * *")
    final val tenPM: CronExpr = Cron.unsafeParse("0 0 22 ? * *")
    final val elevenPM: CronExpr = Cron.unsafeParse("0 0 23 ? * *")
  }

  final val hourly: CronExpr = Cron.unsafeParse("0 0 0-23 ? * *")
  final val bihourly: CronExpr = Cron.unsafeParse("0 0 */2 ? * *")
  final val trihourly: CronExpr = Cron.unsafeParse("0 0 */3 ? * *")
  final val everyHour: CronExpr = hourly
  final val every2Hours: CronExpr = bihourly
  final val every3Hours: CronExpr = trihourly
  final val every4Hours: CronExpr = Cron.unsafeParse("0 0 */4 ? * *")
  final val every6Hours: CronExpr = Cron.unsafeParse("0 0 */6 ? * *")
  final val every8Hours: CronExpr = Cron.unsafeParse("0 0 */8 ? * *")
  final val every12Hours: CronExpr = Cron.unsafeParse("0 0 */12 ? * *")

  final val minutely: CronExpr = Cron.unsafeParse("0 0-59 * ? * *")
  final val biminutely: CronExpr = Cron.unsafeParse("0 */2 * ? * *")
  final val triminutely: CronExpr = Cron.unsafeParse("0 */3 * ? * *")
  final val everyMinute: CronExpr = minutely
  final val every2Minutes: CronExpr = biminutely
  final val every3Minutes: CronExpr = triminutely
  final val every4Minutes: CronExpr = Cron.unsafeParse("0 */4 * ? * *")
  final val every5Minutes: CronExpr = Cron.unsafeParse("0 */5 * ? * *")
  final val every6Minutes: CronExpr = Cron.unsafeParse("0 */6 * ? * *")
  final val every10Minutes: CronExpr = Cron.unsafeParse("0 */10 * ? * *")
  final val every12Minutes: CronExpr = Cron.unsafeParse("0 */12 * ? * *")
  final val every15Minutes: CronExpr = Cron.unsafeParse("0 */15 * ? * *")
  final val every20Minutes: CronExpr = Cron.unsafeParse("0 */20 * ? * *")
  final val every30Minutes: CronExpr = Cron.unsafeParse("0 */30 * ? * *")

  final val secondly: CronExpr = Cron.unsafeParse("0-59 * * ? * *")
  final val bisecondly: CronExpr = Cron.unsafeParse("*/2 * * ? * *")
  final val trisecondly: CronExpr = Cron.unsafeParse("*/3 * * ? * *")
  final val everySecond: CronExpr = secondly
  final val every2Seconds: CronExpr = bisecondly
  final val every3Seconds: CronExpr = trisecondly
  final val every4Seconds: CronExpr = Cron.unsafeParse("*/4 * * ? * *")
  final val every5Seconds: CronExpr = Cron.unsafeParse("*/5 * * ? * *")
  final val every6Seconds: CronExpr = Cron.unsafeParse("*/6 * * ? * *")
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
