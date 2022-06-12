package com.github.chenharryhua.nanjin

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

package object datetime {
  object instances extends DateTimeInstances

  def toLocalDateTime(ts: LocalTime): LocalDateTime = ts.atDate(LocalDate.now)
  def toLocalDateTime(ts: LocalDate): LocalDateTime = ts.atTime(LocalTime.MIDNIGHT)

  final val utcTime: ZoneId       = ZoneId.of("Etc/UTC")
  final val darwinTime: ZoneId    = ZoneId.of("Australia/Darwin")
  final val sydneyTime: ZoneId    = ZoneId.of("Australia/Sydney")
  final val beijingTime: ZoneId   = ZoneId.of("Asia/Shanghai")
  final val singaporeTime: ZoneId = ZoneId.of("Asia/Singapore")
  final val mumbaiTime: ZoneId    = ZoneId.of("Asia/Kolkata")
  final val newyorkTime: ZoneId   = ZoneId.of("America/New_York")
  final val londonTime: ZoneId    = ZoneId.of("Europe/London")
  final val berlinTime: ZoneId    = ZoneId.of("Europe/Berlin")
  final val cairoTime: ZoneId     = ZoneId.of("Africa/Cairo")

  final val oneMillisec: FiniteDuration = Duration(1, TimeUnit.MILLISECONDS)
  final val oneSecond: FiniteDuration   = Duration(1, TimeUnit.SECONDS)
  final val oneMinute: FiniteDuration   = Duration(1, TimeUnit.MINUTES)
  final val oneHour: FiniteDuration     = Duration(1, TimeUnit.HOURS)
  final val oneDay: FiniteDuration      = Duration(1, TimeUnit.DAYS)

}
