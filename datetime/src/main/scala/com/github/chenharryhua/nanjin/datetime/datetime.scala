package com.github.chenharryhua.nanjin

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}

package object datetime extends DateTimeInstances with Isos {

  def toLocalDateTime(ts: LocalTime): LocalDateTime = ts.atDate(LocalDate.now)
  def toLocalDateTime(ts: LocalDate): LocalDateTime = ts.atTime(LocalTime.MIDNIGHT)

  val utcTime: ZoneId       = ZoneId.of("Etc/UTC")
  val darwinTime: ZoneId    = ZoneId.of("Australia/Darwin")
  val sydneyTime: ZoneId    = ZoneId.of("Australia/Sydney")
  val beijingTime: ZoneId   = ZoneId.of("Asia/Shanghai")
  val singaporeTime: ZoneId = ZoneId.of("Asia/Singapore")
  val mumbaiTime: ZoneId    = ZoneId.of("Asia/Kolkata")
  val newyorkTime: ZoneId   = ZoneId.of("America/New_York")
  val londonTime: ZoneId    = ZoneId.of("Europe/London")
  val berlinTime: ZoneId    = ZoneId.of("Europe/Berlin")
  val cairoTime: ZoneId     = ZoneId.of("Africa/Cairo")
}
