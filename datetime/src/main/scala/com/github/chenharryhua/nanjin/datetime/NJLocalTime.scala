package com.github.chenharryhua.nanjin.datetime

import java.time.{Instant, LocalTime, ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.compat.java8.DurationConverters.*
import scala.concurrent.duration.{Duration, FiniteDuration}

final case class NJLocalTime(value: LocalTime) {

  def distance(other: LocalTime): FiniteDuration = {
    val diff: Long = other.toSecondOfDay.toLong - value.toSecondOfDay
    FiniteDuration(if (diff >= 0) diff else diff + 24 * 3600, TimeUnit.SECONDS)
  }
}

final case class NJLocalTimeRange(start: LocalTime, duration: FiniteDuration, zoneId: ZoneId) {

  // start time inclusive, end time exclusive
  def isInBetween(instant: Instant): Boolean =
    if (duration >= oneDay) true
    else if (duration <= Duration.Zero) false
    else {
      val st  = LocalTime.MAX.minus(duration.toJava)
      val ld  = instant.atZone(zoneId).toLocalTime
      val end = start.plus(duration.toJava)
      if (st.isAfter(start)) {
        ld.compareTo(start) >= 0 && ld.isBefore(end)
      } else {
        ld.compareTo(start) >= 0 || ld.isBefore(end)
      }
    }

  def isInBetween(zonedDateTime: ZonedDateTime): Boolean =
    isInBetween(zonedDateTime.toInstant)
}
