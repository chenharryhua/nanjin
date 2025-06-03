package com.github.chenharryhua.nanjin.datetime

import java.time.{Instant, LocalTime, ZoneId, ZonedDateTime}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps

final case class LocalTimeRange(start: LocalTime, duration: FiniteDuration, zoneId: ZoneId) {

  // start time inclusive, end time exclusive
  def inBetween(instant: Instant): Boolean =
    if (duration >= oneDay) true
    else if (duration <= Duration.Zero) false
    else {
      val st = LocalTime.MAX.minus(duration.toJava)
      val ld = instant.atZone(zoneId).toLocalTime
      val end = start.plus(duration.toJava)
      if (st.isAfter(start)) {
        ld.compareTo(start) >= 0 && ld.isBefore(end)
      } else {
        ld.compareTo(start) >= 0 || ld.isBefore(end)
      }
    }

  def inBetween(zonedDateTime: ZonedDateTime): Boolean =
    inBetween(zonedDateTime.toInstant)
}
