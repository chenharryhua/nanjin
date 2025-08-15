package com.github.chenharryhua.nanjin.datetime

import io.circe.generic.JsonCodec

import java.time.*
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final case class LocalTimeRange(start: LocalTime, duration: Duration) {

  // start time inclusive, end time exclusive
  def inBetween(now: LocalTime): Boolean =
    if (duration.compareTo(oneDay.toJava) >= 0) true
    else if (duration.isNegative) false
    else {
      val crossMidnight = LocalTime.MAX.minus(duration).isAfter(start)
      val end = start.plus(duration)
      if (crossMidnight) {
        now.compareTo(start) >= 0 && now.isBefore(end)
      } else {
        now.compareTo(start) >= 0 || now.isBefore(end)
      }
    }

  def inBetween(zonedDateTime: ZonedDateTime): Boolean =
    inBetween(zonedDateTime.toLocalTime)

}

object LocalTimeRange {
  def apply(start: LocalTime, duration: FiniteDuration): LocalTimeRange =
    LocalTimeRange(start, duration.toJava)
}
