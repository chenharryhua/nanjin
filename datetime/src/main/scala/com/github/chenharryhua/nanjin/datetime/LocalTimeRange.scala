package com.github.chenharryhua.nanjin.datetime

import io.circe.generic.JsonCodec

import java.time.*
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** Represents a time range within a single day.
  *
  * @param start
  *   Inclusive start time of the range.
  * @param duration
  *   Duration of the range (can cross midnight).
  */
@JsonCodec
final case class LocalTimeRange(start: LocalTime, duration: Duration) {

  /** Check if a given LocalTime falls inside this range.
    *
    * Rules:
    *   - `start` is inclusive, `end` is exclusive
    *   - If duration >= 24 hours → always true
    *   - If duration is negative → always false
    *   - If range crosses midnight → use OR logic
    *   - Else → use AND logic
    */
  def inBetween(now: LocalTime): Boolean =
    if (duration.compareTo(oneDay.toJava) >= 0) true
    else if (duration.isNegative) false
    else {
      val end: LocalTime = start.plus(duration)
      val crossMidnight: Boolean = end.isBefore(start)
      if (crossMidnight) {
        now.compareTo(start) >= 0 || now.isBefore(end)
      } else {
        now.compareTo(start) >= 0 && now.isBefore(end)
      }
    }

  /** Delegates to `inBetween(LocalTime)` for convenience with ZonedDateTime.
    */
  def inBetween(zonedDateTime: ZonedDateTime): Boolean =
    inBetween(zonedDateTime.toLocalTime)

}

object LocalTimeRange {

  /** Convenience constructor converting Scala's FiniteDuration to Java Duration.
    *
    * Example:
    * {{{
    *   LocalTimeRange(LocalTime.of(10, 0), 2.hours)
    * }}}
    */
  def apply(start: LocalTime, duration: FiniteDuration): LocalTimeRange =
    LocalTimeRange(start, duration.toJava)
}
