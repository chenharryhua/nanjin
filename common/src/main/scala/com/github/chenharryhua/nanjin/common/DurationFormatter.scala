package com.github.chenharryhua.nanjin.common

import squants.time.Time
import squants.time.TimeConversions.timeToScalaDuration

import java.time.{Duration as JavaDuration, Instant, ZonedDateTime}
import scala.concurrent.duration.{Duration as ScalaDuration, DurationInt}
import scala.jdk.DurationConverters.JavaDurationOps

/** Formats a duration into a human-readable string with configurable granularity.
  *
  * Accepts Scala durations, Java durations, instants, zoned date-times, and squants `Time` values.
  *
  * A finite `ScalaDuration` is rendered as a breakdown into the largest units (e.g. `"1 hour 5 minutes"`). A
  * non-finite `ScalaDuration` (`Duration.Inf`, `Duration.MinusInf`, `Duration.Undefined`) has no such
  * breakdown and is rendered via its own `toString` (`"Duration.Inf"`, etc.) rather than throwing.
  *
  * The `JavaDuration`, `Instant`, `ZonedDateTime`, and `Time` overloads carry no non-finite value, but they
  * are not total either: each converts to a `ScalaDuration` first, which holds at most `Long.MaxValue`
  * nanoseconds (~292 years). A wider span throws `IllegalArgumentException` from the conversion instead of
  * being formatted, so `format(Instant.EPOCH, Instant.MAX)` throws. Real timestamps are nowhere near that
  * bound; sentinel values such as `Instant.MIN`/`Instant.MAX` are.
  */
trait DurationFormatter {
  def format(duration: ScalaDuration): String

  final def format(duration: JavaDuration): String = format(duration.toScala)
  final def format(start: Instant, end: Instant): String = format(JavaDuration.between(start, end))
  final def format(start: ZonedDateTime, end: ZonedDateTime): String = format(
    JavaDuration.between(start, end))
  final def format(time: Time): String = format(timeToScalaDuration(time))
}

object DurationFormatter {

  final private class Formatter(maxParts: Int) extends DurationFormatter {

    private val units: List[(ScalaDuration, String)] = List(
      1.day -> "day",
      1.hour -> "hour",
      1.minute -> "minute",
      1.second -> "second",
      1.millisecond -> "milli",
      1.microsecond -> "micro",
      1.nanosecond -> "nano"
    )

    override def format(duration: ScalaDuration): String =
      if (!duration.isFinite) duration.toString
      else formatFinite(duration)

    private def formatFinite(duration: ScalaDuration): String = {
      val isNegative = duration < ScalaDuration.Zero
      val dur = if (isNegative) -duration else duration

      @annotation.tailrec
      def loop(
        rem: ScalaDuration,
        remainingUnits: List[(ScalaDuration, String)],
        acc: List[String],
        count: Int
      ): List[String] = (remainingUnits, count) match {
        case (_, 0)                        => acc
        case (Nil, _)                      => acc
        case ((unitDur, label) :: tail, _) =>
          val qty = rem.toNanos / unitDur.toNanos
          if (qty > 0) {
            val plural = if (qty > 1) "s" else ""
            val consumed = ScalaDuration.fromNanos(unitDur.toNanos * qty)
            loop(rem - consumed, tail, acc :+ s"$qty $label$plural", count - 1)
          } else loop(rem, tail, acc, count)
      }

      val parts = loop(dur, units, Nil, maxParts)
      val formatted = if (parts.isEmpty) "0 second" else parts.mkString(" ")
      if (isNegative) s"-$formatted" else formatted
    }
  }

  val defaultFormatter: DurationFormatter = new Formatter(2)

  /** A formatter showing at most `maxParts` units, largest first.
    *
    * @param maxParts
    *   must be positive. Seven units are available (day down to nano), so a larger value simply shows all of
    *   them. Non-positive values are rejected rather than accepted silently: zero would render every duration
    *   as `"0 second"`, and a negative value would read as "no limit".
    */
  def create(maxParts: Int): DurationFormatter = {
    require(maxParts > 0, s"maxParts must be positive, but was $maxParts")
    new Formatter(maxParts)
  }
}
