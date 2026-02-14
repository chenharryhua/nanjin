package com.github.chenharryhua.nanjin.common

import squants.time.Time
import squants.time.TimeConversions.timeToScalaDuration

import java.time.{Duration as JavaDuration, Instant, ZonedDateTime}
import scala.concurrent.duration.{Duration as ScalaDuration, DurationInt}
import scala.jdk.DurationConverters.JavaDurationOps

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

    private[this] val units: List[(ScalaDuration, String)] = List(
      1.day -> "day",
      1.hour -> "hour",
      1.minute -> "minute",
      1.second -> "second",
      1.millisecond -> "milli",
      1.microsecond -> "micro",
      1.nanosecond -> "nano"
    )

    override def format(duration: ScalaDuration): String = {
      val dur = if (duration < ScalaDuration.Zero) -duration else duration

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
            loop(rem - unitDur * qty.toDouble, tail, acc :+ s"$qty $label$plural", count - 1)
          } else loop(rem, tail, acc, count)
      }

      val parts = loop(dur, units, Nil, maxParts)
      if (parts.isEmpty) "0 second" else parts.mkString(" ")
    }
  }

  val defaultFormatter: DurationFormatter = new Formatter(2)

  def create(maxParts: Int): DurationFormatter = new Formatter(maxParts)
}
