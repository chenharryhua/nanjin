package com.github.chenharryhua.nanjin.common

import cats.implicits.catsSyntaxEq
import org.apache.commons.lang3.time.DurationFormatUtils

import java.time.{Duration as JavaDuration, Instant, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration as ScalaDuration, FiniteDuration}
import scala.jdk.DurationConverters.JavaDurationOps

trait DurationFormatter {

  /** Law: format(duration) == format(-duration)
    */
  def format(duration: ScalaDuration): String

  final def format(duration: JavaDuration): String = format(duration.toScala)
  final def format(start: Instant, end: Instant): String = format(JavaDuration.between(start, end))
  final def format(start: ZonedDateTime, end: ZonedDateTime): String =
    format(JavaDuration.between(start, end))
}

object DurationFormatter {
  final private val microsecond: FiniteDuration = ScalaDuration(1, TimeUnit.MICROSECONDS)
  final private val millisecond: FiniteDuration = ScalaDuration(1, TimeUnit.MILLISECONDS)
  final private val second: FiniteDuration = ScalaDuration(1, TimeUnit.SECONDS)
  final private val minute: FiniteDuration = ScalaDuration(1, TimeUnit.MINUTES)
  final private val hour: FiniteDuration = ScalaDuration(1, TimeUnit.HOURS)
  final private val day: FiniteDuration = ScalaDuration(1, TimeUnit.DAYS)

  final val defaultFormatter: DurationFormatter = (duration: ScalaDuration) => {
    val dur: ScalaDuration = if (duration < ScalaDuration.Zero) duration.neg() else duration
    if (dur < microsecond) {
      s"${dur.toNanos} nano"
    } else if (dur < millisecond) {
      val micro = dur.toMicros
      val nano = dur.toNanos % 1000
      if (nano === 0) s"$micro micro" else s"$micro micro $nano nano"
    } else if (dur < second) {
      val milli = dur.toMillis
      val micro = dur.toMicros % 1000
      if (micro === 0) s"$milli milli" else s"$milli milli $micro micro"
    } else if (dur < minute) {
      val millis = dur.toMillis
      val sec = DurationFormatUtils.formatDurationWords(millis, true, true)
      val milli = millis % 1000
      if (milli === 0) sec else s"$sec $milli milli"
    } else if (dur < hour)
      DurationFormatUtils.formatDurationWords(dur.toSeconds * 1000, true, true)
    else if (dur < day)
      DurationFormatUtils.formatDurationWords(dur.toMinutes * 60 * 1000, true, true)
    else
      DurationFormatUtils.formatDurationWords(dur.toHours * 3600 * 1000, true, true)
  }

  final val words: DurationFormatter = (duration: ScalaDuration) =>
    DurationFormatUtils.formatDurationWords(duration.toMillis, true, true)
}
