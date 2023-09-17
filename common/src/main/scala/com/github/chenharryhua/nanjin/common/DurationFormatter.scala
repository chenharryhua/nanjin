package com.github.chenharryhua.nanjin.common

import org.apache.commons.lang3.time.DurationFormatUtils

import java.time.{Duration as JavaDuration, Instant, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.DurationConverters.JavaDurationOps

trait DurationFormatter {

  /** Law: format(duration) == format(-duration)
    */
  def format(duration: Duration): String

  final def format(duration: JavaDuration): String       = format(duration.toScala)
  final def format(start: Instant, end: Instant): String = format(JavaDuration.between(start, end))
  final def format(start: ZonedDateTime, end: ZonedDateTime): String =
    format(JavaDuration.between(start, end))
}

object DurationFormatter {
  final private val microsecond: FiniteDuration = Duration(1, TimeUnit.MICROSECONDS)
  final private val millisecond: FiniteDuration = Duration(1, TimeUnit.MILLISECONDS)
  final private val second: FiniteDuration      = Duration(1, TimeUnit.SECONDS)
  final private val minute: FiniteDuration      = Duration(1, TimeUnit.MINUTES)
  final private val hour: FiniteDuration        = Duration(1, TimeUnit.HOURS)
  final private val day: FiniteDuration         = Duration(1, TimeUnit.DAYS)

  final val defaultFormatter: DurationFormatter = (duration: Duration) => {
    val dur: Duration = if (duration < Duration.Zero) duration.neg() else duration
    if (dur < microsecond) {
      val nano = dur.toNanos
      if (nano == 1) "1 nanosecond" else s"$nano nanoseconds"
    } else if (dur < millisecond) {
      val micro = dur.toMicros
      if (micro == 1) "1 microsecond" else s"$micro microseconds"
    } else if (dur < second) {
      val milli = dur.toMillis
      if (milli == 1) "1 millisecond" else s"$milli milliseconds"
    } else if (dur < minute) {
      val sec   = DurationFormatUtils.formatDurationWords(dur.toMillis, true, true)
      val milli = dur.toMillis % 1000
      if (milli == 0) sec else if (milli == 1) s"$sec 1 millisecond" else s"$sec $milli milliseconds"
    } else if (dur < hour)
      DurationFormatUtils.formatDurationWords(dur.toSeconds * 1000, true, true)
    else if (dur < day)
      DurationFormatUtils.formatDurationWords(dur.toMinutes * 60 * 1000, true, true)
    else
      DurationFormatUtils.formatDurationWords(dur.toHours * 3600 * 1000, true, true)
  }

  final val words: DurationFormatter = (duration: Duration) =>
    DurationFormatUtils.formatDurationWords(duration.toMillis, true, true)
}
