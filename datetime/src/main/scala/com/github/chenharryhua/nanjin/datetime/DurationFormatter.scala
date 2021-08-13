package com.github.chenharryhua.nanjin.datetime

import org.apache.commons.lang3.time.DurationFormatUtils

import java.time.{Duration as JavaDuration, Instant, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

trait DurationFormatter {

  /** Law: format(duration) == format(-duration)
    */
  def format(duration: Duration): String

  final def format(duration: JavaDuration): String =
    format(FiniteDuration(duration.toNanos, TimeUnit.NANOSECONDS))

  final def format(start: Instant, end: Instant): String =
    format(JavaDuration.between(start, end))

  final def format(start: ZonedDateTime, end: ZonedDateTime): String =
    format(start.toInstant, end.toInstant)
}

object DurationFormatter {

  val defaultFormatter: DurationFormatter = (duration: Duration) => {
    val dur: Duration = if (duration < Duration.Zero) duration.neg() else duration
    if (dur < oneMillisec) {
      val nano = dur.toNanos
      if (nano == 1) "1 nanosecond" else s"$nano nanoseconds"
    } else if (dur < oneSecond) {
      val milli = dur.toMillis
      if (milli == 1) "1 millisecond" else s"$milli milliseconds"
    } else if (dur < oneMinute) {
      val sec   = DurationFormatUtils.formatDurationWords(dur.toMillis, true, true)
      val milli = dur.toMillis % 1000
      if (milli == 0) sec else if (milli == 1) s"$sec 1 millisecond" else s"$sec $milli milliseconds"
    } else if (dur < oneHour)
      DurationFormatUtils.formatDurationWords(dur.toSeconds * 1000, true, true)
    else if (dur < oneDay)
      DurationFormatUtils.formatDurationWords(dur.toMinutes * 60 * 1000, true, true)
    else
      DurationFormatUtils.formatDurationWords(dur.toHours * 3600 * 1000, true, true)
  }

  val words: DurationFormatter = (duration: Duration) =>
    DurationFormatUtils.formatDurationWords(duration.toMillis, true, true)
}
