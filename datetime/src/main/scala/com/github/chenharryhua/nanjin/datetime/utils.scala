package com.github.chenharryhua.nanjin.datetime

import org.apache.commons.lang3.time.DurationFormatUtils

import java.time.{Instant, Duration => JavaDuration}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

object utils {

  /** always positive duration string
    */
  def mkDurationString(duration: Duration): String = {
    val dur: Duration = if (duration < Duration.Zero) duration.neg() else duration
    if (dur < oneMillisec) s"${dur.toNanos} nanoseconds"
    else if (dur < oneSecond) {
      val milli = dur.toMillis
      if (milli == 1) s"1 millisecond"
      else s"$milli milliseconds"
    } else if (dur < oneMinute) {
      val sec   = DurationFormatUtils.formatDurationWords(dur.toMillis, true, true)
      val milli = dur.toMillis % 1000
      if (milli == 0) sec
      else if (milli == 1) s"$sec 1 millisecond"
      else s"$sec $milli milliseconds"
    } else if (dur < oneHour)
      DurationFormatUtils.formatDurationWords(dur.toSeconds * 1000, true, true)
    else if (dur < oneDay)
      DurationFormatUtils.formatDurationWords(dur.toMinutes * 60 * 1000, true, true)
    else
      DurationFormatUtils.formatDurationWords(dur.toHours * 3600 * 1000, true, true)
  }

  def mkDurationString(start: Instant, end: Instant): String =
    mkDurationString(FiniteDuration(JavaDuration.between(start, end).toNanos, TimeUnit.NANOSECONDS))

}
