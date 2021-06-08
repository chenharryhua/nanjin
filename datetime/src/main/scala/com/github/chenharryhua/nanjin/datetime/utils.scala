package com.github.chenharryhua.nanjin.datetime

import org.apache.commons.lang3.time.DurationFormatUtils

import java.time.{Instant, Duration => JavaDuration}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

object utils {

  /** always positive duration string
    */
  def mkDurationString(duration: Duration): String = {
    val dur: Duration = if (duration < Duration.Zero) -duration else duration
    if (dur < oneMillisec) s"${dur.toNanos} nanoseconds"
    else if (dur < oneSecond) s"${dur.toMillis} milliseconds"
    else DurationFormatUtils.formatDurationWords(dur.toMillis, true, true)
  }

  def mkDurationString(start: Instant, end: Instant): String =
    mkDurationString(FiniteDuration(JavaDuration.between(start, end).toNanos, TimeUnit.NANOSECONDS))

}
