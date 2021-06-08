package com.github.chenharryhua.nanjin.datetime

import java.time.LocalTime
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

final case class NJLocalTime(value: LocalTime) {

  def distance(other: LocalTime): FiniteDuration = {
    val diff: Long = other.toSecondOfDay.toLong - value.toSecondOfDay
    FiniteDuration(if (diff >= 0) diff else diff + 24 * 3600, TimeUnit.SECONDS)
  }
}
