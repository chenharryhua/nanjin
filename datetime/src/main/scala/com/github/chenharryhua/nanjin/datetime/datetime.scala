package com.github.chenharryhua.nanjin

import java.time.{Duration as JavaDuration, LocalDate, LocalDateTime, LocalTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.DurationConverters.JavaDurationOps

package object datetime {
  object instances extends DateTimeInstances

  def toLocalDateTime(ts: LocalTime): LocalDateTime = ts.atDate(LocalDate.now)
  def toLocalDateTime(ts: LocalDate): LocalDateTime = ts.atTime(LocalTime.MIDNIGHT)

  final val oneMillisecond: FiniteDuration = Duration(1, TimeUnit.MILLISECONDS)
  final val oneSecond: FiniteDuration = Duration(1, TimeUnit.SECONDS)
  final val oneMinute: FiniteDuration = Duration(1, TimeUnit.MINUTES)
  final val oneHour: FiniteDuration = Duration(1, TimeUnit.HOURS)
  final val oneDay: FiniteDuration = Duration(1, TimeUnit.DAYS)

  def dayResolution(localDateTime: LocalDateTime): LocalDate = localDateTime.toLocalDate

  def hourResolution(localDateTime: LocalDateTime): LocalDateTime =
    localDateTime.withMinute(0).withSecond(0).withNano(0)

  def minuteResolution(localDateTime: LocalDateTime): LocalDateTime =
    localDateTime.withSecond(0).withNano(0)

  def secondResolution(localDateTime: LocalDateTime): LocalDateTime =
    localDateTime.withNano(0)

  def distance(value: LocalTime, other: LocalTime): FiniteDuration = {
    val dur = JavaDuration.between(value, other)
    if (dur.isNegative) dur.plusHours(24).toScala else dur.toScala
  }
}
