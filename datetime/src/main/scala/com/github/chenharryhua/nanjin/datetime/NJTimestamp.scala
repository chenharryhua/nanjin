package com.github.chenharryhua.nanjin.datetime

import java.sql.Timestamp
import java.time._
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.util.concurrent.TimeUnit

import cats.implicits._
import cats.{Hash, Order, Show}
import monocle.Iso

import scala.concurrent.duration.{Duration, FiniteDuration}

final case class NJTimestamp(milliseconds: Long) extends AnyVal {
  def timeUnit: TimeUnit   = TimeUnit.MILLISECONDS
  def instant: Instant     = Instant.ofEpochMilli(milliseconds)
  def utc: ZonedDateTime   = instant.atZone(utcTime)
  def local: ZonedDateTime = atZone(ZoneId.systemDefault())

  def atZone(zoneId: ZoneId): ZonedDateTime = instant.atZone(zoneId)

  @throws[Exception]
  def atZone(zoneId: String): ZonedDateTime = atZone(ZoneId.of(zoneId))

  def javaLong: java.lang.Long = milliseconds

  def yearStr(zoneId: ZoneId): String  = f"${atZone(zoneId).getYear}%4d"
  def monthStr(zoneId: ZoneId): String = f"${atZone(zoneId).getMonthValue}%02d"
  def dayStr(zoneId: ZoneId): String   = f"${atZone(zoneId).getDayOfMonth}%02d"

  def `yyyy-mm-dd`(zoneId: ZoneId): String =
    s"${yearStr(zoneId)}-${monthStr(zoneId)}-${dayStr(zoneId)}"

  def `Year=yyyy/Month=mm/Day=dd`(zoneId: ZoneId): String =
    s"Year=${yearStr(zoneId)}/Month=${monthStr(zoneId)}/Day=${dayStr(zoneId)}"

  def localDate(zoneId: ZoneId): LocalDate = atZone(zoneId).toLocalDate

  def hourResolution(zoneId: ZoneId): LocalDateTime = {
    val dt: LocalDateTime = atZone(zoneId).toLocalDateTime
    LocalDateTime.of(dt.toLocalDate, LocalTime.of(dt.getHour, 0))
  }

  def minuteResolution(zoneId: ZoneId): LocalDateTime = {
    val dt: LocalDateTime = atZone(zoneId).toLocalDateTime
    LocalDateTime.of(dt.toLocalDate, LocalTime.of(dt.getHour, dt.getMinute))
  }

  def minus(amount: Long, unit: TemporalUnit): NJTimestamp =
    NJTimestamp(instant.minus(amount, unit))

  def plus(amount: Long, unit: TemporalUnit): NJTimestamp =
    NJTimestamp(instant.plus(amount, unit))

  def minus(amount: Long): NJTimestamp = minus(amount, ChronoUnit.MILLIS)
  def plus(amount: Long): NJTimestamp  = plus(amount, ChronoUnit.MILLIS)

  def minus(other: NJTimestamp): FiniteDuration =
    Duration(this.milliseconds - other.milliseconds, timeUnit)

  def -(other: NJTimestamp): FiniteDuration = minus(other)

  override def toString: String = local.toString
}

object NJTimestamp {
  def apply(ts: Timestamp): NJTimestamp      = NJTimestamp(ts.getTime)
  def apply(ts: Instant): NJTimestamp        = NJTimestamp(ts.toEpochMilli)
  def apply(ts: ZonedDateTime): NJTimestamp  = apply(ts.toInstant)
  def apply(ts: OffsetDateTime): NJTimestamp = apply(ts.toInstant)

  def apply(ts: LocalDateTime, zoneId: ZoneId): NJTimestamp =
    apply(ts.atZone(zoneId).toInstant)

  def apply(ts: LocalDate, zoneId: ZoneId): NJTimestamp =
    apply(LocalDateTime.of(ts, LocalTime.MIDNIGHT), zoneId)

  def apply(lt: LocalTime, zoneId: ZoneId): NJTimestamp =
    apply(LocalDateTime.of(LocalDate.now(), lt), zoneId)

  def now(clock: Clock): NJTimestamp = NJTimestamp(Instant.now(clock))
  def now(): NJTimestamp             = NJTimestamp(Instant.now)

  val isoKafkaTimestamp: Iso[NJTimestamp, Timestamp] =
    Iso[NJTimestamp, Timestamp]((a: NJTimestamp) => new Timestamp(a.milliseconds))((b: Timestamp) =>
      NJTimestamp(b.getTime))

  implicit val njTimestampInstance
    : Hash[NJTimestamp] with Order[NJTimestamp] with Show[NJTimestamp] =
    new Hash[NJTimestamp] with Order[NJTimestamp] with Show[NJTimestamp] {
      override def hash(x: NJTimestamp): Int = x.hashCode

      override def compare(x: NJTimestamp, y: NJTimestamp): Int =
        x.milliseconds.compareTo(y.milliseconds)

      override def show(x: NJTimestamp): String = x.toString
    }
}
