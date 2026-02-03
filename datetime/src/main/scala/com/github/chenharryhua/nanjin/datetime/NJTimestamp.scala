package com.github.chenharryhua.nanjin.datetime

import cats.{Hash, Order, Show}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.zones.utcTime

import java.sql.Timestamp
import java.time.*
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

final case class NJTimestamp(milliseconds: Long) extends AnyVal {
  def timeUnit: TimeUnit = TimeUnit.MILLISECONDS
  def instant: Instant = Instant.ofEpochMilli(milliseconds)
  def utc: ZonedDateTime = instant.atZone(utcTime)
  def sqlTimestamp: Timestamp = new Timestamp(milliseconds)

  def atZone(zoneId: ZoneId): ZonedDateTime = instant.atZone(zoneId)

  def atZone(zoneId: String): ZonedDateTime = atZone(ZoneId.of(zoneId))

  def javaLong: java.lang.Long = milliseconds

  @SuppressWarnings(Array("AvoidOperatorOverload", "MethodNames"))
  def `Year=yyyy/Month=mm/Day=dd`(zoneId: ZoneId): String =
    codec.year_month_day(atZone(zoneId).toLocalDate)

  @SuppressWarnings(Array("AvoidOperatorOverload", "MethodNames"))
  def `Year=yyyy/Month=mm/Day=dd/Hour=hh`(zoneId: ZoneId): String =
    codec.year_month_day_hour(atZone(zoneId).toLocalDateTime)

  @SuppressWarnings(Array("AvoidOperatorOverload", "MethodNames"))
  def `Year=yyyy/Month=mm/Day=dd/Hour=hh/Minute=mm`(zoneId: ZoneId): String =
    codec.year_month_day_hour_minute(atZone(zoneId).toLocalDateTime)

  def minus(amount: Long, unit: TemporalUnit): NJTimestamp =
    NJTimestamp(instant.minus(amount, unit))

  def plus(amount: Long, unit: TemporalUnit): NJTimestamp =
    NJTimestamp(instant.plus(amount, unit))

  def minus(amount: Long): NJTimestamp = minus(amount, ChronoUnit.MILLIS)
  def plus(amount: Long): NJTimestamp = plus(amount, ChronoUnit.MILLIS)

  def minus(other: NJTimestamp): FiniteDuration =
    Duration(this.milliseconds - other.milliseconds, timeUnit)

  def -(other: NJTimestamp): FiniteDuration = minus(other)

  override def toString: String = utc.toString
}

object NJTimestamp {
  def apply(ts: Timestamp): NJTimestamp = NJTimestamp(ts.getTime)
  def apply(ins: Instant): NJTimestamp = NJTimestamp(ins.toEpochMilli)
  def apply(zdt: ZonedDateTime): NJTimestamp = apply(zdt.toInstant)
  def apply(odt: OffsetDateTime): NJTimestamp = apply(odt.toInstant)

  def apply(ldt: LocalDateTime, zoneId: ZoneId): NJTimestamp =
    apply(ldt.atZone(zoneId).toInstant)

  def apply(ld: LocalDate, zoneId: ZoneId): NJTimestamp =
    apply(toLocalDateTime(ld), zoneId)

  def apply(lt: LocalTime, zoneId: ZoneId): NJTimestamp =
    apply(toLocalDateTime(lt), zoneId)

  def apply(str: String, zoneId: ZoneId): NJTimestamp = {
    val parser: DateTimeParser[NJTimestamp] = DateTimeParser[Instant].map(NJTimestamp(_)) <+>
      DateTimeParser[OffsetDateTime].map(NJTimestamp(_)) <+>
      DateTimeParser[ZonedDateTime].map(NJTimestamp(_)) <+>
      DateTimeParser[LocalDate].map(NJTimestamp(_, zoneId)) <+>
      DateTimeParser[LocalTime].map(NJTimestamp(_, zoneId)) <+>
      DateTimeParser[LocalDateTime].map(NJTimestamp(_, zoneId))

    parser.parse(str) match {
      case Right(r) => r
      case Left(ex) => throw ex.parseException(str) // scalafix:ok
    }
  }

  def apply(str: String): NJTimestamp = {
    val parser: DateTimeParser[NJTimestamp] =
      DateTimeParser[Instant].map(NJTimestamp(_)) <+>
        DateTimeParser[OffsetDateTime].map(NJTimestamp(_)) <+>
        DateTimeParser[ZonedDateTime].map(NJTimestamp(_))

    parser.parse(str) match {
      case Right(r) => r
      case Left(ex) => throw ex.parseException(str) // scalafix:ok
    }
  }

  def now(clock: Clock): NJTimestamp = NJTimestamp(Instant.now(clock))
  def now(): NJTimestamp = NJTimestamp(Instant.now)

  implicit final val njTimestampInstance: Hash[NJTimestamp] & Order[NJTimestamp] & Show[NJTimestamp] =
    new Hash[NJTimestamp] with Order[NJTimestamp] with Show[NJTimestamp] {
      override def hash(x: NJTimestamp): Int = x.hashCode

      override def compare(x: NJTimestamp, y: NJTimestamp): Int =
        x.milliseconds.compareTo(y.milliseconds)

      override def show(x: NJTimestamp): String = x.toString
    }
}
