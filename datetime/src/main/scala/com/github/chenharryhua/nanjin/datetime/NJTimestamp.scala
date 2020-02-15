package com.github.chenharryhua.nanjin.datetime

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter

import cats.{Hash, Order, Show}
import monocle.Iso

import scala.util.Try

final case class NJTimestamp(milliseconds: Long) {
  def instant: Instant                      = Instant.ofEpochMilli(milliseconds)
  def utc: ZonedDateTime                    = instant.atZone(ZoneId.of("Etc/UTC"))
  def local: ZonedDateTime                  = atZone(ZoneId.systemDefault())
  def atZone(zoneId: ZoneId): ZonedDateTime = instant.atZone(zoneId)
  def javaLong: java.lang.Long              = milliseconds
  def yearStr(zoneId: ZoneId): String       = f"${atZone(zoneId).getYear}%4d"
  def monthStr(zoneId: ZoneId): String      = f"${atZone(zoneId).getMonthValue}%02d"
  def dayStr(zoneId: ZoneId): String        = f"${atZone(zoneId).getDayOfMonth}%02d"
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

  def parse(
    dateTimeStr: String,
    zoneId: ZoneId,
    formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME): Option[NJTimestamp] =
    Try(LocalDateTime.parse(dateTimeStr, formatter)).toOption.map(ldt =>
      NJTimestamp(ZonedDateTime.of(ldt, zoneId)))

  def now(clock: Clock): NJTimestamp = NJTimestamp(Instant.now(clock))

  implicit val isoKafkaTimestamp: Iso[NJTimestamp, Timestamp] =
    Iso[NJTimestamp, Timestamp]((a: NJTimestamp) => new Timestamp(a.milliseconds))((b: Timestamp) =>
      NJTimestamp(b.getTime))

  implicit val njTimestampInstance
    : Hash[NJTimestamp] with Order[NJTimestamp] with Show[NJTimestamp] =
    new Hash[NJTimestamp] with Order[NJTimestamp] with Show[NJTimestamp] {
      override def hash(x: NJTimestamp): Int = x.hashCode

      override def compare(x: NJTimestamp, y: NJTimestamp): Int =
        x.milliseconds.compareTo(y.milliseconds)

      override def show(x: NJTimestamp): String = x.utc.toString
    }
}
