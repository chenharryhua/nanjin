package com.github.chenharryhua.nanjin.datetime

import java.time._

import cats.derived.auto.show._
import cats.implicits._
import cats.{Hash, Order, Show}

final case class NJDateTime(instant: Instant, zoneId: ZoneId) {
  def zonedDateTime: ZonedDateTime = instant.atZone(zoneId)
  def localDate: LocalDate         = zonedDateTime.toLocalDate
  def timestamp: NJTimestamp       = NJTimestamp(instant)

  def hourResolution: LocalDateTime = {
    val dt: LocalDateTime = zonedDateTime.toLocalDateTime
    LocalDateTime.of(dt.toLocalDate, LocalTime.of(dt.getHour, 0))
  }

  def minuteResolution: LocalDateTime = {
    val dt: LocalDateTime = zonedDateTime.toLocalDateTime
    LocalDateTime.of(dt.toLocalDate, LocalTime.of(dt.getHour, dt.getMinute))
  }

  def yearStr: String  = f"${zonedDateTime.getYear}%4d"
  def monthStr: String = f"${zonedDateTime.getMonthValue}%02d"
  def dayStr: String   = f"${zonedDateTime.getDayOfMonth}%02d"

  def `yyyy-mm-dd`: String =
    s"$yearStr-$monthStr-$dayStr"

  def `Year=yyyy/Month=mm/Day=dd`: String =
    s"Year=$yearStr/Month=$monthStr/Day=$dayStr"

}

object NJDateTime {
  private val utcZoneId: ZoneId       = ZoneId.of("Etc/UTC")
  private val melbourneZoneId: ZoneId = ZoneId.of("Australia/Melbourne")

  def apply(ts: Instant): NJDateTime         = NJDateTime(ts, utcZoneId)
  def apply(ts: NJTimestamp): NJDateTime     = NJDateTime(ts.instant, utcZoneId)
  def apply(zdt: ZonedDateTime): NJDateTime  = NJDateTime(zdt.toInstant, zdt.getZone)
  def apply(odt: OffsetDateTime): NJDateTime = apply(odt.toZonedDateTime)

  def apply(ldt: LocalDateTime, zoneId: ZoneId): NJDateTime =
    apply(ZonedDateTime.of(ldt, zoneId))

  def apply(ldt: LocalDate, zoneId: ZoneId): NJDateTime =
    apply(ZonedDateTime.of(ldt, LocalTime.of(0, 0, 0), zoneId))

  def melbourne(ldt: LocalDateTime): NJDateTime =
    apply(ZonedDateTime.of(ldt, melbourneZoneId))

  def melbourne(ldt: LocalDate): NJDateTime =
    apply(ldt, melbourneZoneId)

  implicit val njDateTimeInstance: Hash[NJDateTime] with Order[NJDateTime] with Show[NJDateTime] =
    new Hash[NJDateTime] with Order[NJDateTime] with Show[NJDateTime] {
      override def hash(x: NJDateTime): Int = x.hashCode

      override def compare(x: NJDateTime, y: NJDateTime): Int =
        x.zonedDateTime.compareTo(y.zonedDateTime)

      override def show(x: NJDateTime): String = x.show
    }
}
