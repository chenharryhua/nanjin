package com.github.chenharryhua.nanjin.datetime
import java.sql.Timestamp
import java.time._
import java.util.concurrent.TimeUnit

import cats.{Hash, Order, Show}
import cats.implicits._
import monocle.Iso
import monocle.macros.Lenses

import scala.concurrent.duration.{Duration, FiniteDuration}

final case class NJTimestamp(milliseconds: Long) extends AnyVal {
  def instant: Instant                      = Instant.ofEpochMilli(milliseconds)
  def utc: ZonedDateTime                    = instant.atZone(ZoneId.of("Etc/UTC"))
  def atZone(zoneId: ZoneId): ZonedDateTime = instant.atZone(zoneId)
  def javaLong: java.lang.Long              = milliseconds
}

object NJTimestamp {

  def apply(ts: Timestamp): NJTimestamp     = NJTimestamp(ts.getTime)
  def apply(ts: Instant): NJTimestamp       = NJTimestamp(ts.toEpochMilli)
  def apply(ts: ZonedDateTime): NJTimestamp = apply(ts.toInstant)

  def apply(ts: LocalDateTime, zoneId: ZoneId): NJTimestamp =
    apply(ts.atZone(zoneId).toInstant)

  def apply(ts: LocalDate, zoneId: ZoneId): NJTimestamp =
    apply(LocalDateTime.of(ts, LocalTime.MIDNIGHT), zoneId)

  def now(clock: Clock): NJTimestamp = NJTimestamp(Instant.now(clock))

  implicit val isoKafkaTimestamp: Iso[NJTimestamp, Timestamp] =
    Iso[NJTimestamp, Timestamp]((a: NJTimestamp) => new Timestamp(a.milliseconds))((b: Timestamp) =>
      NJTimestamp(b.getTime))

  implicit val NJTimestampInstance
    : Hash[NJTimestamp] with Order[NJTimestamp] with Show[NJTimestamp] =
    new Hash[NJTimestamp] with Order[NJTimestamp] with Show[NJTimestamp] {
      override def hash(x: NJTimestamp): Int = x.hashCode

      override def compare(x: NJTimestamp, y: NJTimestamp): Int =
        x.milliseconds.compareTo(y.milliseconds)

      override def show(x: NJTimestamp): String = x.utc.toString
    }
}

@Lenses final case class NJDateTimeRange(start: Option[NJTimestamp], end: Option[NJTimestamp]) {

  def withStart(ts: Long): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(NJTimestamp(ts)))(this)

  def withStart(ts: Timestamp): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(NJTimestamp(ts)))(this)

  def withStart(ts: Instant): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(NJTimestamp(ts)))(this)

  def withStart(ts: ZonedDateTime): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(NJTimestamp(ts)))(this)

  def withEnd(ts: Long): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(NJTimestamp(ts)))(this)

  def withEnd(ts: Timestamp): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(NJTimestamp(ts)))(this)

  def withEnd(ts: Instant): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(NJTimestamp(ts)))(this)

  def withEnd(ts: ZonedDateTime): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(NJTimestamp(ts)))(this)

  def isInBetween(ts: Long): Boolean = (start, end) match {
    case (Some(s), Some(e)) => ts >= s.milliseconds && ts < e.milliseconds
    case (Some(s), None)    => ts >= s.milliseconds
    case (None, Some(e))    => ts < e.milliseconds
    case (None, None)       => true
  }

  val duration: Option[FiniteDuration] =
    (start, end).mapN((s, e) => Duration(e.milliseconds - s.milliseconds, TimeUnit.MILLISECONDS))

  require(
    duration.forall(_.length > 0),
    s"start time(${start.map(_.utc)}) should be strictly before end time(${end.map(_.utc)}) in UTC.")
}

object NJDateTimeRange {
  val infinite: NJDateTimeRange = NJDateTimeRange(None, None)
}
