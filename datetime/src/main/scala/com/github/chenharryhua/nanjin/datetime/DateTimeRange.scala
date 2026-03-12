package com.github.chenharryhua.nanjin.datetime

import cats.data.Cont
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.partialOrder.catsSyntaxPartialOrder
import cats.syntax.show.showInterpolator
import cats.{Eval, PartialOrder, Show}
import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.typelevel.cats.time.instances.{duration, localdatetime, zoneid}

import java.sql.Timestamp
import java.time.*
import scala.concurrent.duration.FiniteDuration

// lazy range
final case class DateTimeRange(
  private val start: Option[DateTimeRange.TimeTypes],
  private val end: Option[DateTimeRange.TimeTypes],
  zoneId: ZoneId)
    extends localdatetime with duration with zoneid {

  private def calcDateTime(tt: DateTimeRange.TimeTypes): NJTimestamp =
    tt match {
      case nj: NJTimestamp   => nj
      case str: String       => NJTimestamp(str, zoneId)
      case lt: LocalDateTime => NJTimestamp(lt, zoneId)
    }

  def startTimestamp: Option[NJTimestamp] = start.map(calcDateTime)
  def endTimestamp: Option[NJTimestamp] = end.map(calcDateTime)
  def zonedStartTime: Option[ZonedDateTime] = startTimestamp.map(_.atZone(zoneId))
  def zonedEndTime: Option[ZonedDateTime] = endTimestamp.map(_.atZone(zoneId))

  /** @return
    *   list of local-date from start date to end date, both inclusive
    *
    * empty if infinite
    */
  def days: List[LocalDate] =
    (zonedStartTime, zonedEndTime).traverseN { (s, e) =>
      s.toLocalDate.toEpochDay.to(e.toLocalDate.toEpochDay).map(LocalDate.ofEpochDay).toList
    }.flatten

  def dayStrings: List[String] = days.map(d => NJTimestamp(d, zoneId).`Year=yyyy/Month=mm/Day=dd`(zoneId))

  def subranges(interval: FiniteDuration): List[DateTimeRange] =
    (startTimestamp, endTimestamp).traverseN { (s, e) =>
      s.milliseconds
        .until(e.milliseconds, interval.toMillis)
        .toList
        .map(a => DateTimeRange(zoneId).withStartTime(a).withEndTime(a + interval.toMillis))
    }.flatten

  def period: Option[Period] =
    (zonedStartTime, zonedEndTime).mapN((s, e) => Period.between(s.toLocalDate, e.toLocalDate))

  def javaDuration: Option[java.time.Duration] =
    (zonedStartTime, zonedEndTime).mapN((s, e) => java.time.Duration.between(s, e))

  def withZoneId(zoneId: ZoneId): DateTimeRange =
    copy(zoneId = zoneId)

  def withZoneId(zoneId: String): DateTimeRange =
    withZoneId(ZoneId.of(zoneId))

  // start
  def withStartTime(ts: LocalTime): DateTimeRange = copy(start = Some(toLocalDateTime(ts)))
  def withStartTime(ts: LocalDate): DateTimeRange = copy(start = Some(toLocalDateTime(ts)))
  def withStartTime(ts: LocalDateTime): DateTimeRange = copy(start = Some(ts))
  def withStartTime(ts: OffsetDateTime): DateTimeRange = copy(start = Some(NJTimestamp(ts)))
  def withStartTime(ts: ZonedDateTime): DateTimeRange = copy(start = Some(NJTimestamp(ts)))
  def withStartTime(ts: Instant): DateTimeRange = copy(start = Some(NJTimestamp(ts)))
  def withStartTime(ts: Long): DateTimeRange = copy(start = Some(NJTimestamp(ts)))
  def withStartTime(ts: Timestamp): DateTimeRange = copy(start = Some(NJTimestamp(ts)))
  def withStartTime(ts: String): DateTimeRange = copy(start = Some(ts))

  // end
  def withEndTime(ts: LocalTime): DateTimeRange = copy(end = Some(toLocalDateTime(ts)))
  def withEndTime(ts: LocalDate): DateTimeRange = copy(end = Some(toLocalDateTime(ts)))
  def withEndTime(ts: LocalDateTime): DateTimeRange = copy(end = Some(ts))
  def withEndTime(ts: OffsetDateTime): DateTimeRange = copy(end = Some(NJTimestamp(ts)))
  def withEndTime(ts: ZonedDateTime): DateTimeRange = copy(end = Some(NJTimestamp(ts)))
  def withEndTime(ts: Instant): DateTimeRange = copy(end = Some(NJTimestamp(ts)))
  def withEndTime(ts: Long): DateTimeRange = copy(end = Some(NJTimestamp(ts)))
  def withEndTime(ts: Timestamp): DateTimeRange = copy(end = Some(NJTimestamp(ts)))
  def withEndTime(ts: String): DateTimeRange = copy(end = Some(ts))

  def withNSeconds(seconds: Long): DateTimeRange = {
    val now = LocalDateTime.now
    withStartTime(now.minusSeconds(seconds)).withEndTime(now)
  }

  def withTimeRange(start: String, end: String): DateTimeRange =
    withStartTime(start).withEndTime(end)

  def withOneDay(ts: LocalDate): DateTimeRange =
    withStartTime(ts).withEndTime(LocalDateTime.of(ts, LocalTime.MAX))

  def withOneDay(ts: String): DateTimeRange =
    summon[DateTimeParser[LocalDate]].parse(ts).map(withOneDay) match {
      case Left(ex)   => throw ex.parseException(ts) // scalafix:ok
      case Right(day) => day
    }

  def withToday: DateTimeRange = withOneDay(LocalDate.now)
  def withYesterday: DateTimeRange = withOneDay(LocalDate.now.minusDays(1))

  /** The day before yesterday
    * @return
    */
  def withEreyesterday: DateTimeRange = withOneDay(LocalDate.now.minusDays(2))

  def inBetween(ts: Long): Boolean =
    (startTimestamp, endTimestamp) match {
      case (Some(s), Some(e)) => ts >= s.milliseconds && ts < e.milliseconds
      case (Some(s), None)    => ts >= s.milliseconds
      case (None, Some(e))    => ts < e.milliseconds
      case (None, None)       => true
    }

  def duration: Option[FiniteDuration] = (startTimestamp, endTimestamp).mapN((s, e) => e.minus(s))

  override def toString: String =
    (zonedStartTime.map(_.toLocalDateTime), zonedEndTime.map(_.toLocalDateTime)) match {
      case (None, Some(e))    => show"zoneId: $zoneId, start: null, end: $e, range: infinite"
      case (Some(s), None)    => show"zoneId: $zoneId, start: $s, end: null, range: infinite"
      case (Some(s), Some(e)) =>
        show"zoneId: $zoneId, start: $s, end: $e, range: ${java.time.Duration.between(s, e)}"
      case (None, None) => show"zoneId: $zoneId, start: null, end: null, range: infinite"
    }
}

object DateTimeRange {

  def apply(zoneId: ZoneId): DateTimeRange = DateTimeRange(None, None, zoneId)
  def apply(tick: Tick): DateTimeRange =
    DateTimeRange(tick.zoneId).withStartTime(tick.commence).withEndTime(tick.conclude)

  final private type TimeTypes =
    NJTimestamp | LocalDateTime | String // date-time in string, like "03:12"

  given partialOrderNJDateTimeRange: PartialOrder[DateTimeRange] & Show[DateTimeRange] =
    new PartialOrder[DateTimeRange] with Show[DateTimeRange] {

      private def lessStart(a: Option[NJTimestamp], b: Option[NJTimestamp]): Boolean =
        (a, b) match {
          case (None, _)          => true
          case (_, None)          => false
          case (Some(x), Some(y)) => x <= y
        }

      private def biggerEnd(a: Option[NJTimestamp], b: Option[NJTimestamp]): Boolean =
        (a, b) match {
          case (None, _)          => true
          case (_, None)          => false
          case (Some(x), Some(y)) => x > y
        }

      override def partialCompare(x: DateTimeRange, y: DateTimeRange): Double =
        (x, y) match {
          case (a, b) if a.endTimestamp === b.endTimestamp && a.startTimestamp === b.startTimestamp =>
            0.0
          case (a, b)
              if lessStart(a.startTimestamp, b.startTimestamp) && biggerEnd(a.endTimestamp, b.endTimestamp) =>
            1.0
          case (a, b)
              if lessStart(b.startTimestamp, a.startTimestamp) && biggerEnd(b.endTimestamp, a.endTimestamp) =>
            -1.0
          case _ => Double.NaN
        }

      override def show(x: DateTimeRange): String = x.toString

    }

  given Encoder[DateTimeRange] =
    (a: DateTimeRange) =>
      Json.obj(
        "zone_id" -> a.zoneId.asJson,
        "start" -> a.zonedStartTime.map(_.toLocalDateTime).asJson,
        "end" -> a.zonedEndTime.map(_.toLocalDateTime).asJson)

  given Decoder[DateTimeRange] =
    (c: HCursor) =>
      for {
        zoneId <- c.get[ZoneId]("zone_id")
        start <- c.get[Option[LocalDateTime]]("start")
        end <- c.get[Option[LocalDateTime]]("end")
      } yield Cont
        .pure[DateTimeRange, DateTimeRange](DateTimeRange(zoneId))
        .map(dtr => start.fold(dtr)(dtr.withStartTime))
        .map(dtr => end.fold(dtr)(dtr.withEndTime))
        .run(Eval.now)
        .value
}
