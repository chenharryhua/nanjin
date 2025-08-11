package com.github.chenharryhua.nanjin.datetime

import cats.syntax.all.*
import cats.{PartialOrder, Show}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import monocle.Prism
import monocle.macros.Lenses
import org.typelevel.cats.time.instances.{duration, localdatetime, zoneid}
import shapeless.ops.coproduct.{Inject, Selector}
import shapeless.{:+:, CNil, Poly1}

import java.sql.Timestamp
import java.time.*
import scala.concurrent.duration.FiniteDuration

// lazy range
@Lenses final case class DateTimeRange(
  private val start: Option[DateTimeRange.TimeTypes],
  private val end: Option[DateTimeRange.TimeTypes],
  zoneId: ZoneId)
    extends localdatetime with duration with zoneid {

  private object calcDateTime extends Poly1 {

    implicit val stringDateTime: Case.Aux[String, NJTimestamp] =
      at[String](s => NJTimestamp(s, zoneId))

    implicit val localDateTime: Case.Aux[LocalDateTime, NJTimestamp] =
      at[LocalDateTime](NJTimestamp(_, zoneId))

    implicit val njTimestamp: Case.Aux[NJTimestamp, NJTimestamp] =
      at[NJTimestamp](identity)

  }

  val startTimestamp: Option[NJTimestamp] = start.map(_.fold(calcDateTime))
  val endTimestamp: Option[NJTimestamp] = end.map(_.fold(calcDateTime))
  val zonedStartTime: Option[ZonedDateTime] = startTimestamp.map(_.atZone(zoneId))
  val zonedEndTime: Option[ZonedDateTime] = endTimestamp.map(_.atZone(zoneId))

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
    DateTimeRange.zoneId.replace(zoneId)(this)

  def withZoneId(zoneId: String): DateTimeRange =
    DateTimeRange.zoneId.replace(ZoneId.of(zoneId))(this)

  implicit private def coproductPrism[A](implicit
    evInject: Inject[DateTimeRange.TimeTypes, A],
    evSelector: Selector[DateTimeRange.TimeTypes, A]): Prism[DateTimeRange.TimeTypes, A] =
    Prism[DateTimeRange.TimeTypes, A](evSelector.apply)(evInject.apply)

  private def setStart[A](a: A)(implicit prism: Prism[DateTimeRange.TimeTypes, A]): DateTimeRange =
    DateTimeRange.start.replace(Some(prism.reverseGet(a)))(this)

  private def setEnd[A](a: A)(implicit prism: Prism[DateTimeRange.TimeTypes, A]): DateTimeRange =
    DateTimeRange.end.replace(Some(prism.reverseGet(a)))(this)

  // start
  def withStartTime(ts: LocalTime): DateTimeRange = setStart(toLocalDateTime(ts))
  def withStartTime(ts: LocalDate): DateTimeRange = setStart(toLocalDateTime(ts))
  def withStartTime(ts: LocalDateTime): DateTimeRange = setStart(ts)
  def withStartTime(ts: OffsetDateTime): DateTimeRange = setStart(NJTimestamp(ts))
  def withStartTime(ts: ZonedDateTime): DateTimeRange = setStart(NJTimestamp(ts))
  def withStartTime(ts: Instant): DateTimeRange = setStart(NJTimestamp(ts))
  def withStartTime(ts: Long): DateTimeRange = setStart(NJTimestamp(ts))
  def withStartTime(ts: Timestamp): DateTimeRange = setStart(NJTimestamp(ts))
  def withStartTime(ts: String): DateTimeRange = setStart(ts)

  // end
  def withEndTime(ts: LocalTime): DateTimeRange = setEnd(toLocalDateTime(ts))
  def withEndTime(ts: LocalDate): DateTimeRange = setEnd(toLocalDateTime(ts))
  def withEndTime(ts: LocalDateTime): DateTimeRange = setEnd(ts)
  def withEndTime(ts: OffsetDateTime): DateTimeRange = setEnd(NJTimestamp(ts))
  def withEndTime(ts: ZonedDateTime): DateTimeRange = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Instant): DateTimeRange = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Long): DateTimeRange = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Timestamp): DateTimeRange = setEnd(NJTimestamp(ts))
  def withEndTime(ts: String): DateTimeRange = setEnd(ts)

  def withNSeconds(seconds: Long): DateTimeRange = {
    val now = LocalDateTime.now
    withStartTime(now.minusSeconds(seconds)).withEndTime(now)
  }

  def withTimeRange(start: String, end: String): DateTimeRange =
    withStartTime(start).withEndTime(end)

  def withOneDay(ts: LocalDate): DateTimeRange =
    withStartTime(ts).withEndTime(LocalDateTime.of(ts, LocalTime.MAX))

  def withOneDay(ts: String): DateTimeRange =
    DateTimeParser.localDateParser.parse(ts).map(withOneDay) match {
      case Left(ex)   => throw ex.parseException(ts)
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

  final private type TimeTypes =
    NJTimestamp :+:
      LocalDateTime :+:
      String :+: // date-time in string, like "03:12"
      CNil

  implicit final val partialOrderNJDateTimeRange: PartialOrder[DateTimeRange] & Show[DateTimeRange] =
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

  def apply(zoneId: ZoneId): DateTimeRange = DateTimeRange(None, None, zoneId)

  implicit val encoderDateTimeRange: Encoder[DateTimeRange] =
    (a: DateTimeRange) =>
      Json.obj(
        "zone_id" -> a.zoneId.asJson,
        "start" -> a.zonedStartTime.map(_.toLocalDateTime).asJson,
        "end" -> a.zonedEndTime.map(_.toLocalDateTime).asJson)

  implicit val decoderDateTimeRange: Decoder[DateTimeRange] =
    (c: HCursor) =>
      for {
        zoneId <- c.get[ZoneId]("zone_id")
        start <- c.get[LocalDateTime]("start")
        end <- c.get[LocalDateTime]("end")
      } yield DateTimeRange(zoneId).withStartTime(start).withEndTime(end)
}
