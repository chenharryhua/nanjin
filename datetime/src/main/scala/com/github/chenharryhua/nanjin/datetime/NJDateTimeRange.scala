package com.github.chenharryhua.nanjin.datetime

import cats.{PartialOrder, Show}
import cats.implicits.catsSyntaxTuple2Semigroupal
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.DurationFormatter
import monocle.Prism
import monocle.macros.Lenses
import shapeless.{:+:, CNil, Poly1}
import shapeless.ops.coproduct.{Inject, Selector}

import java.sql.Timestamp
import java.time.*
import scala.concurrent.duration.FiniteDuration

// lazy range
@Lenses final case class NJDateTimeRange(
  private val start: Option[NJDateTimeRange.TimeTypes],
  private val end: Option[NJDateTimeRange.TimeTypes],
  zoneId: ZoneId) {

  private object calcDateTime extends Poly1 {

    implicit val stringDateTime: Case.Aux[String, NJTimestamp] =
      at[String](s => NJTimestamp(s, zoneId))

    implicit val localDateTime: Case.Aux[LocalDateTime, NJTimestamp] =
      at[LocalDateTime](NJTimestamp(_, zoneId))

    implicit val njTimestamp: Case.Aux[NJTimestamp, NJTimestamp] =
      at[NJTimestamp](identity)

  }

  val startTimestamp: Option[NJTimestamp]   = start.map(_.fold(calcDateTime))
  val endTimestamp: Option[NJTimestamp]     = end.map(_.fold(calcDateTime))
  val zonedStartTime: Option[ZonedDateTime] = startTimestamp.map(_.atZone(zoneId))
  val zonedEndTime: Option[ZonedDateTime]   = endTimestamp.map(_.atZone(zoneId))

  /** @return
    *   list of local-date from start date(inclusive) to end date(exclusive)
    *
    * empty if start date === end date
    *
    * empty if infinite
    */
  def days: List[LocalDate] =
    (zonedStartTime, zonedEndTime).traverseN { (s, e) =>
      s.toLocalDate.toEpochDay.until(e.toLocalDate.toEpochDay).map(LocalDate.ofEpochDay).toList
    }.flatten

  def dayStrings: List[String] = days.map(d => NJTimestamp(d, zoneId).`Year=yyyy/Month=mm/Day=dd`(zoneId))

  def subranges(interval: FiniteDuration): List[NJDateTimeRange] =
    (startTimestamp, endTimestamp).traverseN { (s, e) =>
      s.milliseconds
        .until(e.milliseconds, interval.toMillis)
        .toList
        .map(a => NJDateTimeRange(zoneId).withStartTime(a).withEndTime(a + interval.toMillis))
    }.flatten

  def period: Option[Period] =
    (zonedStartTime, zonedEndTime).mapN((s, e) => Period.between(s.toLocalDate, e.toLocalDate))

  def javaDuration: Option[java.time.Duration] =
    (zonedStartTime, zonedEndTime).mapN((s, e) => java.time.Duration.between(s, e))

  def withZoneId(zoneId: ZoneId): NJDateTimeRange =
    NJDateTimeRange.zoneId.replace(zoneId)(this)

  def withZoneId(zoneId: String): NJDateTimeRange =
    NJDateTimeRange.zoneId.replace(ZoneId.of(zoneId))(this)

  implicit private def coproductPrism[A](implicit
    evInject: Inject[NJDateTimeRange.TimeTypes, A],
    evSelector: Selector[NJDateTimeRange.TimeTypes, A]): Prism[NJDateTimeRange.TimeTypes, A] =
    Prism[NJDateTimeRange.TimeTypes, A](evSelector.apply)(evInject.apply)

  private def setStart[A](a: A)(implicit prism: Prism[NJDateTimeRange.TimeTypes, A]): NJDateTimeRange =
    NJDateTimeRange.start.replace(Some(prism.reverseGet(a)))(this)

  private def setEnd[A](a: A)(implicit prism: Prism[NJDateTimeRange.TimeTypes, A]): NJDateTimeRange =
    NJDateTimeRange.end.replace(Some(prism.reverseGet(a)))(this)

  // start
  def withStartTime(ts: LocalTime): NJDateTimeRange      = setStart(toLocalDateTime(ts))
  def withStartTime(ts: LocalDate): NJDateTimeRange      = setStart(toLocalDateTime(ts))
  def withStartTime(ts: LocalDateTime): NJDateTimeRange  = setStart(ts)
  def withStartTime(ts: OffsetDateTime): NJDateTimeRange = setStart(NJTimestamp(ts))
  def withStartTime(ts: ZonedDateTime): NJDateTimeRange  = setStart(NJTimestamp(ts))
  def withStartTime(ts: Instant): NJDateTimeRange        = setStart(NJTimestamp(ts))
  def withStartTime(ts: Long): NJDateTimeRange           = setStart(NJTimestamp(ts))
  def withStartTime(ts: Timestamp): NJDateTimeRange      = setStart(NJTimestamp(ts))
  def withStartTime(ts: String): NJDateTimeRange         = setStart(ts)

  // end
  def withEndTime(ts: LocalTime): NJDateTimeRange      = setEnd(toLocalDateTime(ts))
  def withEndTime(ts: LocalDate): NJDateTimeRange      = setEnd(toLocalDateTime(ts))
  def withEndTime(ts: LocalDateTime): NJDateTimeRange  = setEnd(ts)
  def withEndTime(ts: OffsetDateTime): NJDateTimeRange = setEnd(NJTimestamp(ts))
  def withEndTime(ts: ZonedDateTime): NJDateTimeRange  = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Instant): NJDateTimeRange        = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Long): NJDateTimeRange           = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Timestamp): NJDateTimeRange      = setEnd(NJTimestamp(ts))
  def withEndTime(ts: String): NJDateTimeRange         = setEnd(ts)

  def withNSeconds(seconds: Long): NJDateTimeRange = {
    val now = LocalDateTime.now
    withStartTime(now.minusSeconds(seconds)).withEndTime(now)
  }

  def withTimeRange(start: String, end: String): NJDateTimeRange =
    withStartTime(start).withEndTime(end)

  def withOneDay(ts: LocalDate): NJDateTimeRange =
    withStartTime(ts).withEndTime(ts.plusDays(1))

  def withOneDay(ts: String): NJDateTimeRange =
    DateTimeParser.localDateParser.parse(ts).map(withOneDay) match {
      case Left(ex)   => throw ex.parseException(ts)
      case Right(day) => day
    }

  def withToday: NJDateTimeRange     = withOneDay(LocalDate.now)
  def withYesterday: NJDateTimeRange = withOneDay(LocalDate.now.minusDays(1))

  def isInBetween(ts: Long): Boolean =
    (startTimestamp, endTimestamp) match {
      case (Some(s), Some(e)) => ts >= s.milliseconds && ts < e.milliseconds
      case (Some(s), None)    => ts >= s.milliseconds
      case (None, Some(e))    => ts < e.milliseconds
      case (None, None)       => true
    }

  def duration: Option[FiniteDuration] = (startTimestamp, endTimestamp).mapN((s, e) => e.minus(s))
  override def toString: String =
    duration.map(DurationFormatter.defaultFormatter.format).getOrElse("infinite")
}

object NJDateTimeRange {

  final type TimeTypes =
    NJTimestamp :+:
      LocalDateTime :+:
      String :+: // date-time in string, like "03:12"
      CNil

  implicit final val partialOrderNJDateTimeRange: PartialOrder[NJDateTimeRange] & Show[NJDateTimeRange] =
    new PartialOrder[NJDateTimeRange] with Show[NJDateTimeRange] {

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

      override def partialCompare(x: NJDateTimeRange, y: NJDateTimeRange): Double =
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

      override def show(x: NJDateTimeRange): String = x.toString

    }

  def apply(zoneId: ZoneId): NJDateTimeRange = NJDateTimeRange(None, None, zoneId)

}
