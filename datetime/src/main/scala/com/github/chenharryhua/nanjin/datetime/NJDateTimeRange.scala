package com.github.chenharryhua.nanjin.datetime

import java.sql.Timestamp
import java.time._
import java.util.concurrent.TimeUnit

import cats.implicits._
import cats.kernel.UpperBounded
import cats.{Eq, PartialOrder, Show}
import monocle.Prism
import monocle.generic.coproduct.coProductPrism
import monocle.macros.Lenses
import shapeless.{:+:, CNil, Poly1}

import scala.concurrent.duration.{Duration, FiniteDuration}

@Lenses final case class NJDateTimeRange(
  private val start: Option[NJDateTimeRange.TimeTypes],
  private val end: Option[NJDateTimeRange.TimeTypes],
  zoneId: ZoneId) {

  private object calcDateTime extends Poly1 {

    implicit val localDate: Case.Aux[LocalDate, NJTimestamp] =
      at[LocalDate](NJTimestamp(_, zoneId))

    implicit val localTime: Case.Aux[LocalTime, NJTimestamp] =
      at[LocalTime](NJTimestamp(_, zoneId))

    implicit val localDateTime: Case.Aux[LocalDateTime, NJTimestamp] =
      at[LocalDateTime](NJTimestamp(_, zoneId))

    implicit val instant: Case.Aux[Instant, NJTimestamp] =
      at[Instant](NJTimestamp(_))

    implicit val njTimestamp: Case.Aux[NJTimestamp, NJTimestamp] =
      at[NJTimestamp](identity)

    implicit val zonedDatetime: Case.Aux[ZonedDateTime, NJTimestamp] =
      at[ZonedDateTime](NJTimestamp(_))

    implicit val offsetDateTime: Case.Aux[OffsetDateTime, NJTimestamp] =
      at[OffsetDateTime](NJTimestamp(_))

    implicit val longTime: Case.Aux[Long, NJTimestamp] =
      at[Long](NJTimestamp(_))

    implicit val timstamp: Case.Aux[Timestamp, NJTimestamp] =
      at[Timestamp](NJTimestamp(_))

  }

  val startTimestamp: Option[NJTimestamp]   = start.map(_.fold(calcDateTime))
  val endTimestamp: Option[NJTimestamp]     = end.map(_.fold(calcDateTime))
  val zonedStartTime: Option[ZonedDateTime] = startTimestamp.map(_.atZone(zoneId))
  val zonedEndTime: Option[ZonedDateTime]   = endTimestamp.map(_.atZone(zoneId))

  /**
    *
    * @return list of local-date from start date(inclusive) to end date(exclusive)
    *         empty if start date === end date
    *         empty if infinite
    */
  def days: List[LocalDate] =
    (zonedStartTime, zonedEndTime).traverseN { (s, e) =>
      s.toLocalDate.toEpochDay.until(e.toLocalDate.toEpochDay).map(LocalDate.ofEpochDay).toList
    }.flatten

  def withZoneId(zoneId: ZoneId): NJDateTimeRange =
    NJDateTimeRange.zoneId.set(zoneId)(this)

  def withZoneId(zoneId: String): NJDateTimeRange =
    NJDateTimeRange.zoneId.set(ZoneId.of(zoneId))(this)

  implicit private val localDate: Prism[NJDateTimeRange.TimeTypes, LocalDate] =
    coProductPrism[NJDateTimeRange.TimeTypes, LocalDate]

  implicit private val localTime: Prism[NJDateTimeRange.TimeTypes, LocalTime] =
    coProductPrism[NJDateTimeRange.TimeTypes, LocalTime]

  implicit private val localDateTime: Prism[NJDateTimeRange.TimeTypes, LocalDateTime] =
    coProductPrism[NJDateTimeRange.TimeTypes, LocalDateTime]

  implicit private val njTimestamp: Prism[NJDateTimeRange.TimeTypes, NJTimestamp] =
    coProductPrism[NJDateTimeRange.TimeTypes, NJTimestamp]

  private def setStart[A](a: A)(implicit
    prism: Prism[NJDateTimeRange.TimeTypes, A]): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(prism.reverseGet(a)))(this)

  private def setEnd[A](a: A)(implicit
    prism: Prism[NJDateTimeRange.TimeTypes, A]): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(prism.reverseGet(a)))(this)

  //start
  def withStartTime(ts: LocalTime): NJDateTimeRange     = setStart(ts)
  def withStartTime(ts: LocalDate): NJDateTimeRange     = setStart(ts)
  def withStartTime(ts: LocalDateTime): NJDateTimeRange = setStart(ts)
  def withStartTime(ts: Instant): NJDateTimeRange       = setStart(NJTimestamp(ts))
  def withStartTime(ts: Long): NJDateTimeRange          = setStart(NJTimestamp(ts))
  def withStartTime(ts: Timestamp): NJDateTimeRange     = setStart(NJTimestamp(ts))

  @throws[Exception]
  def withStartTime(ts: String): NJDateTimeRange = setStart(NJTimestamp(ts))

  //end
  def withEndTime(ts: LocalTime): NJDateTimeRange     = setEnd(ts)
  def withEndTime(ts: LocalDate): NJDateTimeRange     = setEnd(ts)
  def withEndTime(ts: LocalDateTime): NJDateTimeRange = setEnd(ts)
  def withEndTime(ts: Instant): NJDateTimeRange       = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Long): NJDateTimeRange          = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Timestamp): NJDateTimeRange     = setEnd(NJTimestamp(ts))

  @throws[Exception]
  def withEndTime(ts: String): NJDateTimeRange = setEnd(NJTimestamp(ts))

  def isInBetween(ts: Long): Boolean =
    (startTimestamp, endTimestamp) match {
      case (Some(s), Some(e)) => ts >= s.milliseconds && ts < e.milliseconds
      case (Some(s), None)    => ts >= s.milliseconds
      case (None, Some(e))    => ts < e.milliseconds
      case (None, None)       => true
    }

  val duration: Option[FiniteDuration] =
    (startTimestamp, endTimestamp).mapN((s, e) =>
      Duration(e.milliseconds - s.milliseconds, TimeUnit.MILLISECONDS))

  require(
    duration.forall(_.length > 0),
    s"start time(${startTimestamp.map(_.utc)}) should be strictly before end time(${endTimestamp
      .map(_.utc)}).")
}

object NJDateTimeRange {

  final type TimeTypes = NJTimestamp :+: LocalDateTime :+: LocalDate :+: LocalTime :+: CNil

  implicit val showNJDateTimeRange: Show[NJDateTimeRange] =
    tr =>
      s"NJDateTimeRange(startTime=${tr.zonedStartTime.toString}, endTime=${tr.zonedEndTime.toString})"

  implicit val upperBoundedNJDateTimeRange: UpperBounded[NJDateTimeRange] with Eq[NJDateTimeRange] =
    new UpperBounded[NJDateTimeRange] with Eq[NJDateTimeRange] {
      override val maxBound: NJDateTimeRange = NJDateTimeRange(None, None, ZoneId.systemDefault())

      private def lessStart(a: Option[NJTimestamp], b: Option[NJTimestamp]): Boolean =
        (a, b) match {
          case (None, None)       => true
          case (None, _)          => true
          case (_, None)          => false
          case (Some(x), Some(y)) => x <= y
        }

      private def biggerEnd(a: Option[NJTimestamp], b: Option[NJTimestamp]): Boolean =
        (a, b) match {
          case (None, None)       => true
          case (None, _)          => true
          case (_, None)          => false
          case (Some(x), Some(y)) => x > y
        }

      override val partialOrder: PartialOrder[NJDateTimeRange] = {
        case (a, b) if a.endTimestamp === b.endTimestamp && a.startTimestamp === b.startTimestamp =>
          0.0
        case (a, b)
            if lessStart(a.startTimestamp, b.startTimestamp) && biggerEnd(
              a.endTimestamp,
              b.endTimestamp) =>
          1.0
        case (a, b)
            if lessStart(b.startTimestamp, a.startTimestamp) && biggerEnd(
              b.endTimestamp,
              a.endTimestamp) =>
          -1.0
        case _ => Double.NaN
      }

      override def eqv(x: NJDateTimeRange, y: NJDateTimeRange): Boolean =
        partialOrder.eqv(x, y)
    }

  final val infinite: NJDateTimeRange = UpperBounded[NJDateTimeRange].maxBound

  def oneDay(ts: LocalDate): NJDateTimeRange =
    NJDateTimeRange.infinite.withStartTime(ts).withEndTime(ts.plusDays(1))

  def today: NJDateTimeRange     = oneDay(LocalDate.now)
  def yesterday: NJDateTimeRange = oneDay(LocalDate.now.minusDays(1))
}
