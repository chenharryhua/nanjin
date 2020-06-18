package com.github.chenharryhua.nanjin.datetime

import java.sql.Timestamp
import java.time._

import cats.implicits._
import cats.kernel.UpperBounded
import cats.{Eq, PartialOrder, Show}
import monocle.Prism
import monocle.generic.coproduct.coProductPrism
import monocle.macros.Lenses
import shapeless.{:+:, CNil, Poly1}

import scala.concurrent.duration.FiniteDuration

@Lenses final case class NJDateTimeRange(
  private val start: Option[NJDateTimeRange.TimeTypes],
  private val end: Option[NJDateTimeRange.TimeTypes],
  zoneId: ZoneId) {

  private val parser: DateTimeParser[NJTimestamp] =
    DateTimeParser[Instant].map(NJTimestamp(_)) <+>
      DateTimeParser[ZonedDateTime].map(NJTimestamp(_)) <+>
      DateTimeParser[OffsetDateTime].map(NJTimestamp(_)) <+>
      DateTimeParser[LocalDate].map(NJTimestamp(_, zoneId)) <+>
      DateTimeParser[LocalTime].map(NJTimestamp(_, zoneId)) <+>
      DateTimeParser[LocalDateTime].map(NJTimestamp(_, zoneId))

  private object calcDateTime extends Poly1 {

    implicit val stringDateTime: Case.Aux[String, NJTimestamp] =
      at[String](s =>
        parser.parse(s) match {
          case Right(r) => r
          case Left(ex) => throw ex.parseException(s)
        })

    implicit val localDateTime: Case.Aux[LocalDateTime, NJTimestamp] =
      at[LocalDateTime](NJTimestamp(_, zoneId))

    implicit val njTimestamp: Case.Aux[NJTimestamp, NJTimestamp] =
      at[NJTimestamp](identity)

  }

  val startTimestamp: Option[NJTimestamp]   = start.map(_.fold(calcDateTime))
  val endTimestamp: Option[NJTimestamp]     = end.map(_.fold(calcDateTime))
  val zonedStartTime: Option[ZonedDateTime] = startTimestamp.map(_.atZone(zoneId))
  val zonedEndTime: Option[ZonedDateTime]   = endTimestamp.map(_.atZone(zoneId))

  /**
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

  implicit private val localDateTimePrism: Prism[NJDateTimeRange.TimeTypes, LocalDateTime] =
    coProductPrism[NJDateTimeRange.TimeTypes, LocalDateTime]

  implicit private val njTimestampPrism: Prism[NJDateTimeRange.TimeTypes, NJTimestamp] =
    coProductPrism[NJDateTimeRange.TimeTypes, NJTimestamp]

  implicit private val stringDatetimePrism: Prism[NJDateTimeRange.TimeTypes, String] =
    coProductPrism[NJDateTimeRange.TimeTypes, String]

  private def setStart[A](a: A)(implicit
    prism: Prism[NJDateTimeRange.TimeTypes, A]): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(prism.reverseGet(a)))(this)

  private def setEnd[A](a: A)(implicit
    prism: Prism[NJDateTimeRange.TimeTypes, A]): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(prism.reverseGet(a)))(this)

  //start
  def withStartTime(ts: LocalTime): NJDateTimeRange      = setStart(toLocalDateTime(ts))
  def withStartTime(ts: LocalDate): NJDateTimeRange      = setStart(toLocalDateTime(ts))
  def withStartTime(ts: LocalDateTime): NJDateTimeRange  = setStart(ts)
  def withStartTime(ts: OffsetDateTime): NJDateTimeRange = setStart(NJTimestamp(ts))
  def withStartTime(ts: ZonedDateTime): NJDateTimeRange  = setStart(NJTimestamp(ts))
  def withStartTime(ts: Instant): NJDateTimeRange        = setStart(NJTimestamp(ts))
  def withStartTime(ts: Long): NJDateTimeRange           = setStart(NJTimestamp(ts))
  def withStartTime(ts: Timestamp): NJDateTimeRange      = setStart(NJTimestamp(ts))
  def withStartTime(ts: String): NJDateTimeRange         = setStart(ts)

  //end
  def withEndTime(ts: LocalTime): NJDateTimeRange      = setEnd(toLocalDateTime(ts))
  def withEndTime(ts: LocalDate): NJDateTimeRange      = setEnd(toLocalDateTime(ts))
  def withEndTime(ts: LocalDateTime): NJDateTimeRange  = setEnd(ts)
  def withEndTime(ts: OffsetDateTime): NJDateTimeRange = setEnd(NJTimestamp(ts))
  def withEndTime(ts: ZonedDateTime): NJDateTimeRange  = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Instant): NJDateTimeRange        = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Long): NJDateTimeRange           = setEnd(NJTimestamp(ts))
  def withEndTime(ts: Timestamp): NJDateTimeRange      = setEnd(NJTimestamp(ts))
  def withEndTime(ts: String): NJDateTimeRange         = setEnd(ts)

  def isInBetween(ts: Long): Boolean =
    (startTimestamp, endTimestamp) match {
      case (Some(s), Some(e)) => ts >= s.milliseconds && ts < e.milliseconds
      case (Some(s), None)    => ts >= s.milliseconds
      case (None, Some(e))    => ts < e.milliseconds
      case (None, None)       => true
    }

  val duration: Option[FiniteDuration] =
    (startTimestamp, endTimestamp).mapN((s, e) => e.minus(s))

  override def toString: String =
    s"NJDateTimeRange(startTime=${zonedStartTime.toString}, endTime=${zonedEndTime.toString})"

  require(
    duration.forall(_.length > 0),
    s"start time should be strictly before end time - $toString."
  )
}

object NJDateTimeRange {

  final type TimeTypes =
    NJTimestamp :+:
      LocalDateTime :+:
      String :+: // date-time in string, like "03:12"
      CNil

  implicit val showNJDateTimeRange: Show[NJDateTimeRange] = _.toString

  implicit val upperBoundedNJDateTimeRange: UpperBounded[NJDateTimeRange] with Eq[NJDateTimeRange] =
    new UpperBounded[NJDateTimeRange] with Eq[NJDateTimeRange] {
      override val maxBound: NJDateTimeRange = NJDateTimeRange(None, None, ZoneId.systemDefault())

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
