package com.github.chenharryhua.nanjin.datetime

import java.sql.Timestamp
import java.time._
import java.util.concurrent.TimeUnit

import cats.implicits._
import cats.kernel.UpperBounded
import cats.{Eq, PartialOrder}
import monocle.function.Possible._
import monocle.generic.coproduct.coProductPrism
import monocle.macros.Lenses
import monocle.{Lens, Prism}
import shapeless.{:+:, CNil, Poly1}

import scala.concurrent.duration.{Duration, FiniteDuration}

@Lenses final case class NJDateTimeRange(
  start: Option[NJDateTimeRange.TimeTypes],
  end: Option[NJDateTimeRange.TimeTypes],
  zoneId: ZoneId) {

  private object calcDateTime extends Poly1 {

    implicit val localDate: Case.Aux[LocalDate, NJTimestamp] =
      at[LocalDate](NJTimestamp(_, zoneId))

    implicit val localDateTime: Case.Aux[LocalDateTime, NJTimestamp] =
      at[LocalDateTime](NJTimestamp(_, zoneId))

    implicit val instant: Case.Aux[Instant, NJTimestamp] =
      at[Instant](NJTimestamp(_))

    implicit val njtimestamp: Case.Aux[NJTimestamp, NJTimestamp] =
      at[NJTimestamp](identity)
  }

  val startTimestamp: Option[NJTimestamp] = start.map(_.fold(calcDateTime))
  val endTimestamp: Option[NJTimestamp]   = end.map(_.fold(calcDateTime))

  def withZoneId(zoneId: ZoneId): NJDateTimeRange =
    NJDateTimeRange.zoneId.set(zoneId)(this)

  private val localDate: Prism[NJDateTimeRange.TimeTypes, LocalDate] =
    coProductPrism[NJDateTimeRange.TimeTypes, LocalDate]

  private val localDateTime: Prism[NJDateTimeRange.TimeTypes, LocalDateTime] =
    coProductPrism[NJDateTimeRange.TimeTypes, LocalDateTime]

  private val njTimestamp: Prism[NJDateTimeRange.TimeTypes, NJTimestamp] =
    coProductPrism[NJDateTimeRange.TimeTypes, NJTimestamp]

  private def updateRange[A](
    lens: Lens[NJDateTimeRange, Option[NJDateTimeRange.TimeTypes]],
    prism: Prism[NJDateTimeRange.TimeTypes, A],
    a: A): NJDateTimeRange =
    lens.composeOptional(possible).composePrism(prism).set(a)(this)

  //start
  def withStartTime(ts: NJTimestamp): NJDateTimeRange =
    updateRange(NJDateTimeRange.start, njTimestamp, ts)

  def withStartTime(ts: LocalDate): NJDateTimeRange =
    updateRange(NJDateTimeRange.start, localDate, ts)

  def withStartTime(ts: LocalDateTime): NJDateTimeRange =
    updateRange(NJDateTimeRange.start, localDateTime, ts)

  def withStartTime(ts: Instant): NJDateTimeRange =
    updateRange(NJDateTimeRange.start, njTimestamp, NJTimestamp(ts))

  def withStartTime(ts: ZonedDateTime): NJDateTimeRange =
    updateRange(NJDateTimeRange.start, njTimestamp, NJTimestamp(ts))

  def withStartTime(ts: OffsetDateTime): NJDateTimeRange =
    updateRange(NJDateTimeRange.start, njTimestamp, NJTimestamp(ts))

  def withStartTime(ts: Long): NJDateTimeRange =
    updateRange(NJDateTimeRange.start, njTimestamp, NJTimestamp(ts))

  def withStartTime(ts: Timestamp): NJDateTimeRange =
    updateRange(NJDateTimeRange.start, njTimestamp, NJTimestamp(ts))

  //end
  def withEndTime(ts: NJTimestamp): NJDateTimeRange =
    updateRange(NJDateTimeRange.end, njTimestamp, ts)

  def withEndTime(ts: LocalDate): NJDateTimeRange =
    updateRange(NJDateTimeRange.end, localDate, ts)

  def withEndTime(ts: LocalDateTime): NJDateTimeRange =
    updateRange(NJDateTimeRange.end, localDateTime, ts)

  def withEndTime(ts: Instant): NJDateTimeRange =
    updateRange(NJDateTimeRange.end, njTimestamp, NJTimestamp(ts))

  def withEndTime(ts: ZonedDateTime): NJDateTimeRange =
    updateRange(NJDateTimeRange.end, njTimestamp, NJTimestamp(ts))

  def withEndTime(ts: OffsetDateTime): NJDateTimeRange =
    updateRange(NJDateTimeRange.end, njTimestamp, NJTimestamp(ts))

  def withEndTime(ts: Long): NJDateTimeRange =
    updateRange(NJDateTimeRange.end, njTimestamp, NJTimestamp(ts))

  def withEndTime(ts: Timestamp): NJDateTimeRange =
    updateRange(NJDateTimeRange.end, njTimestamp, NJTimestamp(ts))

  def isInBetween(ts: Long): Boolean = (startTimestamp, endTimestamp) match {
    case (Some(s), Some(e)) => ts >= s.milliseconds && ts < e.milliseconds
    case (Some(s), None)    => ts >= s.milliseconds
    case (None, Some(e))    => ts < e.milliseconds
    case (None, None)       => true
  }

  val duration: Option[FiniteDuration] =
    (startTimestamp, endTimestamp).mapN((s, e) =>
      Duration(e.milliseconds - s.milliseconds, TimeUnit.MILLISECONDS))

  require(duration.forall(_.length > 0), s"start time(${startTimestamp
    .map(_.utc)}) should be strictly before end time(${endTimestamp.map(_.utc)}) in UTC.")
}

object NJDateTimeRange {

  final type TimeTypes =
    LocalDate :+:
      LocalDateTime :+:
      NJTimestamp :+:
      CNil

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
}
