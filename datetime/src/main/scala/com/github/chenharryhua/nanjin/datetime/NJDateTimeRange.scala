package com.github.chenharryhua.nanjin.datetime

import java.sql.Timestamp
import java.time._
import java.util.concurrent.TimeUnit

import cats.PartialOrder
import cats.implicits._
import cats.kernel.UpperBounded
import monocle.macros.Lenses
import shapeless.syntax.inject._
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

    implicit val njtimestamp: Case.Aux[NJTimestamp, NJTimestamp] =
      at[NJTimestamp](identity)
  }

  val startTimestamp: Option[NJTimestamp] = start.map(_.fold(calcDateTime))
  val endTimestamp: Option[NJTimestamp]   = end.map(_.fold(calcDateTime))

  def withZoneId(zoneId: ZoneId): NJDateTimeRange =
    NJDateTimeRange.zoneId.set(zoneId)(this)

  //start
  def withStartTime(ts: NJTimestamp): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(ts.inject[NJDateTimeRange.TimeTypes]))(this)

  def withStartTime(ts: LocalDate): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(ts.inject[NJDateTimeRange.TimeTypes]))(this)

  def withStartTime(ts: LocalDateTime): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(ts.inject[NJDateTimeRange.TimeTypes]))(this)

  def withStartTime(ts: Instant): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(NJTimestamp(ts).inject[NJDateTimeRange.TimeTypes]))(this)

  def withStartTime(ts: ZonedDateTime): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(NJTimestamp(ts).inject[NJDateTimeRange.TimeTypes]))(this)

  def withStartTime(ts: OffsetDateTime): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(NJTimestamp(ts).inject[NJDateTimeRange.TimeTypes]))(this)

  def withStartTime(ts: Long): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(NJTimestamp(ts).inject[NJDateTimeRange.TimeTypes]))(this)

  def withStartTime(ts: Timestamp): NJDateTimeRange =
    NJDateTimeRange.start.set(Some(NJTimestamp(ts).inject[NJDateTimeRange.TimeTypes]))(this)

  //end
  def withEndTime(ts: NJTimestamp): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(ts.inject[NJDateTimeRange.TimeTypes]))(this)

  def withEndTime(ts: LocalDate): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(ts.inject[NJDateTimeRange.TimeTypes]))(this)

  def withEndTime(ts: LocalDateTime): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(ts.inject[NJDateTimeRange.TimeTypes]))(this)

  def withEndTime(ts: Instant): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(NJTimestamp(ts).inject[NJDateTimeRange.TimeTypes]))(this)

  def withEndTime(ts: ZonedDateTime): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(NJTimestamp(ts).inject[NJDateTimeRange.TimeTypes]))(this)

  def withEndTime(ts: OffsetDateTime): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(NJTimestamp(ts).inject[NJDateTimeRange.TimeTypes]))(this)

  def withEndTime(ts: Long): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(NJTimestamp(ts).inject[NJDateTimeRange.TimeTypes]))(this)

  def withEndTime(ts: Timestamp): NJDateTimeRange =
    NJDateTimeRange.end.set(Some(NJTimestamp(ts).inject[NJDateTimeRange.TimeTypes]))(this)

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

  implicit val upperBoundedNJDateTimeRange: UpperBounded[NJDateTimeRange] =
    new UpperBounded[NJDateTimeRange] {
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
    }

  final val infinite: NJDateTimeRange = UpperBounded[NJDateTimeRange].maxBound
}
