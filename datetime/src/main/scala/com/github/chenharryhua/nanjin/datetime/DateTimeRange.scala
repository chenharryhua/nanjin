package com.github.chenharryhua.nanjin.datetime

import cats.data.Cont
import cats.syntax.apply.given
import cats.syntax.functor.given
import cats.syntax.partialOrder.given
import cats.syntax.semigroupk.given
import cats.syntax.show.showInterpolator
import cats.{Eval, PartialOrder, Show}
import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.typelevel.cats.time.instances.duration.given
import org.typelevel.cats.time.instances.instant.given
import org.typelevel.cats.time.instances.localdatetime.given
import org.typelevel.cats.time.instances.zoneid.given

import java.sql.Timestamp
import java.time.*
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.given

/** A time range anchored to a fixed `zoneId`. Every input is resolved to an `Instant` at the setter ("at the
  * door"), so a stored bound is always a valid, fully-resolved point in time; `start`/`end` are therefore
  * total. `None` means the bound is unset (an open/infinite side), never "unparseable". The zone is fixed at
  * construction and cannot be changed afterwards.
  */
final case class DateTimeRange(
  private val reprStart: Option[Instant],
  private val reprEnd: Option[Instant],
  zoneId: ZoneId) {

  def start: Option[Instant] = reprStart
  def end: Option[Instant] = reprEnd

  def zonedStartTime: Option[ZonedDateTime] = start.map(_.atZone(zoneId))
  def zonedEndTime: Option[ZonedDateTime] = end.map(_.atZone(zoneId))

  /** @return
    *   list of local-date from start date to end date, both inclusive
    *
    * empty if infinite
    */
  def days: List[LocalDate] =
    (zonedStartTime, zonedEndTime).traverseN { (s, e) =>
      s.toLocalDate.toEpochDay.to(e.toLocalDate.toEpochDay).map(LocalDate.ofEpochDay).toList
    }.flatten

  def subranges(interval: FiniteDuration): List[DateTimeRange] = {
    val millis = interval.toMillis
    require(millis > 0, s"interval must be at least 1 millisecond, but was $interval")
    (start, end).traverseN { (s, e) =>
      val startMs = s.toEpochMilli
      val endMs = e.toEpochMilli
      startMs
        .until(endMs, millis)
        .toList
        .map(a => DateTimeRange(zoneId).withStartTime(a).withEndTime(math.min(a + millis, endMs)))
    }.flatten
  }

  // Resolve a string to an Instant against this range's fixed zone. Throws on unparseable input so malformed
  // strings fail at the setter rather than silently becoming an unset bound.
  private def parseStr(str: String): Instant = {
    val parser: DateTimeParser[Instant] =
      DateTimeParser[Instant] <+>
        DateTimeParser[OffsetDateTime].map(_.toInstant) <+>
        DateTimeParser[ZonedDateTime].map(_.toInstant) <+>
        DateTimeParser[LocalDate].map(toLocalDateTime(_).atZone(zoneId).toInstant) <+>
        DateTimeParser[LocalTime].map(_.atDate(LocalDate.now(zoneId)).atZone(zoneId).toInstant) <+>
        DateTimeParser[LocalDateTime].map(_.atZone(zoneId).toInstant)

    parser.parse(str) match {
      case Right(r) => r
      case Left(ex) => throw ex.parseException(str) // scalafix:ok
    }
  }

  // start
  def withStartTime(ts: LocalTime): DateTimeRange =
    withStartTime(ts.atDate(LocalDate.now(zoneId)))
  def withStartTime(ts: LocalDate): DateTimeRange =
    withStartTime(toLocalDateTime(ts))
  def withStartTime(ts: LocalDateTime): DateTimeRange =
    withStartTime(ts.atZone(zoneId).toInstant)
  def withStartTime(ts: OffsetDateTime): DateTimeRange =
    withStartTime(ts.toInstant)
  def withStartTime(ts: ZonedDateTime): DateTimeRange =
    withStartTime(ts.toInstant)
  def withStartTime(ts: Instant): DateTimeRange =
    copy(reprStart = Some(ts))
  def withStartTime(ts: Long): DateTimeRange =
    withStartTime(Instant.ofEpochMilli(ts))
  def withStartTime(ts: Timestamp): DateTimeRange =
    withStartTime(ts.toInstant)
  def withStartTime(ts: String): DateTimeRange =
    withStartTime(parseStr(ts))

  // end
  def withEndTime(ts: LocalTime): DateTimeRange =
    withEndTime(ts.atDate(LocalDate.now(zoneId)))
  def withEndTime(ts: LocalDate): DateTimeRange =
    withEndTime(toLocalDateTime(ts))
  def withEndTime(ts: LocalDateTime): DateTimeRange =
    withEndTime(ts.atZone(zoneId).toInstant)
  def withEndTime(ts: OffsetDateTime): DateTimeRange =
    withEndTime(ts.toInstant)
  def withEndTime(ts: ZonedDateTime): DateTimeRange =
    withEndTime(ts.toInstant)
  def withEndTime(ts: Instant): DateTimeRange =
    copy(reprEnd = Some(ts))
  def withEndTime(ts: Long): DateTimeRange =
    withEndTime(Instant.ofEpochMilli(ts))
  def withEndTime(ts: Timestamp): DateTimeRange =
    withEndTime(ts.toInstant)
  def withEndTime(ts: String): DateTimeRange =
    withEndTime(parseStr(ts))

  def withNSeconds(seconds: Long): DateTimeRange = {
    val now = LocalDateTime.now(zoneId)
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

  def withToday: DateTimeRange = withOneDay(LocalDate.now(zoneId))
  def withYesterday: DateTimeRange = withOneDay(LocalDate.now(zoneId).minusDays(1))

  /** The day before yesterday
    * @return
    */
  def withEreyesterday: DateTimeRange = withOneDay(LocalDate.now(zoneId).minusDays(2))

  // closed start, open end
  def inBetween(ts: Instant): Boolean =
    (start, end) match {
      case (Some(s), Some(e)) => s.isBefore(ts) && e.isAfter(ts) || s === ts
      case (Some(s), None)    => s.isBefore(ts) || s === ts
      case (None, Some(e))    => e.isAfter(ts)
      case (None, None)       => true
    }

  def period: Option[Period] =
    (zonedStartTime, zonedEndTime).mapN((s, e) => Period.between(s.toLocalDate, e.toLocalDate))

  def javaDuration: Option[Duration] = (start, end).mapN((s, e) => Duration.between(s, e))
  def finiteDuration: Option[FiniteDuration] = javaDuration.map(_.toScala)

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

  given PartialOrder[DateTimeRange] =
    new PartialOrder[DateTimeRange] {

      private def lessStart(a: Option[Instant], b: Option[Instant]): Boolean =
        (a, b) match {
          case (None, _)          => true
          case (_, None)          => false
          case (Some(x), Some(y)) => x <= y
        }

      private def biggerEnd(a: Option[Instant], b: Option[Instant]): Boolean =
        (a, b) match {
          case (None, _)          => true
          case (_, None)          => false
          case (Some(x), Some(y)) => x >= y
        }

      override def partialCompare(x: DateTimeRange, y: DateTimeRange): Double =
        (x, y) match {
          case (a, b) if a.end === b.end && a.start === b.start =>
            0.0
          case (a, b) if lessStart(a.start, b.start) && biggerEnd(a.end, b.end) =>
            1.0
          case (a, b) if lessStart(b.start, a.start) && biggerEnd(b.end, a.end) =>
            -1.0
          case _ => Double.NaN
        }
    }

  given Show[DateTimeRange] = Show.fromToString[DateTimeRange]

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
