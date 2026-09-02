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
  * construction and cannot be changed afterward.
  */
final case class DateTimeRange(start: Option[Instant], end: Option[Instant], zoneId: ZoneId) {

  /** The start bound as a `ZonedDateTime` in this range's zone, or `None` if the start is unset. */
  def zonedStartTime: Option[ZonedDateTime] = start.map(_.atZone(zoneId))

  /** The end bound as a `ZonedDateTime` in this range's zone, or `None` if the end is unset. */
  def zonedEndTime: Option[ZonedDateTime] = end.map(_.atZone(zoneId))

  /** @return
    *   lazy sequence of local-dates from start date to end date, both inclusive
    *
    * empty if either bound is unset (infinite)
    */
  def days: LazyList[LocalDate] =
    (zonedStartTime, zonedEndTime) match {
      case (Some(s), Some(e)) =>
        LazyList.from(s.toLocalDate.toEpochDay.to(e.toLocalDate.toEpochDay)).map(LocalDate.ofEpochDay)
      case _ => LazyList.empty
    }

  /** Split this range into consecutive, non-overlapping sub-ranges of the given width, evaluated lazily.
    *
    * Each sub-range is `[start, start + interval)`; the final one is clipped so it never extends past this
    * range's end. Empty if either bound is unset (infinite).
    *
    * @param interval
    *   sub-range width; must be at least one millisecond (throws `IllegalArgumentException` otherwise)
    */
  def subranges(interval: FiniteDuration): LazyList[DateTimeRange] = {
    val millis = interval.toMillis
    require(millis > 0, s"interval must be at least 1 millisecond, but was $interval")
    (start, end) match {
      case (Some(s), Some(e)) =>
        val startMs = s.toEpochMilli
        val endMs = e.toEpochMilli
        LazyList
          .from(startMs.until(endMs, millis))
          .map(a => DateTimeRange(zoneId).withStartTime(a).withEndTime(math.min(a + millis, endMs)))
      case _ => LazyList.empty
    }
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

  /** Set the start bound. Every overload resolves its input to an `Instant` immediately, against this range's
    * fixed zone where the input lacks one (`LocalTime`/`LocalDate`/`LocalDateTime` are interpreted in
    * `zoneId`; `LocalTime` is anchored to today, `LocalDate` to the start of the day). `Long` is epoch
    * milliseconds. The `String` overload parses ISO-8601 date/time formats and throws
    * `DateTimeParseException` if the text cannot be parsed.
    */
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
    copy(start = Some(ts))
  def withStartTime(ts: Long): DateTimeRange =
    withStartTime(Instant.ofEpochMilli(ts))
  def withStartTime(ts: Timestamp): DateTimeRange =
    withStartTime(ts.toInstant)
  def withStartTime(ts: String): DateTimeRange =
    withStartTime(parseStr(ts))

  /** Set the end bound. Resolution follows the same rules as `withStartTime`: the input is resolved to an
    * `Instant` at the call, using this range's fixed zone where needed, and the `String` overload throws
    * `DateTimeParseException` on unparseable text.
    */
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
    copy(end = Some(ts))
  def withEndTime(ts: Long): DateTimeRange =
    withEndTime(Instant.ofEpochMilli(ts))
  def withEndTime(ts: Timestamp): DateTimeRange =
    withEndTime(ts.toInstant)
  def withEndTime(ts: String): DateTimeRange =
    withEndTime(parseStr(ts))

  /** Set the range to the last `seconds` up to now (in this range's zone): start = now − seconds, end = now.
    */
  def withNSeconds(seconds: Long): DateTimeRange = {
    val now = LocalDateTime.now(zoneId)
    withStartTime(now.minusSeconds(seconds)).withEndTime(now)
  }

  /** Set both bounds from ISO-8601 strings. Throws `DateTimeParseException` if either cannot be parsed. */
  def withTimeRange(start: String, end: String): DateTimeRange =
    withStartTime(start).withEndTime(end)

  /** Set the range to cover the whole of `ts`: start at the beginning of the day, end at `LocalTime.MAX`. */
  def withOneDay(ts: LocalDate): DateTimeRange =
    withStartTime(ts).withEndTime(LocalDateTime.of(ts, LocalTime.MAX))

  /** Set the range to cover the whole day parsed from `ts` (a `LocalDate` string). Throws
    * `DateTimeParseException` if the text is not a valid date.
    */
  def withOneDay(ts: String): DateTimeRange =
    summon[DateTimeParser[LocalDate]].parse(ts).map(withOneDay) match {
      case Left(ex)   => throw ex.parseException(ts) // scalafix:ok
      case Right(day) => day
    }

  /** The range covering today, in this range's zone. */
  def withToday: DateTimeRange = withOneDay(LocalDate.now(zoneId))

  /** The range covering yesterday, in this range's zone. */
  def withYesterday: DateTimeRange = withOneDay(LocalDate.now(zoneId).minusDays(1))

  /** The range covering the day before yesterday, in this range's zone. */
  def withEreyesterday: DateTimeRange = withOneDay(LocalDate.now(zoneId).minusDays(2))

  /** Whether `ts` falls within the range, treated as closed on the start and open on the end (`start <= ts <
    * end`). An unset start is unbounded below, an unset end unbounded above, so an unset range contains
    * everything.
    */
  def inBetween(ts: Instant): Boolean =
    (start, end) match {
      case (Some(s), Some(e)) => s.isBefore(ts) && e.isAfter(ts) || s === ts
      case (Some(s), None)    => s.isBefore(ts) || s === ts
      case (None, Some(e))    => e.isAfter(ts)
      case (None, None)       => true
    }

  /** Calendar period (years/months/days) between the two bounds' local dates, or `None` if either bound is
    * unset.
    */
  def period: Option[Period] =
    (zonedStartTime, zonedEndTime).mapN((s, e) => Period.between(s.toLocalDate, e.toLocalDate))

  /** Elapsed time between start and end as a `java.time.Duration`, or `None` if either bound is unset. */
  def javaDuration: Option[Duration] = (start, end).mapN((s, e) => Duration.between(s, e))

  /** Elapsed time between start and end as a Scala `FiniteDuration`, or `None` if either bound is unset. */
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

  /** An unbounded range (both bounds unset) in the given zone; add bounds via the `with*` methods. */
  def apply(zoneId: ZoneId): DateTimeRange = DateTimeRange(None, None, zoneId)

  /** The range spanned by a `Tick`: start at its commence, end at its conclude, in the tick's zone. */
  def apply(tick: Tick): DateTimeRange =
    DateTimeRange(tick.zoneId).withStartTime(tick.commence).withEndTime(tick.conclude)

  /** Partial order by containment: `a >= b` when `a` starts no later and ends no earlier than `b` (i.e. `a`
    * contains `b`), with an unset start/end treated as unbounded. Ranges that neither contains the other are
    * incomparable (`NaN`).
    */
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

  /** `Show` renders the same text as `toString`. */
  given Show[DateTimeRange] = Show.fromToString[DateTimeRange]

  /** JSON object `{ zone_id, start, end }`, where bounds are the zoned `LocalDateTime` (null when unset).
    * Paired with the `Decoder` below for a round trip.
    */
  given Encoder[DateTimeRange] =
    (a: DateTimeRange) =>
      Json.obj(
        "zone_id" -> a.zoneId.asJson,
        "start" -> a.zonedStartTime.map(_.toLocalDateTime).asJson,
        "end" -> a.zonedEndTime.map(_.toLocalDateTime).asJson)

  /** Reads the `{ zone_id, start, end }` shape produced by the `Encoder`, interpreting the `LocalDateTime`
    * bounds in the decoded zone.
    */
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
