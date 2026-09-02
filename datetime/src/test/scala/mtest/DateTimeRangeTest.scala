package mtest

import cats.{Alternative, Eq}
import cats.kernel.laws.discipline.PartialOrderTests
import cats.laws.discipline.AlternativeTests
import cats.syntax.all.*
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.*
import com.github.chenharryhua.nanjin.common.chrono.zones.*
import com.github.chenharryhua.nanjin.datetime.*
import com.github.chenharryhua.nanjin.datetime.instances.given
import io.circe.syntax.EncoderOps
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

import java.sql.Timestamp
import java.time.*
import scala.concurrent.duration.*
import scala.util.Random

class DateTimeRangeTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  implicit val arbiNJDateTimeRange: Arbitrary[DateTimeRange] =
    Arbitrary(for {
      date <- genZonedDateTimeWithZone(None)
      inc <- Gen.choose[Long](1, 50 * 365 * 24 * 3600) // 50 years
      d = date.toLocalDateTime
    } yield DateTimeRange(darwinTime).withStartTime(d).withEndTime(d.plusSeconds(inc)))

  implicit val cogen: Cogen[DateTimeRange] =
    Cogen(m => m.start.map(_.toEpochMilli).getOrElse(0))

  implicit val arbParser: Arbitrary[DateTimeParser[Instant]] = Arbitrary(Gen.const(DateTimeParser[Instant]))
  implicit val cogenInstant: Cogen[Instant] = Cogen((i: Instant) => i.getEpochSecond)

  implicit val eqInstant: Eq[DateTimeParser[Instant]] = new Eq[DateTimeParser[Instant]] {
    override def eqv(x: DateTimeParser[Instant], y: DateTimeParser[Instant]): Boolean = true
  }

  implicit val eqInstant3: Eq[DateTimeParser[(Instant, Instant, Instant)]] =
    new Eq[DateTimeParser[(Instant, Instant, Instant)]] {

      override def eqv(
        x: DateTimeParser[(Instant, Instant, Instant)],
        y: DateTimeParser[(Instant, Instant, Instant)]): Boolean = true
    }

  implicit val arbFunction: Arbitrary[DateTimeParser[Instant => Instant]] = Arbitrary(
    Gen
      .function1[Instant, Instant](genZonedDateTime.map(_.toInstant))
      .map(f => Alternative[DateTimeParser].pure(f)))

  checkAll("NJDateTimeRange-UpperBounded", PartialOrderTests[DateTimeRange].partialOrder)
  checkAll("NJDateTimeRange-PartialOrder", PartialOrderTests[DateTimeRange].partialOrder)
  checkAll("NJTimestamp", AlternativeTests[DateTimeParser].alternative[Instant, Instant, Instant])

  test("1.order of applying time data does not matter") {
    val zoneId = ZoneId.of("Asia/Chongqing")
    val startTime = LocalDateTime.of(2012, 10, 26, 18, 0, 0)
    val endTime = LocalDateTime.of(2012, 10, 26, 23, 0, 0)

    // zone is fixed at construction; only the order and representation of the bounds should be irrelevant
    val param = DateTimeRange(zoneId)

    val a = param.withEndTime(endTime).withStartTime(startTime)
    val b = param.withStartTime(startTime).withEndTime(endTime)
    val c = param.withStartTime(startTime.atZone(zoneId)).withEndTime(endTime)
    val d = param.withEndTime("2012-10-26T23:00:00").withStartTime("2012-10-26T18:00:00")

    assert(a.eqv(b))
    assert(a.eqv(c))
    assert(a.eqv(d))
    assert(a.zonedStartTime.get.eqv(startTime.atZone(zoneId)))
    assert(a.zonedEndTime.get.eqv(endTime.atZone(zoneId)))
  }
  test("2.days should return list of date") {
    val d1 = LocalDate.of(2012, 10, 26)
    val d2 = LocalDate.of(2012, 10, 27)
    val d3 = LocalDate.of(2012, 10, 28)

    val dtr = DateTimeRange(beijingTime).withStartTime(d1).withEndTime("2012-10-28")

    assert(dtr.days.toList.eqv(List(d1, d2, d3)))

    assert(dtr.withOneDay(d3).days.toList.eqv(List(d3)))
  }

  test("3.start after end") {
    val d1 = LocalDate.of(2012, 10, 26)
    val d3 = LocalDate.of(2012, 10, 28)
    val dtr = DateTimeRange(beijingTime).withStartTime(d3).withEndTime(d1)
    assert(dtr.days.isEmpty)
  }

  test("4.infinite range should return empty list") {
    assert(DateTimeRange(cairoTime).days.isEmpty)
  }

  test("5.days of same day should return one") {
    val d3 = LocalDate.of(2012, 10, 28)
    val dt4 = LocalDateTime.of(d3, LocalTime.of(10, 1, 1))
    val dt5 = LocalDateTime.of(d3, LocalTime.of(10, 1, 2))

    val sameDay = DateTimeRange(newyorkTime).withStartTime(dt4).withEndTime(dt5)
    assert(sameDay.days.size == 1)
    assert(sameDay.days.head == d3)

    println(DateTimeRange(newyorkTime))
    println(DateTimeRange(newyorkTime).withStartTime(dt4))
    println(DateTimeRange(newyorkTime).withEndTime(dt4))
  }

  test("6.json") {
    val dr = DateTimeRange(newyorkTime).withToday
    println(dr.asJson.noSpaces)
    println(dr)
  }

  test("7.days") {
    val dr =
      DateTimeRange(sydneyTime)
        .withStartTime("2020-12-20T23:00:00+11:00")
        .withEndTime("2020-12-29T01:00:00+11:00")

    assert((dr.start, dr.end).mapN { (s, e) =>
      java.time.Duration.between(s, e)
    }.get.toDays == 8)
    assert(dr.days.length == 10)
  }

  test("8.one day") {
    val t = DateTimeRange(sydneyTime).withToday
    val y = DateTimeRange(sydneyTime).withYesterday
    val e = DateTimeRange(sydneyTime).withEreyesterday
    assert(t.days.size == 1)
    assert(y.days.size == 1)
    assert(e.days.size == 1)
    // fail on day leap
    assert(t.javaDuration.get.toMillis == 86399999)
    assert(y.javaDuration.get.toMillis == 86399999)
    assert(e.javaDuration.get.toMillis == 86399999)
    println(t)
    println(y)
    println(e)
  }

  test("9.fluent api") {
    val dr = DateTimeRange(sydneyTime)
      .withOneDay(LocalDate.now())
      .withOneDay(LocalDate.now().toString)
      .withToday
      .withYesterday
      .withEreyesterday
      .withStartTime(1000L)
      .withStartTime("2012-12-30")
      .withStartTime(Instant.now)
      .withStartTime(LocalDate.now())
      .withStartTime(Timestamp.from(Instant.now))
      .withStartTime(LocalTime.now())
      .withStartTime(LocalDateTime.now)
      .withStartTime(ZonedDateTime.now)
      .withStartTime(OffsetDateTime.now)
      .withEndTime(1000L)
      .withEndTime("2012-12-30")
      .withEndTime(Instant.now)
      .withEndTime(LocalDate.now())
      .withEndTime(Timestamp.from(Instant.now))
      .withEndTime(LocalTime.now())
      .withEndTime(LocalDateTime.now)
      .withEndTime(ZonedDateTime.now)
      .withEndTime(OffsetDateTime.now)
      .withNSeconds(1000)
      .withTimeRange("2020-12-30", "2020-12-31")

    dr.period
    dr.javaDuration
    assert(dr.start.isDefined)
    assert(dr.end.isDefined)
    assert(dr.zonedStartTime.isDefined)
    assert(dr.zonedEndTime.isDefined)
    assert(dr.javaDuration.isDefined)
    dr.show
  }

  test("10.subranges") {
    val dr = DateTimeRange(sydneyTime).withStartTime("2021-01-01").withEndTime("2021-02-01")
    val sr = dr.subranges(24.hours)
    assert(sr.size == 31)
    assert(sr == dr.subranges(1.day))
    val rd = Random.nextInt(30)
    assert(sr(rd).end == sr(rd + 1).start)
  }
  test("11.subranges - irregular") {
    val dr = DateTimeRange(sydneyTime).withStartTime("2021-01-01").withEndTime("2021-02-01T08:00")
    val sr = dr.subranges(12.hours)
    assert(sr.size == 63)
    sr.sliding(2).map(_.toList).foreach {
      case List(a, b) => assert(a.end === b.start)
      case _          => ()
    }
  }

  test("12.withNSeconds uses configured zoneId, not system default") {
    // Use a zone far from system default to detect incorrect zone usage
    val zoneId = ZoneId.of("Pacific/Auckland")
    val dr = DateTimeRange(zoneId).withNSeconds(60)
    val startZoned = dr.zonedStartTime.get
    val endZoned = dr.zonedEndTime.get
    assert(startZoned.getZone == zoneId)
    assert(endZoned.getZone == zoneId)
    val diff = java.time.Duration.between(startZoned, endZoned)
    assert(diff.getSeconds == 60)
  }

  test("13.withToday uses configured zoneId") {
    // If it's a different date in the configured zone vs system zone, this catches the bug.
    // We can at least verify the zone is correct on the result.
    val zoneId = ZoneId.of("Pacific/Auckland")
    val dr = DateTimeRange(zoneId).withToday
    val startZoned = dr.zonedStartTime.get
    assert(startZoned.getZone == zoneId)
    assert(startZoned.toLocalDate == LocalDate.now(zoneId))
  }

  test("14.withYesterday uses configured zoneId") {
    val zoneId = ZoneId.of("Pacific/Auckland")
    val dr = DateTimeRange(zoneId).withYesterday
    val startZoned = dr.zonedStartTime.get
    assert(startZoned.getZone == zoneId)
    assert(startZoned.toLocalDate == LocalDate.now(zoneId).minusDays(1))
  }

  test("15.withEreyesterday uses configured zoneId") {
    val zoneId = ZoneId.of("Pacific/Auckland")
    val dr = DateTimeRange(zoneId).withEreyesterday
    val startZoned = dr.zonedStartTime.get
    assert(startZoned.getZone == zoneId)
    assert(startZoned.toLocalDate == LocalDate.now(zoneId).minusDays(2))
  }

  test("16.subranges rejects sub-millisecond intervals") {
    val dr = DateTimeRange(sydneyTime).withStartTime("2021-01-01").withEndTime("2021-01-02")
    assertThrows[IllegalArgumentException] {
      dr.subranges(500.microseconds)
    }
  }

  test("17.subranges last subrange does not exceed parent end") {
    // 10 seconds split by 3 seconds: [0,3), [3,6), [6,9), [9,10) — last one capped
    val dr = DateTimeRange(sydneyTime)
      .withStartTime(Instant.ofEpochMilli(0))
      .withEndTime(Instant.ofEpochMilli(10000))
    val sr = dr.subranges(3.seconds)
    assert(sr.size == 4)
    // last subrange end should be capped at parent end
    assert(sr.last.end.get == dr.end.get)
    // verify no subrange exceeds parent end
    sr.foreach(sub => assert(!sub.end.get.isAfter(dr.end.get)))
  }

  test("18.FiniteDuration decoder returns failure for huge durations instead of throwing") {
    import io.circe.{Decoder, Json}
    val decoder = summon[Decoder[FiniteDuration]]
    // PT2562047788H = ~292 years which overflows Long nanoseconds
    val result = decoder.decodeJson(Json.fromString("PT2562047789H"))
    assert(result.isLeft)
  }

  test("19.FiniteDuration decoder round-trips for normal durations") {
    import io.circe.{Decoder, Encoder}
    val encoder = summon[Encoder[FiniteDuration]]
    val decoder = summon[Decoder[FiniteDuration]]
    val dur = 5.hours + 30.minutes
    val json = encoder(dur)
    val decoded = decoder.decodeJson(json)
    assert(decoded.isRight)
    assert(decoded.toOption.get == dur)
  }

  // -------------------- edge cases --------------------

  test("20.inBetween - bounded range is closed on start, open on end") {
    val s = Instant.parse("2021-01-01T00:00:00Z")
    val e = Instant.parse("2021-01-02T00:00:00Z")
    val dr = DateTimeRange(utcTime).withStartTime(s).withEndTime(e)

    assert(dr.inBetween(s), "start is inclusive")
    assert(!dr.inBetween(e), "end is exclusive")
    assert(dr.inBetween(s.plusSeconds(3600)), "interior point is inside")
    assert(!dr.inBetween(s.minusSeconds(1)), "before start is outside")
    assert(!dr.inBetween(e.plusSeconds(1)), "after end is outside")
  }

  test("21.inBetween - half-open and fully-open ranges") {
    val s = Instant.parse("2021-01-01T00:00:00Z")
    val e = Instant.parse("2021-01-02T00:00:00Z")
    val probe = Instant.parse("2021-06-01T00:00:00Z")

    // start only: unbounded above, inclusive start
    val startOnly = DateTimeRange(utcTime).withStartTime(s)
    assert(startOnly.inBetween(s))
    assert(startOnly.inBetween(probe))
    assert(!startOnly.inBetween(s.minusSeconds(1)))

    // end only: unbounded below, exclusive end
    val endOnly = DateTimeRange(utcTime).withEndTime(e)
    assert(endOnly.inBetween(e.minusSeconds(1)))
    assert(!endOnly.inBetween(e))

    // unbounded: contains everything
    val infinite = DateTimeRange(utcTime)
    assert(infinite.inBetween(probe))
  }

  test("22.malformed strings fail fast at the setter") {
    assertThrows[java.time.format.DateTimeParseException] {
      DateTimeRange(utcTime).withStartTime("not-a-date")
    }
    assertThrows[java.time.format.DateTimeParseException] {
      DateTimeRange(utcTime).withEndTime("2021-99-99")
    }
    assertThrows[java.time.format.DateTimeParseException] {
      DateTimeRange(utcTime).withTimeRange("2021-01-01", "garbage")
    }
    assertThrows[java.time.format.DateTimeParseException] {
      DateTimeRange(utcTime).withOneDay("nope")
    }
  }

  test("23.JSON round-trips a bounded range through the AST") {
    val dr = DateTimeRange(sydneyTime)
      .withStartTime("2021-01-01T00:00:00")
      .withEndTime("2021-01-02T00:00:00")
    assert(dr.asJson.as[DateTimeRange] == Right(dr))
  }

  test("24.JSON round-trips an unbounded range (null bounds)") {
    val dr = DateTimeRange(sydneyTime)
    val json = dr.asJson
    assert(json.hcursor.get[Option[LocalDateTime]]("start") == Right(None))
    assert(json.hcursor.get[Option[LocalDateTime]]("end") == Right(None))
    assert(json.as[DateTimeRange] == Right(dr))
  }

  test("25.duration accessors are None on an infinite range") {
    val dr = DateTimeRange(utcTime)
    assert(dr.period.isEmpty)
    assert(dr.javaDuration.isEmpty)
    assert(dr.finiteDuration.isEmpty)

    // start-only range is also infinite for duration purposes
    val startOnly = DateTimeRange(utcTime).withStartTime(Instant.now)
    assert(startOnly.javaDuration.isEmpty)
  }

  test("26.days and duration when start equals end") {
    val x = Instant.parse("2021-03-14T12:00:00Z")
    val dr = DateTimeRange(utcTime).withStartTime(x).withEndTime(x)
    assert(dr.days.toList == List(LocalDate.parse("2021-03-14")))
    assert(dr.javaDuration.get.isZero)
    assert(dr.inBetween(x), "single-instant range still includes its start (closed start)")
  }

  test("27.subranges - interval at least as wide as the span yields one bucket") {
    val dr = DateTimeRange(utcTime)
      .withStartTime(Instant.ofEpochMilli(0))
      .withEndTime(Instant.ofEpochMilli(5000))
    val sr = dr.subranges(1.hour)
    assert(sr.size == 1)
    assert(sr.head.start == dr.start)
    assert(sr.head.end == dr.end)
  }

  test("28.subranges is empty when a bound is unset") {
    assert(DateTimeRange(utcTime).withStartTime(Instant.now).subranges(1.hour).isEmpty)
    assert(DateTimeRange(utcTime).subranges(1.hour).isEmpty)
  }

  test("29.withNSeconds(0) produces a zero-width range") {
    val dr = DateTimeRange(utcTime).withNSeconds(0)
    assert(dr.start == dr.end)
    assert(dr.javaDuration.get.isZero)
  }

  test("30.PartialOrder - containment and incomparability") {
    val inner = DateTimeRange(utcTime)
      .withStartTime(Instant.parse("2021-01-10T00:00:00Z"))
      .withEndTime(Instant.parse("2021-01-20T00:00:00Z"))
    val outer = DateTimeRange(utcTime)
      .withStartTime(Instant.parse("2021-01-01T00:00:00Z"))
      .withEndTime(Instant.parse("2021-02-01T00:00:00Z"))

    // outer contains inner -> outer >= inner
    assert(outer >= inner)
    assert(inner <= outer)

    // an unbounded range contains any bounded range
    assert(DateTimeRange(utcTime) >= outer)

    // overlapping but neither contains the other -> incomparable (partialCompare = NaN)
    val left = DateTimeRange(utcTime)
      .withStartTime(Instant.parse("2021-01-01T00:00:00Z"))
      .withEndTime(Instant.parse("2021-01-15T00:00:00Z"))
    val right = DateTimeRange(utcTime)
      .withStartTime(Instant.parse("2021-01-10T00:00:00Z"))
      .withEndTime(Instant.parse("2021-01-20T00:00:00Z"))
    assert(left.partialCompare(right).isNaN)
    assert(!(left <= right) && !(left >= right))
  }

  // -------------------- string parsing input formats --------------------

  test("31.parse Instant string (UTC 'Z') is zone-independent") {
    val s = "2021-01-01T00:00:00Z"
    val expected = Instant.parse(s)
    // same instant regardless of the range's configured zone
    assert(DateTimeRange(utcTime).withStartTime(s).start.contains(expected))
    assert(DateTimeRange(sydneyTime).withStartTime(s).start.contains(expected))
    assert(DateTimeRange(newyorkTime).withStartTime(s).start.contains(expected))
  }

  test("32.parse OffsetDateTime string uses its own offset") {
    val s = "2021-01-01T10:00:00+11:00"
    val expected = OffsetDateTime.parse(s).toInstant
    // the offset in the string wins, independent of the range zone
    assert(DateTimeRange(utcTime).withStartTime(s).start.contains(expected))
    assert(DateTimeRange(newyorkTime).withStartTime(s).start.contains(expected))
  }

  test("33.parse ZonedDateTime string uses its own zone") {
    val s = "2021-01-01T10:00:00+11:00[Australia/Sydney]"
    val expected = ZonedDateTime.parse(s).toInstant
    assert(DateTimeRange(utcTime).withStartTime(s).start.contains(expected))
  }

  test("34.parse LocalDate string is start-of-day in the range's zone") {
    val s = "2021-06-01"
    val sydney = DateTimeRange(sydneyTime).withStartTime(s)
    assert(sydney.start.contains(LocalDate.parse(s).atStartOfDay(sydneyTime).toInstant))
    // start-of-day differs by zone, so the same string resolves to a different instant
    val ny = DateTimeRange(newyorkTime).withStartTime(s)
    assert(ny.start.contains(LocalDate.parse(s).atStartOfDay(newyorkTime).toInstant))
    assert(sydney.start != ny.start)
  }

  test("35.parse LocalDateTime string is resolved in the range's zone") {
    val s = "2021-06-01T14:30:00"
    val ldt = LocalDateTime.parse(s)
    assert(DateTimeRange(sydneyTime).withStartTime(s).start.contains(ldt.atZone(sydneyTime).toInstant))
    assert(DateTimeRange(utcTime).withStartTime(s).start.contains(ldt.atZone(utcTime).toInstant))
    // same wall-clock string, different zone -> different instant
    assert(DateTimeRange(sydneyTime).withStartTime(s).start != DateTimeRange(utcTime).withStartTime(s).start)
  }

  test("36.parse LocalTime string is anchored to today in the range's zone") {
    val s = "14:30:00"
    val dr = DateTimeRange(sydneyTime).withStartTime(s)
    val z = dr.zonedStartTime.get
    assert(z.getZone == sydneyTime)
    assert(z.toLocalTime == LocalTime.parse(s))
    assert(z.toLocalDate == LocalDate.now(sydneyTime))
  }

  test("37.withTimeRange parses both bounds with the configured zone") {
    val dr = DateTimeRange(sydneyTime).withTimeRange("2021-01-01T00:00:00", "2021-01-02T00:00:00")
    assert(dr.start.contains(LocalDateTime.parse("2021-01-01T00:00:00").atZone(sydneyTime).toInstant))
    assert(dr.end.contains(LocalDateTime.parse("2021-01-02T00:00:00").atZone(sydneyTime).toInstant))
    assert(dr.javaDuration.get == java.time.Duration.ofDays(1))
  }

  test("38.withOneDay(String) covers the whole day in the range's zone") {
    val dr = DateTimeRange(sydneyTime).withOneDay("2021-06-01")
    assert(dr.days.toList == List(LocalDate.parse("2021-06-01")))
    assert(dr.zonedStartTime.get.toLocalTime == LocalTime.MIDNIGHT)
    assert(dr.zonedEndTime.get.toLocalTime == LocalTime.MAX)
  }
}
