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
    // TODO: how to compare two parsers?
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

  test("order of applying time data does not matter") {
    val zoneId = ZoneId.of("Asia/Chongqing")
    val startTime = LocalDateTime.of(2012, 10, 26, 18, 0, 0)
    val endTime = LocalDateTime.of(2012, 10, 26, 23, 0, 0)

    val param = DateTimeRange(sydneyTime)

    val a = param.withEndTime(endTime).withZoneId(zoneId).withStartTime(startTime)
    val b = param.withStartTime(startTime).withZoneId(zoneId).withEndTime(endTime)
    val c = param.withZoneId(zoneId).withStartTime(startTime).withEndTime(endTime)
    val d = param.withEndTime(endTime).withStartTime(startTime.atZone(zoneId)).withZoneId(zoneId)
    val e = param.withEndTime("2012-10-26T23:00:00").withStartTime("2012-10-26T18:00:00").withZoneId(zoneId)

    assert(a.eqv(b))
    assert(a.eqv(c))
    assert(a.eqv(d))
    assert(a.eqv(e))
    assert(a.zonedStartTime.get.eqv(startTime.atZone(zoneId)))
    assert(a.zonedEndTime.get.eqv(endTime.atZone(zoneId)))
  }
  test("days should return list of date") {
    val d1 = LocalDate.of(2012, 10, 26)
    val d2 = LocalDate.of(2012, 10, 27)
    val d3 = LocalDate.of(2012, 10, 28)

    val dtr = DateTimeRange(beijingTime).withStartTime(d1).withEndTime("2012-10-28")

    assert(dtr.days.eqv(List(d1, d2, d3)))

    assert(dtr.withOneDay(d3).days.eqv(List(d3)))
  }

  test("start after end") {
    val d1 = LocalDate.of(2012, 10, 26)
    val d3 = LocalDate.of(2012, 10, 28)
    val dtr = DateTimeRange(beijingTime).withStartTime(d3).withEndTime(d1)
    assert(dtr.days.isEmpty)
  }

  test("infinite range should return empty list") {
    assert(DateTimeRange(cairoTime).days.isEmpty)
  }

  test("days of same day should return one") {
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

  test("json") {
    val dr = DateTimeRange(newyorkTime).withToday
    println(dr.asJson.noSpaces)
    println(dr)
  }

  test("days") {
    val dr =
      DateTimeRange(sydneyTime)
        .withStartTime("2020-12-20T23:00:00+11:00")
        .withEndTime("2020-12-29T01:00:00+11:00")

    assert((dr.start, dr.end).mapN { (s, e) =>
      java.time.Duration.between(s, e)
    }.get.toDays == 8)
    assert(dr.days.length == 10)
  }

  test("one day") {
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

  test("fluent api") {
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
      .withZoneId("Australia/Sydney")
      .withZoneId(sydneyTime)
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

  test("subranges") {
    val dr = DateTimeRange(sydneyTime).withStartTime("2021-01-01").withEndTime("2021-02-01")
    val sr = dr.subranges(24.hours)
    assert(sr.size == 31)
    assert(sr == dr.subranges(1.day))
    val rd = Random.nextInt(30)
    assert(sr(rd).end == sr(rd + 1).start)
  }
  test("subranges - irregular") {
    val dr = DateTimeRange(sydneyTime).withStartTime("2021-01-01").withEndTime("2021-02-01T08:00")
    val sr = dr.subranges(12.hours)
    assert(sr.size == 63)
    sr.sliding(2).toList.map {
      case List(a, b) => assert(a.end === b.start)
      case _          => ()
    }
  }
}
