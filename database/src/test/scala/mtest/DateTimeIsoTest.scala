package mtest

import java.sql.{Date, Timestamp}
import java.time._
import java.util.GregorianCalendar

import cats.Eq
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.{arbLocalDateJdk8 => _, _}
import com.github.chenharryhua.nanjin.database._
import doobie.util.Meta
import monocle.law.discipline.IsoTests
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import monocle.Iso

class DateTimeIsoTest extends AnyFunSuite with Discipline {
  val instant: Meta[Instant]             = Meta[Instant]
  val date: Meta[Date]                   = Meta[Date]
  val timestamp: Meta[Timestamp]         = Meta[Timestamp]
  val localdate: Meta[LocalDate]         = Meta[LocalDate]
  implicit val zoneId: ZoneId            = ZoneId.systemDefault()
  val localdatetime: Meta[LocalDateTime] = Meta[LocalDateTime]

  implicit val eqInstant: Eq[Instant]       = (x: Instant, y: Instant)               => x === y
  implicit val eqTimestamp: Eq[Timestamp]   = (x: Timestamp, y: Timestamp)           => x === y
  implicit val eqLocalDT: Eq[LocalDateTime] = (x: LocalDateTime, y: LocalDateTime)   => x === y
  implicit val eqLocalDate: Eq[LocalDate]   = (x: LocalDate, y: LocalDate)           => x === y
  implicit val eqZoned: Eq[ZonedDateTime]   = (x: ZonedDateTime, y: ZonedDateTime)   => x === y
  implicit val eqOffset: Eq[OffsetDateTime] = (x: OffsetDateTime, y: OffsetDateTime) => x === y

  implicit val eqJavaZoned: Eq[JavaZonedDateTime] = (x: JavaZonedDateTime, y: JavaZonedDateTime) =>
    x.zonedDateTime === y.zonedDateTime

  implicit val eqJavaOffset: Eq[JavaOffsetDateTime] =
    (x: JavaOffsetDateTime, y: JavaOffsetDateTime) => x.offsetDateTime === y.offsetDateTime

  implicit val eqDate: Eq[Date] =
    (x: Date, y: Date) => x.toLocalDate === y.toLocalDate

  implicit val coTimestamp: Cogen[Timestamp] =
    Cogen[Timestamp]((a: Timestamp) => a.getTime)

  implicit val coDate: Cogen[Date] =
    Cogen[Date]((a: Date) => a.getTime)

  implicit val coJavaZoned: Cogen[JavaZonedDateTime] =
    Cogen[JavaZonedDateTime]((a: JavaZonedDateTime) => a.zonedDateTime.toEpochSecond)

  implicit val coOffsetDateTime: Cogen[OffsetDateTime] =
    Cogen[OffsetDateTime]((a: OffsetDateTime) => a.toInstant.getEpochSecond)

  implicit val coJavaOffset: Cogen[JavaOffsetDateTime] =
    Cogen[JavaOffsetDateTime]((a: JavaOffsetDateTime) => a.offsetDateTime.toEpochSecond)

  implicit val arbTimestamp: Arbitrary[Timestamp] = Arbitrary(
    Gen.posNum[Long].map(new Timestamp(_)))

  val genLocalDate: Gen[LocalDate] = Gen.calendar
    .suchThat(c =>
      c.before(new GregorianCalendar(8099, 1, 1)) &&
        c.after(new GregorianCalendar(1900, 1, 1)))
    .map(_.toInstant)
    .map(i => LocalDateTime.ofInstant(i, zoneId).toLocalDate)

  implicit val arbLocalDate: Arbitrary[LocalDate] = Arbitrary(genLocalDate)

  implicit val arbDate: Arbitrary[Date] = Arbitrary(genLocalDate.map(d => Date.valueOf(d)))

  implicit val arbJavaZoned: Arbitrary[JavaZonedDateTime] = Arbitrary(
    genZonedDateTimeWithZone(None).map(zd => JavaZonedDateTime(zd))
  )

  implicit val arbJavaOffset: Arbitrary[JavaOffsetDateTime] = Arbitrary(
    genZonedDateTimeWithZone(None).map(zd => JavaOffsetDateTime(zd.toOffsetDateTime))
  )

  implicit val arbJavaOffset2: Arbitrary[OffsetDateTime] = Arbitrary(
    genZonedDateTimeWithZone(None).map(zd => OffsetDateTime.of(zd.toLocalDateTime, zd.getOffset))
  )

  checkAll("instant", IsoTests[Instant, Timestamp](isoInstant))
  checkAll("local-date-time", IsoTests[LocalDateTime, Timestamp](isoLocalDateTimeByZoneId))
  checkAll("local-date", IsoTests[LocalDate, Date](isoLocalDate))
  checkAll(
    "zoned-date-time",
    IsoTests[ZonedDateTime, JavaZonedDateTime](implicitly[Iso[ZonedDateTime, JavaZonedDateTime]]))
  checkAll(
    "offset-date-time",
    IsoTests[OffsetDateTime, JavaOffsetDateTime](
      implicitly[Iso[OffsetDateTime, JavaOffsetDateTime]]))
}
