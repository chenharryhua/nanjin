package mtest

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util.GregorianCalendar

import cats.Eq
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.{arbLocalDateJdk8 => _, _}
import com.github.chenharryhua.nanjin.database._
import doobie.util.Meta
import monocle.law.discipline.IsoTests
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class DateTimeIsoTest extends AnyFunSuite with Discipline {
  val instant         = Meta[Instant]
  val date            = Meta[Date]
  val timestamp       = Meta[Timestamp]
  val localdate       = Meta[LocalDate]
  implicit val zoneId = ZoneId.systemDefault()
  val localdatetime   = Meta[LocalDateTime]

  implicit val eqInstant: Eq[Instant]             = (x: Instant, y: Instant)             => x === y
  implicit val eqTimestamp: Eq[Timestamp]         = (x: Timestamp, y: Timestamp)         => x === y
  implicit val eqLocalDateTime: Eq[LocalDateTime] = (x: LocalDateTime, y: LocalDateTime) => x === y
  implicit val eqLocalDate: Eq[LocalDate]         = (x: LocalDate, y: LocalDate)         => x === y

  implicit val eqDate: Eq[Date] =
    (x: Date, y: Date) => x.toLocalDate === y.toLocalDate

  implicit val coTimestamp: Cogen[Timestamp] =
    Cogen[Timestamp]((a: Timestamp) => a.getTime)

  implicit val coDate: Cogen[Date] =
    Cogen[Date]((a: Date) => a.getTime)

  implicit val arbTimestamp: Arbitrary[Timestamp] = Arbitrary(
    Gen.posNum[Long].map(new Timestamp(_)))

  val genLocalDate = Gen.calendar
    .suchThat(c =>
      c.before(new GregorianCalendar(8099, 1, 1)) &&
        c.after(new GregorianCalendar(1900, 1, 1)))
    .map(_.toInstant)
    .map(i => LocalDateTime.ofInstant(i, zoneId).toLocalDate)

  implicit val arbLocalDate: Arbitrary[LocalDate] = Arbitrary(genLocalDate)

  implicit val arbDate: Arbitrary[Date] = Arbitrary(genLocalDate.map(d => Date.valueOf(d)))

  checkAll("instant", IsoTests[Instant, Timestamp](isoInstant))
  checkAll("local-date-time", IsoTests[LocalDateTime, Timestamp](isoLocalDateTime))
  checkAll("local-date", IsoTests[LocalDate, Date](isoLocalDate))

}
