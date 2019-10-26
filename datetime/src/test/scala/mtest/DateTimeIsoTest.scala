package mtest

import java.sql.Timestamp
import java.time._

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.datetime.iso._
import monocle.Iso
import monocle.law.discipline.IsoTests
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class DateTimeIsoTest extends AnyFunSuite with Discipline {
  import ArbitaryData._

  checkAll("instant", IsoTests[Instant, Timestamp](implicitly[Iso[Instant, Timestamp]]))
  checkAll(
    "local-date-time",
    IsoTests[LocalDateTime, Timestamp](implicitly[Iso[LocalDateTime, Timestamp]]))
  checkAll(
    "zoned-date-time",
    IsoTests[ZonedDateTime, JavaZonedDateTime](implicitly[Iso[ZonedDateTime, JavaZonedDateTime]]))
  checkAll(
    "offset-date-time",
    IsoTests[OffsetDateTime, JavaOffsetDateTime](
      implicitly[Iso[OffsetDateTime, JavaOffsetDateTime]]))

  checkAll(
    "nanjin-timestamp",
    IsoTests[NJTimestamp, Timestamp](implicitly[Iso[NJTimestamp, Timestamp]]))
}
