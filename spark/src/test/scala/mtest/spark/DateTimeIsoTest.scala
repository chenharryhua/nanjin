package mtest.spark

import java.sql.Timestamp
import java.time._

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import monocle.Iso
import monocle.law.discipline.IsoTests
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class DateTimeIsoTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {
  import ArbitaryData._

  checkAll("instant", IsoTests[Instant, Timestamp](implicitly[Iso[Instant, Timestamp]]))

  checkAll(
    "local-date",
    IsoTests[LocalDate, JavaLocalDate](implicitly[Iso[LocalDate, JavaLocalDate]]))

  checkAll(
    "local-time",
    IsoTests[LocalTime, JavaLocalTime](implicitly[Iso[LocalTime, JavaLocalTime]]))

  checkAll(
    "local-date-time",
    IsoTests[LocalDateTime, JavaLocalDateTime](implicitly[Iso[LocalDateTime, JavaLocalDateTime]]))

  checkAll(
    "zoned-date-time",
    IsoTests[ZonedDateTime, JavaZonedDateTime](implicitly[Iso[ZonedDateTime, JavaZonedDateTime]]))

  checkAll(
    "offset-date-time",
    IsoTests[OffsetDateTime, JavaOffsetDateTime](
      implicitly[Iso[OffsetDateTime, JavaOffsetDateTime]]))
}
