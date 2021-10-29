package mtest

import java.sql.{Date, Timestamp}
import java.time._

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.{arbInstantJdk8, arbLocalDateTimeJdk8}
import com.github.chenharryhua.nanjin.datetime.instances._
import monocle.law.discipline.IsoTests
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class DateTimeIsoTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {
  import ArbitaryData._

  checkAll("instant", IsoTests[Instant, Timestamp](isoInstant))

  checkAll("local-date", IsoTests[LocalDate, Date](isoLocalDate))

  checkAll("local-date-time", IsoTests[LocalDateTime, Instant](isoLocalDateTime))

}
