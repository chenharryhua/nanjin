package mtest

import java.sql.{Date, Timestamp}

import cats.kernel.laws.discipline.{HashTests, OrderTests}
import cats.laws.discipline.AlternativeTests
import cats.tests.CatsSuite
import com.github.chenharryhua.nanjin.datetime._
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class HashOrderTests extends CatsSuite with FunSuiteDiscipline {
  import ArbitaryData._

  checkAll("Timestamp", HashTests[Timestamp].hash)
  checkAll("Timestamp", OrderTests[Timestamp].order)

  checkAll("Date", HashTests[Date].hash)
  checkAll("Date", OrderTests[Date].order)

  checkAll("NJTimestamp", HashTests[NJTimestamp].hash)
  checkAll("NJTimestamp", OrderTests[NJTimestamp].order)

  checkAll("JavaZonedDateTime", HashTests[JavaZonedDateTime].hash)
  checkAll("JavaZonedDateTime", OrderTests[JavaZonedDateTime].order)

  checkAll("JavaOffsetDateTime", HashTests[JavaOffsetDateTime].hash)
  checkAll("JavaOffsetDateTime", OrderTests[JavaOffsetDateTime].order)

  // checkAll("parsing", AlternativeTests[DateTimeParser].alternative)

}
