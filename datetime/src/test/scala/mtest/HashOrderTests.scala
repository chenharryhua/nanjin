package mtest

import cats.kernel.laws.discipline.{HashTests, OrderTests}
import cats.tests.CatsSuite
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.datetime.instances.*
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

import java.sql.{Date, Timestamp}

class HashOrderTests extends CatsSuite with FunSuiteDiscipline {
  import ArbitaryData.*

  checkAll("Timestamp", HashTests[Timestamp].hash)
  checkAll("Timestamp", OrderTests[Timestamp].order)

  checkAll("Date", HashTests[Date].hash)
  checkAll("Date", OrderTests[Date].order)

  checkAll("NJTimestamp", HashTests[NJTimestamp].hash)
  checkAll("NJTimestamp", OrderTests[NJTimestamp].order)

  // checkAll("parsing", AlternativeTests[DateTimeParser].alternative)
}
