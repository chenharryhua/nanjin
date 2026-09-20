package mtest

import cats.kernel.laws.discipline.{HashTests, OrderTests}
import com.github.chenharryhua.nanjin.datetime.instances.given
import munit.DisciplineSuite

import java.sql.{Date, Timestamp}

class HashOrderTests extends DisciplineSuite {
  import ArbitaryData.*

  checkAll("Timestamp", HashTests[Timestamp].hash)
  checkAll("Timestamp", OrderTests[Timestamp].order)

  checkAll("Date", HashTests[Date].hash)
  checkAll("Date", OrderTests[Date].order)

  // checkAll("parsing", AlternativeTests[DateTimeParser].alternative)
}
