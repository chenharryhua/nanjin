package mtest.spark

import cats.kernel.laws.discipline.{HashTests, OrderTests}
import cats.tests.CatsSuite
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.datetime.{
  JavaLocalDate,
  JavaLocalDateTime,
  JavaLocalTime,
  JavaOffsetDateTime,
  JavaZonedDateTime
}
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class HashOrderTests extends CatsSuite with FunSuiteDiscipline {
  import ArbitaryData._

  checkAll("JavaZonedDateTime", HashTests[JavaZonedDateTime].hash)
  checkAll("JavaZonedDateTime", OrderTests[JavaZonedDateTime].order)

  checkAll("JavaOffsetDateTime", HashTests[JavaOffsetDateTime].hash)
  checkAll("JavaOffsetDateTime", OrderTests[JavaOffsetDateTime].order)

  checkAll("JavaLocalTime", HashTests[JavaLocalTime].hash)
  checkAll("JavaLocalTime", OrderTests[JavaLocalTime].order)

  checkAll("JavaLocalDate", HashTests[JavaLocalDate].hash)
  checkAll("JavaLocalDate", OrderTests[JavaLocalDate].order)

  checkAll("JavaLocalDateTime", HashTests[JavaLocalDateTime].hash)
  checkAll("JavaLocalDateTime", OrderTests[JavaLocalDateTime].order)

}
