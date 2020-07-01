package mtest.kafka

import cats.derived.auto.eq._
import cats.implicits._
import cats.kernel.laws.discipline.{LowerBoundedTests, OrderTests}
import cats.laws.discipline.BifunctorTests
import com.github.chenharryhua.nanjin.messages.kafka._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class BimapNJRecords extends AnyFunSuite with FunSuiteDiscipline with Configuration {
  checkAll(
    "NJConsumerRecord - bifunctor",
    BifunctorTests[NJConsumerRecord].bifunctor[Int, Int, Int, Int, Int, Int])

  checkAll(
    "NJProducerRecord - bifunctor",
    BifunctorTests[NJProducerRecord].bifunctor[Int, Int, Int, Int, Int, Int])

  checkAll("NJConsumerRecord - order", OrderTests[NJConsumerRecord[Int, Int]].order)

  checkAll(
    "NJConsumerRecord - lowerbounded",
    LowerBoundedTests[NJConsumerRecord[Int, Int]].lowerBounded)
}
