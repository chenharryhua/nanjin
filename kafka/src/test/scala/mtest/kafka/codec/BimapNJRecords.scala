package mtest.kafka.codec

import cats.derived.auto.eq._
import cats.implicits._
import cats.kernel.laws.discipline.OrderTests
import cats.laws.discipline.BifunctorTests
import com.github.chenharryhua.nanjin.kafka.{NJConsumerRecord, NJProducerRecord}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class BimapNJRecords extends AnyFunSuite with FunSuiteDiscipline with Configuration {
  checkAll(
    "NJConsumerRecord",
    BifunctorTests[NJConsumerRecord].bifunctor[Int, Int, Int, Int, Int, Int])

  checkAll(
    "NJProducerRecord",
    BifunctorTests[NJProducerRecord].bifunctor[Int, Int, Int, Int, Int, Int])

}
