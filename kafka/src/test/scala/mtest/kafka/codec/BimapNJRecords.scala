package mtest.kafka.codec

import cats.laws.discipline.BifunctorTests
import com.github.chenharryhua.nanjin.kafka.{NJConsumerRecord, NJProducerRecord}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import cats.derived.auto.eq._ 
import cats.implicits._ 
class BimapNJRecords extends AnyFunSuite with Discipline {
  checkAll(
    "NJConsumerRecord",
    BifunctorTests[NJConsumerRecord].bifunctor[Int, Int, Int, Int, Int, Int])

  checkAll(
    "NJProducerRecord",
    BifunctorTests[NJProducerRecord].bifunctor[Int, Int, Int, Int, Int, Int])

}
