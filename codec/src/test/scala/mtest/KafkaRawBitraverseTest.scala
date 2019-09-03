package mtest

import cats.implicits._
import cats.laws.discipline.BitraverseTests
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.Arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class KafkaRawBitraverseTest extends AnyFunSuite with Discipline with KafkaRawMessageGen {

  implicit val cr: Arbitrary[ConsumerRecord[Int, Int]] = Arbitrary(genConsumerRecord)
  implicit val pr: Arbitrary[ProducerRecord[Int, Int]] = Arbitrary(genProducerRecord)

  checkAll(
    "ConsumerRecord",
    BitraverseTests[ConsumerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "ProducerRecord",
    BitraverseTests[ProducerRecord].bitraverse[Option, Int, Int, Int, Int, Int, Int])

}
