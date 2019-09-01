package mtest

import cats.implicits._
import cats.laws.discipline.BitraverseTests
import com.github.chenharryhua.nanjin.codec.BitraverseKafkaRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.Arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class MessageBitraverseTest extends AnyFunSuite with Discipline with BitraverseKafkaRecord {
  implicit val cr: Arbitrary[ConsumerRecord[Int, Int]] = Arbitrary(genConsumerRecord)
  implicit val pr: Arbitrary[ProducerRecord[Int, Int]] = Arbitrary(genProducerRecord)

  checkAll(
    "ConsumerRecord.Bitraverse",
    BitraverseTests[ConsumerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])
  checkAll(
    "ProducerRecord.Bitraverse",
    BitraverseTests[ProducerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])

}
