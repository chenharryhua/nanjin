package mtest

import cats.implicits._
import cats.laws.discipline.BitraverseTests
import com.github.chenharryhua.nanjin.codec.{BitraverseFs2Message, BitraverseKafkaRecord}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.Arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import fs2.kafka.{
  CommittableConsumerRecord,
  Headers,
  ProducerRecords,
  Timestamp,
  ConsumerRecord => Fs2ConsumerRecord,
  ProducerRecord => Fs2ProducerRecord
}

class MessageBitraverseTest extends AnyFunSuite with Discipline with BitraverseFs2Message {
  implicit val cr: Arbitrary[ConsumerRecord[Int, Int]]       = Arbitrary(genConsumerRecord)
  implicit val pr: Arbitrary[ProducerRecord[Int, Int]]       = Arbitrary(genProducerRecord)
  implicit val fs2pr: Arbitrary[Fs2ProducerRecord[Int, Int]] = Arbitrary(genFs2ProducerRecord)

  checkAll(
    "ConsumerRecord.Bitraverse",
    BitraverseTests[ConsumerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "ProducerRecord.Bitraverse",
    BitraverseTests[ProducerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "Fs2ProducerRecord.Bitraverse",
    BitraverseTests[Fs2ProducerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])
}
