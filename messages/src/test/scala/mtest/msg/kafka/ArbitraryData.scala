package mtest.msg.kafka

import cats.effect.IO
import fs2.Chunk
import fs2.kafka.{
  CommittableConsumerRecord as Fs2ConsumerMessage,
  CommittableProducerRecords as Fs2CommittableProducerRecords,
  ConsumerRecord as Fs2ConsumerRecord,
  ProducerRecord as Fs2ProducerRecord,
  ProducerRecords as Fs2ProducerRecords,
  TransactionalProducerRecords as Fs2TransactionalProducerRecords
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.{Arbitrary, Cogen, Gen}

final case class PrimitiveTypeCombined(
  a: Int,
  b: Long,
  c: Float,
  d: Double,
  e: String
)

object ArbitraryData extends genMessage.GenFs2Message {

  // kafka
  implicit val abKafkaConsumerRecord: Arbitrary[ConsumerRecord[Int, Int]] =
    Arbitrary(genConsumerRecord)

  implicit val cogenConsumerRecord: Cogen[ConsumerRecord[Int, Int]] =
    Cogen(m => m.key.toLong + m.value.toLong)

  implicit val abKafkaProducerRecord: Arbitrary[ProducerRecord[Int, Int]] =
    Arbitrary(genProducerRecord)

  implicit val cogenKafkaProducerRecordF: Cogen[ProducerRecord[Int, Int]] =
    Cogen(m => m.key.toLong + m.timestamp())

  implicit val abKafkaProducerRecords: Arbitrary[Chunk[ProducerRecord[Int, Int]]] =
    Arbitrary(Gen.containerOfN[List, ProducerRecord[Int, Int]](10, genProducerRecord).map(Chunk.seq))

  implicit val cogenKafkaProducerRecords: Cogen[Chunk[ProducerRecord[Int, Int]]] =
    Cogen(_.size.toLong)

  // fs2
  implicit val abFs2ConsumerRecord: Arbitrary[Fs2ConsumerRecord[Int, Int]] =
    Arbitrary(genFs2ConsumerRecord)

  implicit val cogenFs2ConsumerRecordF: Cogen[Fs2ConsumerRecord[Int, Int]] =
    Cogen(m => m.key.toLong)

  implicit val abFs2ConsumerMessage: Arbitrary[Fs2ConsumerMessage[IO, Int, Int]] =
    Arbitrary(genFs2ConsumerMessage)

  implicit val cogenFs2ConsumerMessageF: Cogen[Fs2ConsumerMessage[IO, Int, Int]] =
    Cogen(m => m.record.value.toLong)

  implicit val abFs2ProducerRecord: Arbitrary[Fs2ProducerRecord[Int, Int]] =
    Arbitrary(genFs2ProducerRecord)

  implicit val cogenFs2ProducerRecordF: Cogen[Fs2ConsumerRecord[Int, Int]] =
    Cogen(m => m.key.toLong + m.offset)

  implicit val abFs2ProducerRecords: Arbitrary[Fs2ProducerRecords[Int, Int]] =
    Arbitrary(genFs2ProducerRecords)

  implicit val abFs2CommittableProducerRecords: Arbitrary[Fs2CommittableProducerRecords[IO, Int, Int]] =
    Arbitrary(genFs2CommittableProducerRecords)

  implicit val abFs2TransactionalProducerRecords: Arbitrary[Fs2TransactionalProducerRecords[IO, Int, Int]] =
    Arbitrary(genFs2TransactionalProducerRecords)

}
