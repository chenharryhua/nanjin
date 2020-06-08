package mtest.kafka.codec

import akka.kafka.ConsumerMessage.{
  CommittableMessage   => AkkaConsumerMessage,
  TransactionalMessage => AkkaTransactionalMessage
}
import akka.kafka.ProducerMessage.{Message => AkkaProducerMessage, MultiMessage => AkkaMultiMessage}
import cats.effect.IO
import fs2.Chunk
import fs2.kafka.{
  CommittableConsumerRecord    => Fs2ConsumerMessage,
  CommittableProducerRecords   => Fs2CommittableProducerRecords,
  ConsumerRecord               => Fs2ConsumerRecord,
  ProducerRecord               => Fs2ProducerRecord,
  ProducerRecords              => Fs2ProducerRecords,
  TransactionalProducerRecords => Fs2TransactionalProducerRecords
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.{Arbitrary, Cogen, Gen}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, NJProducerRecord}

final case class PrimitiveTypeCombined(
  a: Int,
  b: Long,
  c: Float,
  d: Double,
  e: String
)

trait ArbitraryData extends genMessage.GenFs2Message with genMessage.GenAkkaMessage {

  //kafka
  implicit val abKafkaConsumerRecord: Arbitrary[ConsumerRecord[Int, Int]] =
    Arbitrary(genConsumerRecord)

  implicit val cogenConsumerRecord: Cogen[ConsumerRecord[Int, Int]] =
    Cogen(m => m.key.toLong + m.value.toLong)

  implicit val abKafkaProducerRecord: Arbitrary[ProducerRecord[Int, Int]] =
    Arbitrary(genProducerRecord)

  implicit val cogenKafkaProducerRecordF: Cogen[ProducerRecord[Int, Int]] =
    Cogen(m => m.key.toLong + m.timestamp())

  implicit val abKafkaProducerRecords: Arbitrary[Chunk[ProducerRecord[Int, Int]]] =
    Arbitrary(
      Gen.containerOfN[List, ProducerRecord[Int, Int]](10, genProducerRecord).map(Chunk.seq))

  implicit val cogenKafkaProducerRecords: Cogen[Chunk[ProducerRecord[Int, Int]]] =
    Cogen(_.size.toLong)

  //fs2
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

  implicit val abFs2ProducerRecords: Arbitrary[Fs2ProducerRecords[Int, Int, String]] =
    Arbitrary(genFs2ProducerRecords)

  implicit val abFs2CommittableProducerRecords
    : Arbitrary[Fs2CommittableProducerRecords[IO, Int, Int]] =
    Arbitrary(genFs2CommittableProducerRecords)

  implicit val abFs2TransactionalProducerRecords
    : Arbitrary[Fs2TransactionalProducerRecords[IO, Int, Int, String]] =
    Arbitrary(genFs2TransactionalProducerRecords)

  //akka
  implicit val abAkkaConsumerRecord: Arbitrary[AkkaConsumerMessage[Int, Int]] =
    Arbitrary(genAkkaConsumerMessage)

  implicit val cogenAkkaConsumerRecordF: Cogen[AkkaConsumerMessage[Int, Int]] =
    Cogen(_.record.key().toLong)

  implicit val abAkkaProducerRecord: Arbitrary[AkkaProducerMessage[Int, Int, String]] =
    Arbitrary(genAkkaProducerMessage)

  implicit val cogenAkkaProducerRecordF: Cogen[AkkaProducerMessage[Int, Int, String]] =
    Cogen(_.record.value().toLong)

  implicit val abAkkaProducerRecords: Arbitrary[AkkaMultiMessage[Int, Int, String]] = {
    Arbitrary(genAkkaProducerMultiMessage)
  }

  implicit val abAkkaTransactionalMessage: Arbitrary[AkkaTransactionalMessage[Int, Int]] = {
    Arbitrary(genAkkaTransactionalMessage)
  }

  //nj
  implicit val abNJConsumerRecord: Arbitrary[NJConsumerRecord[Int, Int]] =
    Arbitrary(genNJConsumerRecord)

  implicit val cogenNJConsumerRecord: Cogen[NJConsumerRecord[Int, Int]] =
    Cogen(m => (m.key |+| m.value).getOrElse(0).toLong)

  implicit val abNJProducerRecord: Arbitrary[NJProducerRecord[Int, Int]] =
    Arbitrary(genNJProducerRecord)

  implicit val cogenNJProducerRecordF: Cogen[NJProducerRecord[Int, Int]] =
    Cogen(m => (m.timestamp |+| m.key.map(_.toLong)).getOrElse(0))

}
