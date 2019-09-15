package mtest

import akka.kafka.ConsumerMessage.{
  CommittableMessage   => AkkaConsumerMessage,
  TransactionalMessage => AkkaTransactionalMessage
}
import akka.kafka.ProducerMessage.{Message => AkkaProducerMessage, MultiMessage => AkkaMultiMessage}
import cats.effect.IO
import cats.implicits._
import fs2.Chunk
import fs2.kafka.{
  CommittableProducerRecords,
  TransactionalProducerRecords,
  CommittableConsumerRecord => Fs2ConsumerMessage,
  ConsumerRecord            => Fs2ConsumerRecord,
  ProducerRecord            => Fs2ProducerRecord,
  ProducerRecords           => Fs2ProducerRecords
}
import mtest.genMessage._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
final case class PrimitiveTypeCombined(
  a: Int,
  b: Long,
  c: Float,
  d: Double,
  e: String
)
import com.github.chenharryhua.nanjin.codec.BitraverseMessage._

trait ArbitraryData extends GenKafkaMessage with GenFs2Message with GenAkkaMessage {
  //kafka
  implicit val abKafkaConsumerRecord: Arbitrary[ConsumerRecord[Int, Int]] =
    Arbitrary(genConsumerRecord)

  implicit val abKafkaConsumerRecordF
    : Arbitrary[ConsumerRecord[Int, Int] => ConsumerRecord[Int, Int]] =
    Arbitrary(for {
      i <- arbitrary[Int]
      j <- arbitrary[Int]
    } yield (cm: ConsumerRecord[Int, Int]) => cm.bimap(_ + i, _ - j))

  implicit val abKafkaProducerRecord: Arbitrary[ProducerRecord[Int, Int]] =
    Arbitrary(genProducerRecord)

  implicit val abKafkaProducerRecordF
    : Arbitrary[ProducerRecord[Int, Int] => ProducerRecord[Int, Int]] =
    Arbitrary(for {
      i <- arbitrary[Int]
      j <- arbitrary[Int]
    } yield (cm: ProducerRecord[Int, Int]) => cm.bimap(_ - i, _ + j))

  implicit val abKafkaProducerRecords: Arbitrary[Chunk[ProducerRecord[Int, Int]]] =
    Arbitrary(
      Gen.containerOfN[List, ProducerRecord[Int, Int]](10, genProducerRecord).map(Chunk.seq))

  implicit val abKafkaProducerRecordsF
    : Arbitrary[Chunk[ProducerRecord[Int, Int]] => Chunk[ProducerRecord[Int, Int]]] = {
    Arbitrary((a: Chunk[ProducerRecord[Int, Int]]) => a)
  }

  //fs2
  implicit val abFs2ConsumerRecord: Arbitrary[Fs2ConsumerRecord[Int, Int]] =
    Arbitrary(genFs2ConsumerRecord)

  implicit val abFs2ConsumerRecordF
    : Arbitrary[Fs2ConsumerRecord[Int, Int] => Fs2ConsumerRecord[Int, Int]] =
    Arbitrary((cm: Fs2ConsumerRecord[Int, Int]) => cm.bimap(_ - 1, _ + 1))

  implicit val abFs2ConsumerMessage: Arbitrary[Fs2ConsumerMessage[IO, Int, Int]] =
    Arbitrary(genFs2ConsumerMessage)

  implicit val abFs2ConsumerMessageF
    : Arbitrary[Fs2ConsumerMessage[IO, Int, Int] => Fs2ConsumerMessage[IO, Int, Int]] =
    Arbitrary((cm: Fs2ConsumerMessage[IO, Int, Int]) => cm)

  implicit val abFs2ProducerRecord: Arbitrary[Fs2ProducerRecord[Int, Int]] =
    Arbitrary(genFs2ProducerRecord)

  implicit val abFs2ProducerRecordF
    : Arbitrary[Fs2ConsumerRecord[Int, Int] => Fs2ConsumerRecord[Int, Int]] =
    Arbitrary((pr: Fs2ConsumerRecord[Int, Int]) => pr)

  implicit val abFs2ProducerRecords: Arbitrary[Fs2ProducerRecords[Int, Int, String]] =
    Arbitrary(genFs2ProducerRecords)

  implicit val abFs2CommittableProducerRecords
    : Arbitrary[CommittableProducerRecords[IO, Int, Int]] =
    Arbitrary(genFs2CommittableProducerRecords)

  implicit val abFs2TransactionalProducerRecords
    : Arbitrary[TransactionalProducerRecords[IO, Int, Int, String]] =
    Arbitrary(genFs2TransactionalProducerRecords)

  //akka
  implicit val abAkkaConsumerRecord: Arbitrary[AkkaConsumerMessage[Int, Int]] =
    Arbitrary(genAkkaConsumerMessage)

  implicit val abAkkaConsumerRecordF
    : Arbitrary[AkkaConsumerMessage[Int, Int] => AkkaConsumerMessage[Int, Int]] =
    Arbitrary((cm: AkkaConsumerMessage[Int, Int]) => cm)

  implicit val abAkkaProducerRecord: Arbitrary[AkkaProducerMessage[Int, Int, String]] =
    Arbitrary(genAkkaProducerMessage)

  implicit val abAkkaProducerRecordF
    : Arbitrary[AkkaProducerMessage[Int, Int, String] => AkkaProducerMessage[Int, Int, String]] =
    Arbitrary((pr: AkkaProducerMessage[Int, Int, String]) => pr)

  implicit val abAkkaProducerRecords: Arbitrary[AkkaMultiMessage[Int, Int, String]] = {
    Arbitrary(genAkkaProducerMultiMessage)
  }
  implicit val abAkkaTransactionalMessage: Arbitrary[AkkaTransactionalMessage[Int, Int]] = {
    Arbitrary(genAkkaTransactionalMessage)
  }
}
