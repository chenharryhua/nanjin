package mtest

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import fs2.kafka.{
  CommittableConsumerRecord => Fs2ConsumerMessage,
  ConsumerRecord            => Fs2ConsumerRecord,
  ProducerRecord            => Fs2ProducerRecord
}
import org.scalacheck.Arbitrary
import Arbitrary.arbitrary
import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaConsumerMessage}
import akka.kafka.ProducerMessage.{Message            => AkkaProducerMessage}
import cats.effect.IO
import com.github.chenharryhua.nanjin.codec.LikeConsumerRecord._
import com.github.chenharryhua.nanjin.codec.LikeProducerRecord._

import genMessage._
final case class PrimitiveTypeCombined(
  a: Int,
  b: Long,
  c: Float,
  d: Double,
  e: String
)
import cats.implicits._

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

  //fs2
  implicit val abFs2ConsumerRecord: Arbitrary[Fs2ConsumerRecord[Int, Int]] =
    Arbitrary(genFs2ConsumerRecord)

  implicit val abFs2ConsumerRecordF = Arbitrary(
    (cm: Fs2ConsumerRecord[Int, Int]) => cm.bimap(_ - 1, _ + 1))

  implicit val abFs2ConsumerMessage: Arbitrary[Fs2ConsumerMessage[IO, Int, Int]] =
    Arbitrary(genFs2ConsumerMessage)

  implicit val abFs2ConsumerMessageF = Arbitrary((cm: Fs2ConsumerMessage[IO, Int, Int]) => cm)

  implicit val abFs2ProducerRecord: Arbitrary[Fs2ProducerRecord[Int, Int]] =
    Arbitrary(genFs2ProducerRecord)

  implicit val abFs2ProducerRecordF = Arbitrary((pr: Fs2ConsumerRecord[Int, Int]) => pr)

  //akka
  implicit val abAkkaConsumerRecord: Arbitrary[AkkaConsumerMessage[Int, Int]] =
    Arbitrary(genAkkaConsumerMessage)

  implicit val abAkkaConsumerRecordF = Arbitrary((cm: AkkaConsumerMessage[Int, Int]) => cm)

  implicit val abAkkaProducerRecord: Arbitrary[AkkaProducerMessage[Int, Int, String]] =
    Arbitrary(genAkkaProducerMessage)

  implicit val abAkkaProducerRecordF = Arbitrary((pr: AkkaProducerMessage[Int, Int, String]) => pr)

}
