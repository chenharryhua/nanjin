package mtest

import akka.kafka.{ConsumerMessage, ProducerMessage}
import cats.Id
import cats.effect.IO
import cats.implicits._
import cats.laws.discipline.BitraverseTests
import com.github.chenharryhua.nanjin.codec._
import fs2.kafka.{
  CommittableConsumerRecord,
  ConsumerRecord => Fs2ConsumerRecord,
  ProducerRecord => Fs2ProducerRecord
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class BitraverseTest extends AnyFunSuite with Discipline with EqMessage {
  implicit val akkaCMBitraverse = LikeConsumerRecord[ConsumerMessage.CommittableMessage]
  implicit val akkaPMBitraverse = LikeProducerRecord[ProducerMessage.Message[*, *, String]]

  implicit val fs2CMBitraverse = LikeConsumerRecord[CommittableConsumerRecord[IO, *, *]]
  implicit val fs2PRBitraverse = LikeProducerRecord[Fs2ProducerRecord]
  implicit val fs2CRBitraverse = LikeConsumerRecord[Fs2ConsumerRecord]

  implicit val kafkaCRBitraverse = LikeConsumerRecord[ConsumerRecord]
  implicit val kafkaPRBitraverse = LikeProducerRecord[ProducerRecord]

  implicit val arbChain: Arbitrary[List[Int]] =
    Arbitrary(Gen.containerOfN[List, Int](3, arbitrary[Int]))

  checkAll(
    "fs2.CommittableConsumerRecord",
    BitraverseTests[CommittableConsumerRecord[IO, *, *]]
      .bitraverse[Id, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.ConsumerRecord",
    BitraverseTests[Fs2ConsumerRecord].bitraverse[Option, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.ProducerRecord",
    BitraverseTests[Fs2ProducerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "akka.ProducerMessage",
    BitraverseTests[ProducerMessage.Message[*, *, String]]
      .bitraverse[Either[String, *], Int, Int, Int, Int, Int, Int])

  checkAll(
    "akka.CommittableMessage",
    BitraverseTests[ConsumerMessage.CommittableMessage]
      .bitraverse[Either[Long, *], Int, Int, Int, Int, Int, Int])

  checkAll(
    "kafka.ConsumerRecord",
    BitraverseTests[ConsumerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "kafka.ProducerRecord",
    BitraverseTests[ProducerRecord].bitraverse[Option, Int, Int, Int, Int, Int, Int])

}
