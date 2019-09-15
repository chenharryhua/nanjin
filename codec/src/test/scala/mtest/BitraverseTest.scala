package mtest

import akka.kafka.{ConsumerMessage => AkkaConsumerMessage, ProducerMessage => AkkaProducerMessage}
import cats.Id
import cats.effect.IO
import cats.implicits._
import cats.laws.discipline.BitraverseTests
import com.github.chenharryhua.nanjin.codec._
import fs2.kafka.{
  CommittableConsumerRecord    => Fs2CommittableConsumerRecord,
  CommittableProducerRecords   => Fs2CommittableProducerRecords,
  TransactionalProducerRecords => Fs2TransactionalProducerRecords,
  ConsumerRecord               => Fs2ConsumerRecord,
  ProducerRecord               => Fs2ProducerRecord,
  ProducerRecords              => Fs2ProducerRecords
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class BitraverseTest extends AnyFunSuite with Discipline {
  implicit val akkaCMBitraverse = BitraverseMessage[AkkaConsumerMessage.CommittableMessage,ConsumerRecord]
  implicit val akkaPMBitraverse = BitraverseMessage[AkkaProducerMessage.Message[*, *, String],ProducerRecord]
  implicit val akkaTrBitraverse = BitraverseMessage[AkkaConsumerMessage.TransactionalMessage,ConsumerRecord]

  implicit val fs2CMBitraverse = BitraverseMessage[Fs2CommittableConsumerRecord[IO, *, *],ConsumerRecord]
  implicit val fs2PRBitraverse = BitraverseMessage[Fs2ProducerRecord,ProducerRecord]
  implicit val fs2CRBitraverse = BitraverseMessage[Fs2ConsumerRecord,ConsumerRecord]

  implicit val kafkaCRBitraverse = BitraverseMessage[ConsumerRecord,ConsumerRecord]
  implicit val kafkaPRBitraverse = BitraverseMessage[ProducerRecord,ProducerRecord]

  implicit val akkaPMsBitraverse =
    BitraverseMessages[AkkaProducerMessage.MultiMessage[*, *, String]]
  implicit val fs2PMsBitraverse  = BitraverseMessages[Fs2ProducerRecords[*, *, String]]
  implicit val fs2CPRBitraverses = BitraverseMessages[Fs2CommittableProducerRecords[IO, *, *]]
  implicit val fs2TransBitraverses =
    BitraverseMessages[Fs2TransactionalProducerRecords[IO, *, *, String]]

  implicit val arbChain: Arbitrary[List[Int]] =
    Arbitrary(Gen.containerOfN[List, Int](3, arbitrary[Int]))

  checkAll(
    "fs2.consumer.CommittableConsumerRecord",
    BitraverseTests[Fs2CommittableConsumerRecord[IO, *, *]]
      .bitraverse[Id, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.consumer.ConsumerRecord",
    BitraverseTests[Fs2ConsumerRecord].bitraverse[Option, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.producer.ProducerRecord",
    BitraverseTests[Fs2ProducerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.producer.ProducerRecords",
    BitraverseTests[Fs2ProducerRecords[*, *, String]]
      .bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.producer.CommittableProducerRecords",
    BitraverseTests[Fs2CommittableProducerRecords[IO, *, *]]
      .bitraverse[Option, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.producer.TransactionalProducerRecords",
    BitraverseTests[Fs2TransactionalProducerRecords[IO, *, *, String]]
      .bitraverse[Option, Int, Int, Int, Int, Int, Int]
  )

  checkAll(
    "akka.producer.ProducerMessage",
    BitraverseTests[AkkaProducerMessage.Message[*, *, String]]
      .bitraverse[Either[String, *], Int, Int, Int, Int, Int, Int])

  checkAll(
    "akka.consumer.CommittableMessage",
    BitraverseTests[AkkaConsumerMessage.CommittableMessage]
      .bitraverse[Either[Long, *], Int, Int, Int, Int, Int, Int])

  checkAll(
    "akka.consumer.TransactionalMessage",
    BitraverseTests[AkkaConsumerMessage.TransactionalMessage]
      .bitraverse[Either[Long, *], Int, Int, Int, Int, Int, Int])

  checkAll(
    "akka.producer.MultiMessage",
    BitraverseTests[AkkaProducerMessage.MultiMessage[*, *, String]]
      .bitraverse[Either[Long, *], Int, Int, Int, Int, Int, Int])

  checkAll(
    "kafka.consumer.ConsumerRecord",
    BitraverseTests[ConsumerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "kafka.producer.ProducerRecord",
    BitraverseTests[ProducerRecord].bitraverse[Option, Int, Int, Int, Int, Int, Int])

}
