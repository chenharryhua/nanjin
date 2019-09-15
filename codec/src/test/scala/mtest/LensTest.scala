package mtest

import akka.kafka.ConsumerMessage.{
  CommittableMessage   => AkkaConsumerMessage,
  TransactionalMessage => AkkaTransactionalMessage
}
import akka.kafka.ProducerMessage.{Message => AkkaProducerMessage, MultiMessage => AkkaMultiMessage}
import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.{BitraverseMessage, BitraverseMessages}
import fs2.kafka.{
  CommittableConsumerRecord => Fs2CommittableConsumerRecord,
  ConsumerRecord            => Fs2ConsumerRecord,
  ProducerRecord            => Fs2ProducerRecord,
  ProducerRecords           => Fs2ProducerRecords
}
import monocle.PLens
import monocle.law.discipline.LensTests
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class LensTest extends AnyFunSuite with Discipline {

  checkAll(
    "fs2.consumer.CommittableConsumerRecord",
    LensTests(BitraverseMessage[Fs2CommittableConsumerRecord[IO, *, *]].lens[Int, Int, Int, Int]))

  checkAll(
    "fs2.consumer.ConsumerRecord",
    LensTests(BitraverseMessage[Fs2ConsumerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "fs2.producer.ProducerRecord",
    LensTests(BitraverseMessage[Fs2ProducerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "kafka.consumer.ConsumerRecord",
    LensTests(BitraverseMessage[ConsumerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "kafka.producer.ProducerRecord",
    LensTests(BitraverseMessage[ProducerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.consumer.ConsumerMessage",
    LensTests(BitraverseMessage[AkkaConsumerMessage].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.consumer.TransactionalMessage",
    LensTests(BitraverseMessage[AkkaTransactionalMessage].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.producer.ProducerMessage",
    LensTests(BitraverseMessage[AkkaProducerMessage[*, *, String]].lens[Int, Int, Int, Int]))

}
