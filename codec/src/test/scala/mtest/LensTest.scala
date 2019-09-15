package mtest

import akka.kafka.ConsumerMessage.{
  CommittableMessage   => AkkaConsumerMessage,
  TransactionalMessage => AkkaTransactionalMessage
}
import akka.kafka.ProducerMessage.{Message => AkkaProducerMessage, MultiMessage => AkkaMultiMessage}
import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.{
  LikeConsumerRecord,
  LikeProducerRecord,
  LikeProducerRecords
}
import fs2.kafka.{
  CommittableConsumerRecord => Fs2CommittableConsumerRecord,
  ConsumerRecord            => Fs2ConsumerRecord,
  ProducerRecord            => Fs2ProducerRecord,
  ProducerRecords           => Fs2ProducerRecords
}
import monocle.law.discipline.LensTests
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class LensTest extends AnyFunSuite with Discipline {

  checkAll(
    "fs2.consumer.CommittableConsumerRecord",
    LensTests(LikeConsumerRecord[Fs2CommittableConsumerRecord[IO, *, *]].lens[Int, Int, Int, Int]))

  checkAll(
    "fs2.consumer.ConsumerRecord",
    LensTests(LikeConsumerRecord[Fs2ConsumerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "fs2.producer.ProducerRecord",
    LensTests(LikeProducerRecord[Fs2ProducerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "kafka.consumer.ConsumerRecord",
    LensTests(LikeConsumerRecord[ConsumerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "kafka.producer.ProducerRecord",
    LensTests(LikeProducerRecord[ProducerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.consumer.ConsumerMessage",
    LensTests(LikeConsumerRecord[AkkaConsumerMessage].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.consumer.TransactionalMessage",
    LensTests(LikeConsumerRecord[AkkaTransactionalMessage].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.producer.ProducerMessage",
    LensTests(LikeProducerRecord[AkkaProducerMessage[*, *, String]].lens[Int, Int, Int, Int]))

}
