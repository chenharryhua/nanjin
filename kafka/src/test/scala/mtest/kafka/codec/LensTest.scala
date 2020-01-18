package mtest.kafka.codec

import akka.kafka.ConsumerMessage.{
  CommittableMessage   => AkkaConsumerMessage,
  TransactionalMessage => AkkaTransactionalMessage
}
import akka.kafka.ProducerMessage.{Message => AkkaProducerMessage}
import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec._
import com.github.chenharryhua.nanjin.kafka.codec.eq._
import com.github.chenharryhua.nanjin.kafka.codec.{NJConsumerMessage, NJProducerMessage}
import fs2.kafka.{
  CommittableConsumerRecord => Fs2CommittableConsumerRecord,
  ConsumerRecord            => Fs2ConsumerRecord,
  ProducerRecord            => Fs2ProducerRecord
}
import monocle.law.discipline.LensTests
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import org.scalatest.prop.Configuration

class LensTest extends AnyFunSuite with FunSuiteDiscipline with Configuration{

  checkAll(
    "fs2.consumer.CommittableConsumerRecord",
    LensTests(NJConsumerMessage[Fs2CommittableConsumerRecord[IO, *, *]].lens[Int, Int, Int, Int]))

  checkAll(
    "fs2.consumer.ConsumerRecord",
    LensTests(NJConsumerMessage[Fs2ConsumerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "fs2.producer.ProducerRecord",
    LensTests(NJProducerMessage[Fs2ProducerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "kafka.consumer.ConsumerRecord",
    LensTests(NJConsumerMessage[ConsumerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "kafka.producer.ProducerRecord",
    LensTests(NJProducerMessage[ProducerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.consumer.ConsumerMessage",
    LensTests(NJConsumerMessage[AkkaConsumerMessage].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.consumer.TransactionalMessage",
    LensTests(NJConsumerMessage[AkkaTransactionalMessage].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.producer.ProducerMessage",
    LensTests(NJProducerMessage[AkkaProducerMessage[*, *, String]].lens[Int, Int, Int, Int]))

}
