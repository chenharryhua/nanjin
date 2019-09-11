package mtest

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaConsumerMessage}
import akka.kafka.ProducerMessage.{Message            => AkkaProducerMessage}
import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.{LikeConsumerRecord, LikeProducerRecord}
import fs2.kafka.{
  CommittableConsumerRecord,
  ConsumerRecord => Fs2ConsumerRecord,
  ProducerRecord => Fs2ProducerRecord
}
import monocle.law.discipline.LensTests
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import com.github.chenharryhua.nanjin.codec.EqMessage

class LensTest extends AnyFunSuite with Discipline with EqMessage{

  checkAll(
    "fs2.CommittableConsumerRecord",
    LensTests(LikeConsumerRecord[CommittableConsumerRecord[IO, *, *]].lens[Int, Int, Int, Int]))

  checkAll(
    "fs2.ConsumerRecord",
    LensTests(LikeConsumerRecord[Fs2ConsumerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "fs2.ProducerRecord",
    LensTests(LikeProducerRecord[Fs2ProducerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "kafka.ConsumerRecord",
    LensTests(LikeConsumerRecord[ConsumerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "kafka.ProducerRecord",
    LensTests(LikeProducerRecord[ProducerRecord].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.ConsumerMessage",
    LensTests(LikeConsumerRecord[AkkaConsumerMessage].lens[Int, Int, Int, Int]))

  checkAll(
    "akka.ProducerMessage",
    LensTests(LikeProducerRecord[AkkaProducerMessage[*, *, String]].lens[Int, Int, Int, Int]))

}
