package mtest.msg.kafka

import akka.kafka.{ConsumerMessage => AkkaConsumerMessage, ProducerMessage => AkkaProducerMessage}
import cats.Id
import cats.effect.IO
import cats.laws.discipline.BitraverseTests
import fs2.kafka.{
  CommittableConsumerRecord => Fs2CommittableConsumerRecord,
  CommittableProducerRecords => Fs2CommittableProducerRecords,
  ConsumerRecord => Fs2ConsumerRecord,
  ProducerRecord => Fs2ProducerRecord,
  ProducerRecords => Fs2ProducerRecords,
  TransactionalProducerRecords => Fs2TransactionalProducerRecords
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import com.github.chenharryhua.nanjin.messages.kafka._
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage._
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerMessage._
import com.github.chenharryhua.nanjin.messages.kafka.BitraverseMessages._
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
class BitraverseTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  implicit val arbChain: Arbitrary[List[Int]] =
    Arbitrary(Gen.containerOfN[List, Int](3, arbitrary[Int]))

  checkAll(
    "fs2.consumer.CommittableConsumerRecord",
    BitraverseTests[Fs2CommittableConsumerRecord[IO, *, *]].bitraverse[Id, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.consumer.ConsumerRecord",
    BitraverseTests[Fs2ConsumerRecord].bitraverse[Option, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.producer.ProducerRecord",
    BitraverseTests[Fs2ProducerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.producer.ProducerRecords",
    BitraverseTests[Fs2ProducerRecords[String, *, *]].bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.producer.CommittableProducerRecords",
    BitraverseTests[Fs2CommittableProducerRecords[IO, *, *]].bitraverse[Option, Int, Int, Int, Int, Int, Int])

  checkAll(
    "fs2.producer.TransactionalProducerRecords",
    BitraverseTests[Fs2TransactionalProducerRecords[IO, String, *, *]].bitraverse[Option, Int, Int, Int, Int, Int, Int]
  )

  checkAll(
    "akka.producer.ProducerMessage",
    BitraverseTests[AkkaProducerMessage.Message[*, *, String]]
      .bitraverse[Either[String, *], Int, Int, Int, Int, Int, Int])

  checkAll(
    "akka.consumer.CommittableMessage",
    BitraverseTests[AkkaConsumerMessage.CommittableMessage].bitraverse[Either[Long, *], Int, Int, Int, Int, Int, Int])

  checkAll(
    "akka.consumer.TransactionalMessage",
    BitraverseTests[AkkaConsumerMessage.TransactionalMessage].bitraverse[Either[Long, *], Int, Int, Int, Int, Int, Int])

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
