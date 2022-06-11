package mtest.msg.kafka

import akka.kafka.{ConsumerMessage as AkkaConsumerMessage, ProducerMessage as AkkaProducerMessage}
import cats.Id
import cats.effect.IO
import cats.laws.discipline.BitraverseTests
import com.github.chenharryhua.nanjin.messages.kafka.BitraverseMessages.*
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage.*
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerMessage.*
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import fs2.kafka.{
  CommittableConsumerRecord as Fs2CommittableConsumerRecord,
  CommittableProducerRecords as Fs2CommittableProducerRecords,
  ConsumerRecord as Fs2ConsumerRecord,
  ProducerRecord as Fs2ProducerRecord
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
class BitraverseTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {
  import ArbitraryData.*

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
    "fs2.producer.CommittableProducerRecords",
    BitraverseTests[Fs2CommittableProducerRecords[IO, *, *]].bitraverse[Option, Int, Int, Int, Int, Int, Int])

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
