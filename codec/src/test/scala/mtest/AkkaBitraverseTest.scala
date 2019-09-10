package mtest

import akka.kafka.{ConsumerMessage, ProducerMessage}
import cats.Bitraverse
import cats.implicits._
import cats.laws.discipline.BitraverseTests
import com.github.chenharryhua.nanjin.codec.{LikeConsumerRecord, LikeProducerRecord}
import org.scalacheck.Arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class AkkaBitraverseTest extends AnyFunSuite with Discipline with AkkaMessageGen {
  implicit val akkaCM: Arbitrary[ConsumerMessage.CommittableMessage[Int, Int]] =
    Arbitrary(genAkkaConsumerMessage)
  implicit val akkaPM: Arbitrary[ProducerMessage.Message[Int, Int, String]] =
    Arbitrary(genAkkaProducerMessage)

  implicit val pmBitraverse = LikeProducerRecord[ProducerMessage.Message[*, *, String]]
  implicit val cmBitraverse = LikeConsumerRecord[ConsumerMessage.CommittableMessage]

  checkAll(
    "Akka-CommittableMessage",
    BitraverseTests[ConsumerMessage.CommittableMessage]
      .bitraverse[Option, Int, Int, Int, Int, Int, Int])

  implicit val inst: Bitraverse[ProducerMessage.Message[*, *, String]] =
    LikeProducerRecord[ProducerMessage.Message[*, *, String]]
  checkAll(
    "Akka-ProducerMessage",
    BitraverseTests[ProducerMessage.Message[*, *, String]]
      .bitraverse[List, Int, Int, Int, Int, Int, Int])
}
