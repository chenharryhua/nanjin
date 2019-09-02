package mtest

import akka.kafka.{ConsumerMessage, ProducerMessage}
import cats.implicits._
import cats.laws.discipline.BitraverseTests
import org.scalacheck.Arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class AkkaBitraverseTest extends AnyFunSuite with Discipline with AkkaMessageGen {
  implicit val akkaCM: Arbitrary[ConsumerMessage.CommittableMessage[Int, Int]] =
    Arbitrary(genAkkaConsumerMessage)
  implicit val akkaPM: Arbitrary[ProducerMessage.Message[Int, Int, String]] =
    Arbitrary(genAkkaProducerMessage)

  checkAll(
    "Akka-CommittableMessage",
    BitraverseTests[ConsumerMessage.CommittableMessage]
      .bitraverse[List, Int, Int, Int, Int, Int, Int])

  checkAll(
    "Akka-ProducerMessage",
    BitraverseTests[ProducerMessage.Message[?, ?, String]]
      .bitraverse[List, Int, Int, Int, Int, Int, Int])

}
