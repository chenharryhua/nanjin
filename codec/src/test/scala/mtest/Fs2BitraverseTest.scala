package mtest

import cats.effect.IO
import cats.implicits._
import cats.laws.discipline.BitraverseTests
import fs2.kafka.{CommittableConsumerRecord, ProducerRecords}
import org.scalacheck.Arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class Fs2BitraverseTest extends AnyFunSuite with Discipline with Fs2MessageGen {
  implicit val fs2CM: Arbitrary[CommittableConsumerRecord[IO, Int, Int]] =
    Arbitrary(genFs2ConsumerMessage)
  implicit val fs2PM: Arbitrary[ProducerRecords[Int, Int, String]] =
    Arbitrary(genFs2ProducerMessage)

  checkAll(
    "Fs2-CommittableConsumerRecord",
    BitraverseTests[CommittableConsumerRecord[IO, ?, ?]]
      .bitraverse[Option, Int, Int, Int, Int, Int, Int])

  checkAll(
    "Fs2-ProducerRecords",
    BitraverseTests[ProducerRecords[?, ?, String]].bitraverse[Option, Int, Int, Int, Int, Int, Int])

}
