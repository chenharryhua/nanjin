package mtest

import cats.implicits._
import cats.laws.discipline.BitraverseTests
import fs2.kafka.{ProducerRecord => Fs2ProducerRecord}
import org.scalacheck.Arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class Fs2BitraverseTest extends AnyFunSuite with Discipline with Fs2MessageGen {
  implicit val fs2pr: Arbitrary[Fs2ProducerRecord[Int, Int]] = Arbitrary(genFs2ProducerRecord)

}
