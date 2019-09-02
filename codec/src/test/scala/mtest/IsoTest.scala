package mtest

import cats.implicits._
import fs2.kafka.{ConsumerRecord => Fs2ConsumerRecord, ProducerRecord => Fs2ProducerRecord}
import monocle.law.discipline.IsoTests
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.Arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class IsoTest extends AnyFunSuite with Discipline with Fs2MessageGen {
  implicit val abFs2ProducerRecord: Arbitrary[Fs2ProducerRecord[Int, Int]] =
    Arbitrary(genFs2ProducerRecord)
  implicit val abProducerRecord: Arbitrary[ProducerRecord[Int, Int]] = Arbitrary(genProducerRecord)

  implicit val abProducerRecordF: Arbitrary[ProducerRecord[Int, Int] => ProducerRecord[Int, Int]] =
    Arbitrary((a: ProducerRecord[Int, Int]) => a)

  checkAll(
    "Fs2-ProducerRecord",
    IsoTests[Fs2ProducerRecord[Int, Int], ProducerRecord[Int, Int]](isoFs2ProducerRecord[Int, Int]))

  implicit val abConsumerRecord: Arbitrary[ConsumerRecord[Int, Int]] =
    Arbitrary(genConsumerRecord)
  implicit val abFs2ConsumerRecord: Arbitrary[Fs2ConsumerRecord[Int, Int]] = Arbitrary(
    genFs2ConsumerRecord)

  implicit val abConsumerRecordF: Arbitrary[ConsumerRecord[Int, Int] => ConsumerRecord[Int, Int]] =
    Arbitrary((a: ConsumerRecord[Int, Int]) => a)

  checkAll(
    "Fs2-ConsumerRecord",
    IsoTests[Fs2ConsumerRecord[Int, Int], ConsumerRecord[Int, Int]](isoFs2ComsumerRecord[Int, Int]))

}
