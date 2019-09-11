package mtest

import cats.implicits._
import fs2.kafka.{ConsumerRecord => Fs2ConsumerRecord, ProducerRecord => Fs2ProducerRecord}
import monocle.law.discipline.IsoTests
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.Arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import com.github.chenharryhua.nanjin.codec.EqMessage

class IsoTest extends AnyFunSuite with Discipline with EqMessage{

  checkAll(
    "fs2.ProducerRecord",
    IsoTests[Fs2ProducerRecord[Int, Int], ProducerRecord[Int, Int]](isoFs2ProducerRecord[Int, Int]))

  checkAll(
    "fs2.ConsumerRecord",
    IsoTests[Fs2ConsumerRecord[Int, Int], ConsumerRecord[Int, Int]](isoFs2ComsumerRecord[Int, Int]))
}
