package mtest.kafka

import cats.implicits._
import fs2.kafka.{ConsumerRecord => Fs2ConsumerRecord, ProducerRecord => Fs2ProducerRecord}
import monocle.law.discipline.IsoTests
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import com.github.chenharryhua.nanjin.messages.kafka._

class IsoTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  checkAll(
    "fs2.ProducerRecord",
    IsoTests[Fs2ProducerRecord[Int, Int], ProducerRecord[Int, Int]](
      isoFs2ProducerRecord[Int, Int]))

  checkAll(
    "fs2.ConsumerRecord",
    IsoTests[Fs2ConsumerRecord[Int, Int], ConsumerRecord[Int, Int]](
      isoFs2ComsumerRecord[Int, Int]))
}
