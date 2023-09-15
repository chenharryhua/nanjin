package mtest.msg.kafka

import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import fs2.kafka.{ConsumerRecord as Fs2ConsumerRecord, ProducerRecord as Fs2ProducerRecord}
import monocle.law.discipline.IsoTests
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
class IsoTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {
  import ArbitraryData.*

  checkAll(
    "fs2.ProducerRecord",
    IsoTests[Fs2ProducerRecord[Int, Int], ProducerRecord[Int, Int]](isoFs2ProducerRecord[Int, Int]))
  checkAll(
    "kafka.ProducerRecord",
    IsoTests[ProducerRecord[Int, Int], ProducerRecord[Int, Int]](isoIdentityProducerRecord[Int, Int]))

  checkAll(
    "fs2.ConsumerRecord",
    IsoTests[Fs2ConsumerRecord[Int, Int], ConsumerRecord[Int, Int]](isoFs2ConsumerRecord[Int, Int]))

  checkAll(
    "kafka.ConsumerRecord",
    IsoTests[ConsumerRecord[Int, Int], ConsumerRecord[Int, Int]](isoIdentityConsumerRecord[Int, Int]))

}
