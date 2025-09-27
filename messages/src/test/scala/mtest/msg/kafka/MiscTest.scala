package mtest.msg.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJHeader, NJProducerRecord}
import fs2.kafka.{ConsumerRecord, ProducerRecord}
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import io.scalaland.chimney.dsl.*
import org.scalatest.funsuite.AnyFunSuite
class MiscTest extends AnyFunSuite {

  test("consumer record equal") {
    val cr: NJConsumerRecord[Int, Int] = NJConsumerRecord[Int, Int](
      topic = "topic",
      partition = 1,
      offset = 1,
      timestamp = 1,
      timestampType = 1,
      serializedKeySize = Some(1),
      serializedValueSize = Some(1),
      key = None,
      value = Some(1),
      headers = List(NJHeader("header", "header".getBytes().toList)),
      leaderEpoch = Some(1)
    )

    val cr1: NJConsumerRecord[Int, Int] = decode[NJConsumerRecord[Int, Int]](cr.asJson.noSpaces).toOption.get
    assert(cr1.eqv(cr))

    val cr2: ConsumerRecord[Int, Int] = cr.transformInto[ConsumerRecord[Int, Int]]
    val cr3: ConsumerRecord[Int, Int] = cr1.transformInto[ConsumerRecord[Int, Int]]
    assert(cr2.eqv(cr3))
  }

  test("producer record equal") {
    val pr = NJProducerRecord[Int, Int](
      topic = "topic",
      partition = Some(1),
      offset = None,
      timestamp = None,
      key = Some(1),
      value = None,
      headers = List(NJHeader("header", "header".getBytes().toList)))
    val pr1 = decode[NJProducerRecord[Int, Int]](pr.asJson.noSpaces).toOption.get
      .transformInto[ProducerRecord[Int, Int]]
    assert(pr1.eqv(pr.toProducerRecord))
  }
}
