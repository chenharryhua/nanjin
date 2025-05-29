package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef}
import eu.timepit.refined.auto.*
import fs2.kafka.ConsumerRecord
import org.scalatest.funsuite.AnyFunSuite

class NJConsumerRecordDecoderTest extends AnyFunSuite {


  val topic: KafkaTopic[IO, Int, Int] = ctx.topic(TopicDef[Int, Int](TopicName("decode.test")))
  val goodData: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0, 0, 0, 1), Array[Byte](0, 0, 0, 2))

  val badKey: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0), Array[Byte](0, 0, 0, 2))

  val badVal: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0, 0, 0, 1), Array[Byte](0))

  val badKV: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0), Array[Byte](0))
  test("decode good key value") {
    val rst = topic.serde.toNJConsumerRecord(goodData)
    assert(rst.key.contains(1))
    assert(rst.value.contains(2))
  }

  test("decode bad key") {
    val rst = topic.serde.toNJConsumerRecord(badKey)
    assert(rst.value.contains(2))
    assert(rst.key.isEmpty)
  }
  test("decode bad value") {
    val rst = topic.serde.toNJConsumerRecord(badVal)
    assert(rst.key.contains(1))
    assert(rst.value.isEmpty)
  }
  test("decode bad key vaule") {
    val rst = topic.serde.toNJConsumerRecord(badKV)
    assert(rst.key.isEmpty)
    assert(rst.value.isEmpty)
  }

}
