package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.serdes.Primitive
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import fs2.kafka.ConsumerRecord
import org.scalatest.funsuite.AnyFunSuite

class NJConsumerRecordDecoderTest extends AnyFunSuite {

  val topic = TopicDef[Integer, Integer](TopicName("decode.test"), Primitive[Integer], Primitive[Integer])
  val goodData: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0, 0, 0, 1), Array[Byte](0, 0, 0, 2))

  val badKey: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0), Array[Byte](0, 0, 0, 2))

  val badVal: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0, 0, 0, 1), Array[Byte](0))

  val badKV: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0), Array[Byte](0))

  val serde = ctx.serde(topic)
  test("1.decode good key value") {
    val rst = serde.optionalDeserialize(goodData)
    assert(rst.key.contains(1))
    assert(rst.value.contains(2))
  }

  test("2.decode bad key") {
    val rst = serde.optionalDeserialize(badKey)
    assert(rst.value.contains(2))
    assert(rst.key.isEmpty)
  }
  test("3.decode bad value") {
    val rst = serde.optionalDeserialize(badVal)
    assert(rst.key.contains(1))
    assert(rst.value.isEmpty)
  }
  test("4.decode bad key vaule") {
    val rst = serde.optionalDeserialize(badKV)
    assert(rst.key.isEmpty)
    assert(rst.value.isEmpty)
  }

}
