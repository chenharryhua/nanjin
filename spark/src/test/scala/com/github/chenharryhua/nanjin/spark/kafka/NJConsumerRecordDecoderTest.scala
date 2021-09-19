package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.kafka.KeyValueCodecPair
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import fs2.kafka.ConsumerRecord
import org.scalatest.funsuite.AnyFunSuite

class NJConsumerRecordDecoderTest extends AnyFunSuite {

  val decoder: KeyValueCodecPair[Int, Int] =
    KeyValueCodecPair(SerdeOf[Int].asKey(Map.empty).codec("test"), SerdeOf[Int].asValue(Map.empty).codec("test"))

  val goodData: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0, 0, 0, 1), Array[Byte](0, 0, 0, 2))

  val badKey: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0), Array[Byte](0, 0, 0, 2))

  val badVal: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0, 0, 0, 1), Array[Byte](0))

  val badKV: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0), Array[Byte](0))

  test("decode good key value") {
    val rst = NJConsumerRecordWithError(decoder, goodData)
    assert(rst.key.contains(1))
    assert(rst.value.contains(2))
  }

  test("decode bad key") {
    val rst = NJConsumerRecordWithError(decoder, badKey)
    assert(rst.value.contains(2))
    assert(rst.key.isLeft)
  }
  test("decode bad value") {
    val rst = NJConsumerRecordWithError(decoder, badVal)
    assert(rst.key.contains(1))
    assert(rst.value.isLeft)
  }
  test("decode bad key vaule") {
    val rst = NJConsumerRecordWithError(decoder, badKV)
    assert(rst.key.isLeft)
    assert(rst.value.isLeft)
  }
}
