package com.github.chenharryhua.nanjin.spark.kafka

import cats.data.{Chain, Writer}
import com.github.chenharryhua.nanjin.kafka.RegisteredKeyValueSerdePair
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import fs2.kafka.ConsumerRecord
import org.scalatest.funsuite.AnyFunSuite

class NJConsumerRecordDecoderOptionalKVTest extends AnyFunSuite {

  val decoder = new NJDecoder[Writer[Chain[Throwable], *], Int, Int](
    RegisteredKeyValueSerdePair(
      SerdeOf[Int].asKey(Map.empty).codec("test"),
      SerdeOf[Int].asValue(Map.empty).codec("test")))

  val goodData: NJConsumerRecord[Array[Byte], Array[Byte]] =
    NJConsumerRecord(0, 0, 0, Some(Array[Byte](0, 0, 0, 1)), Some(Array[Byte](0, 0, 0, 2)), "test", 0)

  val badKey: NJConsumerRecord[Array[Byte], Array[Byte]] =
    NJConsumerRecord(0, 0, 0, Some(Array[Byte](0)), Some(Array[Byte](0, 0, 0, 2)), "test", 0)

  val badVal: NJConsumerRecord[Array[Byte], Array[Byte]] =
    NJConsumerRecord(0, 0, 0, Some(Array[Byte](0, 0, 0, 1)), Some(Array[Byte](0)), "test", 0)

  val badKV: NJConsumerRecord[Array[Byte], Array[Byte]] =
    NJConsumerRecord(0, 0, 0, Some(Array[Byte](0)), Some(Array[Byte](0)), "test", 0)

  test("decode good key value") {
    val (err, data) = decoder.decode(goodData).run
    assert(err.isEmpty)
    assert(data == goodData.newKey(Some(1)).newValue(Some(2)))
  }
  test("decode bad key") {
    val (err, data) = decoder.decode(badKey).run
    assert(err.size == 1)
    assert(data.value == badKey.newValue(Some(2)).value)
    assert(data.key.isEmpty)
  }
  test("decode bad value") {
    val (err, data) = decoder.decode(badVal).run
    assert(err.size == 1)
    assert(data.key == badVal.newKey(Some(1)).key)
    assert(data.value.isEmpty)
  }
  test("decode bad key value") {
    val (err, data) = decoder.decode(badKV).run
    assert(err.size == 2)
    assert(data.key.isEmpty)
    assert(data.value.isEmpty)
  }
}

class NJConsumerRecordDecoderTest extends AnyFunSuite {

  val decoder = new NJDecoder[Writer[Chain[Throwable], *], Int, Int](
    RegisteredKeyValueSerdePair(
      SerdeOf[Int].asKey(Map.empty).codec("test"),
      SerdeOf[Int].asValue(Map.empty).codec("test")))

  val goodData: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0, 0, 0, 1), Array[Byte](0, 0, 0, 2))

  val badKey: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0), Array[Byte](0, 0, 0, 2))

  val badVal: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0, 0, 0, 1), Array[Byte](0))

  val badKV: ConsumerRecord[Array[Byte], Array[Byte]] =
    ConsumerRecord("test", 0, 0, Array[Byte](0), Array[Byte](0))

  test("decode good key value") {
    val (err, data) = decoder.decode(goodData).run
    assert(err.isEmpty)
    assert(data.key.contains(1))
    assert(data.value.contains(2))
  }
  test("decode bad key") {
    val (err, data) = decoder.decode(badKey).run
    assert(err.size == 1)
    assert(data.value.contains(2))
    assert(data.key.isEmpty)
  }
  test("decode bad value") {
    val (err, data) = decoder.decode(badVal).run
    assert(err.size == 1)
    assert(data.key.contains(1))
    assert(data.value.isEmpty)
  }
  test("decode bad key vaule") {
    val (err, data) = decoder.decode(badKV).run
    assert(err.size == 2)
    assert(data.key.isEmpty)
    assert(data.value.isEmpty)
  }
}
