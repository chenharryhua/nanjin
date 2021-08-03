package com.github.chenharryhua.nanjin.kafka

import mtest.kafka.ctx
import org.scalatest.funsuite.AnyFunSuite

class KafkaTopicCodecTest extends AnyFunSuite {
  val topic = ctx.topic[Int, Int]("na")
  test("topic name should be same") {
    assertThrows[Exception](
      new RegisteredKeyValueSerdePair[Int, Int](
        topic.topicDef.rawSerdes.keySerde.asKey(Map()).codec("a"),
        topic.topicDef.rawSerdes.valSerde.asValue(Map()).codec("b")))
  }
}
