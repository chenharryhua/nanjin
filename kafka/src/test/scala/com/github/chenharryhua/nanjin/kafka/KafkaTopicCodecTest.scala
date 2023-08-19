package com.github.chenharryhua.nanjin.kafka

import mtest.kafka.ctx
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*

class KafkaTopicCodecTest extends AnyFunSuite {
  val topic = ctx.topic[Int, Int]("na")
  test("topic name should be same") {
    assertThrows[Exception](
      new KeyValueSerdePair[Int, Int](
        topic.topicDef.rawSerdes.key.asKey(Map()).topic("a"),
        topic.topicDef.rawSerdes.value.asValue(Map()).topic("b")))
  }
}
