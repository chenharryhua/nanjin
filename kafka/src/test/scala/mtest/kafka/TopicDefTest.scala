package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.kafka.serdes.Primitive
import munit.FunSuite

class TopicDefTest extends FunSuite {

  test("1.TopicDef.toString contains the topic name") {
    val topic = TopicDef("topic-def-to-string", Primitive[String], Primitive[java.lang.Integer])
    assert(topic.toString.contains("topic-def-to-string"))
  }

  test("2.TopicSerde.toString contains the topic name") {
    val topic = TopicDef("topic-serde-to-string", Primitive[String], Primitive[java.lang.Integer])
    val serde = ctx.serde(topic)
    assert(serde.toString.contains("topic-serde-to-string"))
  }
}
