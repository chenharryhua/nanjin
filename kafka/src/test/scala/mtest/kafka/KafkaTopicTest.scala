package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.{KafkaSettings, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import org.scalatest.funsuite.AnyFunSuite
import cats.syntax.all._

class KafkaTopicTest extends AnyFunSuite {
  val t1 = ctx.topic[Int, Int]("topic")
  val t2 = TopicDef[Int, Int](TopicName("topic")).in(ctx)
  val t3 = TopicDef[Int, Int](TopicName("topic"), AvroCodec[Int]).in(ctx)
  val t4 = TopicDef[Int, Int](TopicName("topic"), AvroCodec[Int], AvroCodec[Int]).in(ctx)
  test("equality") {
    assert(t1.topicDef.eqv(t2.topicDef))
    assert(t1.topicDef.eqv(t3.topicDef))
    assert(t1.topicDef.eqv(t4.topicDef))
    assert(!t1.topicDef.eqv(t1.topicDef.withTopicName("abc")))
  }
  test("show topic") {
    println(t1.show)
    println(t1.topicDef.show)
  }
  test("with clause") {
    t1.withContext(ctx)
    t1.withGroupId("cid")
    t1.withSettings(KafkaSettings.local)
    t1.withTopicName("new.name")
  }
}
