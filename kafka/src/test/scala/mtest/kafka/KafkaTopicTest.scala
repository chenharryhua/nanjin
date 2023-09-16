package mtest.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, NJAvroCodec}
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*
import io.circe.Json

class KafkaTopicTest extends AnyFunSuite {
  val t1 = ctx.topic[Int, Int]("topic")
  val t2 = TopicDef[Int, Int](TopicName("topic")).in(ctx)
  val t3 = TopicDef[Int, Int](TopicName("topic"), NJAvroCodec[Int]).in(ctx)
  val t4 = TopicDef[Int, Int](TopicName("topic"), NJAvroCodec[Int], NJAvroCodec[Int]).in(ctx)
  test("equality") {
    assert(t1.topicDef.eqv(t2.topicDef))
    assert(t1.topicDef.eqv(t3.topicDef))
    assert(t1.topicDef.eqv(t4.topicDef))
    assert(!t1.topicDef.eqv(t1.topicDef.withTopicName("abc")))
  }
  test("show topic") {
    println(t1.topicDef.show)
  }
  test("with clause") {
    t1.withTopicName("new.name")
  }

  test("valid name") {
    ctx.topic[Int, Int]("topic.1")
  }

  test("as value") {
    ctx.asValue[Boolean]
    ctx.asValue[Int]
    ctx.asValue[Long]
    ctx.asValue[Float]
    ctx.asValue[Double]
    ctx.asValue[String]
    ctx.asValue[Array[Byte]]
    ctx.asValue[KJson[Json]]
  }

  test("as key") {
    ctx.asKey[Boolean]
    ctx.asKey[Int]
    ctx.asKey[Long]
    ctx.asKey[Float]
    ctx.asKey[Double]
    ctx.asKey[String]
    ctx.asKey[Array[Byte]]
    ctx.asKey[KJson[Json]]
  }

}
