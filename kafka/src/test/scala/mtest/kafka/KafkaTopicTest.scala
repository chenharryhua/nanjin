package mtest.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import eu.timepit.refined.auto.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

class KafkaTopicTest extends AnyFunSuite {
  private val t1 = ctx.topic(TopicDef[Int, Int](TopicName("topic")))
  test("equality") {
    assert(!t1.topicDef.eqv(t1.topicDef.withTopicName("abc")))
  }
  test("show topic") {
    println(t1.topicDef.show)
  }
  test("with clause") {
    t1.withTopicName("new.name")
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
