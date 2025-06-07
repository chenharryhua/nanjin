package mtest.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import eu.timepit.refined.auto.*
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

}
