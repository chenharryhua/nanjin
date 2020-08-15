package mtest.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef

class ConsumerGroupIdTest extends AnyFunSuite {
  val t1      = TopicDef[Int, Int](TopicName("consumer.group.id")).in(ctx)
  val testcid = "my-consumer-group-id"
  test("should be able to change consumer group id") {
    val t2 = t1.withGroupId(testcid)
    assert(t2.context.settings.consumerSettings.config(ConsumerConfig.GROUP_ID_CONFIG) === testcid)
  }
}
