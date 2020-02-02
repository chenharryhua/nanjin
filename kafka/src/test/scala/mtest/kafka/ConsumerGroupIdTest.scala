package mtest.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.funsuite.AnyFunSuite

class ConsumerGroupIdTest extends AnyFunSuite {
  val t1      = ctx.topic[Int, Int]("consumer.group.id")
  val testcid = "my-consumer-group-id"
  test("should be able to change consumer group id") {
    val t2 = t1.withConsumerGroupId(testcid)
    assert(t2.consumerGroupId.get.value === testcid)
    assert(t2.kit.settings.consumerSettings.config(ConsumerConfig.GROUP_ID_CONFIG) === testcid)
  }
}
