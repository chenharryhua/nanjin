package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaSettings
import fs2.kafka.AutoOffsetReset
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.funsuite.AnyFunSuite

class KafkaSettingsTest extends AnyFunSuite {
  val setting: KafkaSettings = KafkaSettings("broker-url", "schema-registry-url").withConsumer(
    _.withAutoOffsetReset(AutoOffsetReset.Earliest))

  test("should allow independently change properties") {
    val p = setting.withProducerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "producer")
    assert(p.producerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "producer")
    assert(p.consumerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker-url")
    assert(p.consumerSettings.properties(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) === "earliest")
    assert(
      p.withConsumer(_.withGroupId("abc"))
        .consumerSettings
        .properties(CommonClientConfigs.GROUP_ID_CONFIG) === "abc")

    assert(
      p.withProducer(_.withClientId("abc"))
        .producerSettings
        .properties(CommonClientConfigs.CLIENT_ID_CONFIG) === "abc")

    assert(
      p.withAdmin(_.withClientId("abc"))
        .adminSettings
        .properties(CommonClientConfigs.CLIENT_ID_CONFIG) === "abc")

    val b = setting.withBrokers("broker")
    assert(b.consumerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker")
    assert(b.producerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker")
    assert(b.streamSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker")
  }
}
