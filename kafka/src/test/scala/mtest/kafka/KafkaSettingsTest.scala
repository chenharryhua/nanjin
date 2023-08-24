package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaSettings
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.funsuite.AnyFunSuite

class KafkaSettingsTest extends AnyFunSuite {
  val setting: KafkaSettings = KafkaSettings("broker-url", "schema-registry-url")
    .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  test("should allow independently change properties") {
    val p = setting.withProducerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "producer")
    assert(p.producerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "producer")
    assert(p.consumerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker-url")
    assert(p.consumerSettings.properties(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) === "earliest")

    val b = setting.withBrokers("broker")
    assert(b.consumerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker")
    assert(b.producerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker")
    assert(b.streamSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker")
  }
}
