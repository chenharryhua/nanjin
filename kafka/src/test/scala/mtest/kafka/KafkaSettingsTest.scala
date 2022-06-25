package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaSettings
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.funsuite.AnyFunSuite

class KafkaSettingsTest extends AnyFunSuite {
  val setting: KafkaSettings = KafkaSettings("broker-url", "schema-registry-url")

  test("should allow independently change properties") {
    val p = setting
      .withProducerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "producer")
      .withEarliestAutoOffset
    assert(p.producerSettings.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "producer")
    assert(p.consumerSettings.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker-url")
    assert(p.adminSettings.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker-url")
    assert(p.consumerSettings.config(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) === "earliest")
    assert(p.streamSettings.config(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) === "earliest")
    val p2 = p.withLatestAutoOffset
    assert(p2.consumerSettings.config(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) === "latest")
    assert(p2.streamSettings.config(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) === "latest")
  }
}
