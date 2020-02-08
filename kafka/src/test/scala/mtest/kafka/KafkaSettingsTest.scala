package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaSettings
import org.apache.kafka.clients.CommonClientConfigs
import org.scalatest.funsuite.AnyFunSuite

class KafkaSettingsTest extends AnyFunSuite {
  val setting: KafkaSettings = KafkaSettings("broker-url", "schema-registry-url")

  test("should allow independently change properties") {
    val p = setting.withProducerProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "producer")
    assert(p.producerSettings.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "producer")
    assert(p.consumerSettings.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) === "broker-url")
  }
}
