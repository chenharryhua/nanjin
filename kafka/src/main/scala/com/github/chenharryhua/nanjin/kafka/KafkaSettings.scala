package com.github.chenharryhua.nanjin.kafka

import cats.Show
import fs2.kafka.AdminClientSettings
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol

/** [[https://kafka.apache.org/]]
  */

final case class KafkaConsumerSettings(properties: Map[String, String]) {
  def withProperty(key: String, value: String): KafkaConsumerSettings =
    copy(properties = properties.updated(key, value))
}

final case class KafkaProducerSettings(properties: Map[String, String]) {
  def withProperty(key: String, value: String): KafkaProducerSettings =
    copy(properties = properties.updated(key, value))
}

final case class KafkaStreamSettings(properties: Map[String, String]) {
  def withProperty(key: String, value: String): KafkaStreamSettings =
    copy(properties = properties.updated(key, value))
}

final case class SchemaRegistrySettings(config: Map[String, String]) {
  def withProperty(key: String, value: String): SchemaRegistrySettings =
    copy(config = config.updated(key, value))
}

final case class KafkaSettings(
  consumerSettings: KafkaConsumerSettings,
  producerSettings: KafkaProducerSettings,
  adminSettings: AdminClientSettings,
  streamSettings: KafkaStreamSettings,
  schemaRegistrySettings: SchemaRegistrySettings) {

  def withBrokers(brokers: String): KafkaSettings =
    KafkaSettings(
      consumerSettings.withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
      producerSettings.withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
      adminSettings.withBootstrapServers(brokers),
      streamSettings.withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
      schemaRegistrySettings
    )

  def withSaslJaas(sasl: String): KafkaSettings =
    KafkaSettings(
      consumerSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      producerSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      adminSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      streamSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      schemaRegistrySettings
    )

  def withSecurityProtocol(sp: SecurityProtocol): KafkaSettings =
    KafkaSettings(
      consumerSettings.withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name),
      producerSettings.withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name),
      adminSettings.withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name),
      streamSettings.withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name),
      schemaRegistrySettings
    )

  def withProducerProperty(key: String, value: String): KafkaSettings =
    copy(producerSettings = producerSettings.withProperty(key, value))

  def withConsumerProperty(key: String, value: String): KafkaSettings =
    copy(consumerSettings = consumerSettings.withProperty(key, value))

  def withStreamingProperty(key: String, value: String): KafkaSettings =
    copy(streamSettings = streamSettings.withProperty(key, value))

  def withSchemaRegistryProperty(key: String, value: String): KafkaSettings =
    copy(schemaRegistrySettings = schemaRegistrySettings.withProperty(key, value))

}

object KafkaSettings {
  implicit val showKafkaSettings: Show[KafkaSettings] = cats.derived.semiauto.show[KafkaSettings]

  def apply(brokers: String, schemaRegistry: String): KafkaSettings =
    KafkaSettings(
      KafkaConsumerSettings(Map.empty),
      KafkaProducerSettings(Map.empty),
      AdminClientSettings(brokers),
      KafkaStreamSettings(Map.empty),
      SchemaRegistrySettings(Map.empty)
    ).withBrokers(brokers)
      .withSchemaRegistryProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry)
      .withSecurityProtocol(SecurityProtocol.PLAINTEXT)

  val local: KafkaSettings = apply("localhost:9092", "http://localhost:8081")
}
