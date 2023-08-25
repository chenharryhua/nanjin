package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.effect.IO
import com.github.chenharryhua.nanjin.common.utils
import fs2.kafka.AdminClientSettings
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.typelevel.cats.time.instances.zoneid

import java.time.ZoneId
import java.util.Properties

/** [[https://kafka.apache.org/]]
  */

final case class KafkaConsumerSettings(properties: Map[String, String]) {
  def withProperty(key: String, value: String): KafkaConsumerSettings =
    copy(properties = properties.updatedWith(key)(_ => Some(value)))
}

final case class KafkaProducerSettings(properties: Map[String, String]) {
  def withProperty(key: String, value: String): KafkaProducerSettings =
    copy(properties = properties.updatedWith(key)(_ => Some(value)))
}

final case class KafkaStreamSettings(properties: Map[String, String]) {
  def withProperty(key: String, value: String): KafkaStreamSettings =
    copy(properties = properties.updatedWith(key)(_ => Some(value)))

  def javaProperties: Properties = utils.toProperties(properties)
}

final case class SchemaRegistrySettings(config: Map[String, String]) {
  def withProperty(key: String, value: String): SchemaRegistrySettings =
    copy(config = config.updatedWith(key)(_ => Some(value)))
}

final case class KafkaSettings private (
  zoneId: ZoneId,
  consumerSettings: KafkaConsumerSettings,
  producerSettings: KafkaProducerSettings,
  adminSettings: AdminClientSettings,
  streamSettings: KafkaStreamSettings,
  schemaRegistrySettings: SchemaRegistrySettings) {

  def withZoneId(zoneId: ZoneId): KafkaSettings =
    copy(zoneId = zoneId)

  def withBrokers(brokers: String): KafkaSettings =
    KafkaSettings(
      zoneId,
      consumerSettings.withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
      producerSettings.withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
      adminSettings.withBootstrapServers(brokers),
      streamSettings.withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
      schemaRegistrySettings
    )

  def withSaslJaas(sasl: String): KafkaSettings =
    KafkaSettings(
      zoneId,
      consumerSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      producerSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      adminSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      streamSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      schemaRegistrySettings
    )

  def withSecurityProtocol(sp: SecurityProtocol): KafkaSettings =
    KafkaSettings(
      zoneId,
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

  def ioContext: KafkaContext[IO]    = new KafkaContext[IO](this)
  def context[F[_]]: KafkaContext[F] = new KafkaContext[F](this)
}

object KafkaSettings extends zoneid {
  implicit val showKafkaSettings: Show[KafkaSettings] = cats.derived.semiauto.show[KafkaSettings]

  def apply(brokers: String, schemaRegistry: String): KafkaSettings =
    KafkaSettings(
      ZoneId.systemDefault(),
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
