package com.github.chenharryhua.nanjin.kafka.config

import cats.Show
import cats.derived.derived
import com.github.chenharryhua.nanjin.kafka.KafkaContext
import fs2.kafka.AdminClientSettings
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol

/** `https://kafka.apache.org/`
  */

sealed trait Settings[A] {
  def properties: Map[String, String]
  def withProperty(key: String, value: String): A
}

final case class KafkaConsumerSettings(properties: Map[String, String])
    extends Settings[KafkaConsumerSettings] {
  override def withProperty(key: String, value: String): KafkaConsumerSettings =
    copy(properties = properties.updated(key, value))

  def withProperty(f: ConsumerConfigKeys => String, value: String): KafkaConsumerSettings =
    withProperty(f(ConsumerConfigKeys), value)
}

final case class KafkaProducerSettings(properties: Map[String, String])
    extends Settings[KafkaProducerSettings] {
  override def withProperty(key: String, value: String): KafkaProducerSettings =
    copy(properties = properties.updated(key, value))

  def withProperty(f: ProducerConfigKeys => String, value: String): KafkaProducerSettings =
    withProperty(f(ProducerConfigKeys), value)
}

final case class KafkaStreamSettings(properties: Map[String, String]) extends Settings[KafkaStreamSettings] {
  override def withProperty(key: String, value: String): KafkaStreamSettings =
    copy(properties = properties.updated(key, value))

  def withProperty(f: StreamsConfigKeys => String, value: String): KafkaStreamSettings =
    withProperty(f(StreamsConfigKeys), value)
}

final case class SerdeSettings(properties: Map[String, String]) extends Settings[SerdeSettings] {
  override def withProperty(key: String, value: String): SerdeSettings =
    copy(properties = properties.updated(key, value))

  def withProperty(f: AbstractKafkaSchemaSerDeConfigKeys => String, value: String): SerdeSettings =
    withProperty(f(AbstractKafkaSchemaSerDeConfigKeys), value)
}

final case class KafkaSettings(
  consumerSettings: KafkaConsumerSettings,
  producerSettings: KafkaProducerSettings,
  adminSettings: AdminClientSettings,
  streamSettings: KafkaStreamSettings,
  serdeSettings: SerdeSettings)
    derives Show {

  def withBrokers(brokers: String): KafkaSettings =
    KafkaSettings(
      consumerSettings.withProperty(_.BOOTSTRAP_SERVERS_CONFIG, brokers),
      producerSettings.withProperty(_.BOOTSTRAP_SERVERS_CONFIG, brokers),
      adminSettings.withBootstrapServers(brokers),
      streamSettings.withProperty(_.BOOTSTRAP_SERVERS_CONFIG, brokers),
      serdeSettings
    )

  def withSaslJaas(sasl: String): KafkaSettings =
    KafkaSettings(
      consumerSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      producerSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      adminSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      streamSettings.withProperty(SaslConfigs.SASL_JAAS_CONFIG, sasl),
      serdeSettings
    )

  def withSecurityProtocol(sp: SecurityProtocol): KafkaSettings =
    KafkaSettings(
      consumerSettings.withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name),
      producerSettings.withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name),
      adminSettings.withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name),
      streamSettings.withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name),
      serdeSettings
    )

  def withProducerProperty(f: ProducerConfigKeys => String, value: String): KafkaSettings =
    copy(producerSettings = producerSettings.withProperty(f, value))

  def withConsumerProperty(f: ConsumerConfigKeys => String, value: String): KafkaSettings =
    copy(consumerSettings = consumerSettings.withProperty(f, value))

  def withStreamingProperty(f: StreamsConfigKeys => String, value: String): KafkaSettings =
    copy(streamSettings = streamSettings.withProperty(f, value))

  def withSerdeProperty(f: AbstractKafkaSchemaSerDeConfigKeys => String, value: String): KafkaSettings =
    copy(serdeSettings = serdeSettings.withProperty(f, value))

  def withAvroSerializerConfig(f: KafkaAvroSerializerConfigKeys => String, value: String): KafkaSettings =
    copy(serdeSettings = serdeSettings.withProperty(f(KafkaAvroSerializerConfigKeys), value))
  def withAvroDeserializerConfig(f: KafkaAvroDeserializerConfigKeys => String, value: String): KafkaSettings =
    copy(serdeSettings = serdeSettings.withProperty(f(KafkaAvroDeserializerConfigKeys), value))

  def withJsonSerializerConfig(
    f: KafkaJsonSchemaSerializerConfigKeys => String,
    value: String): KafkaSettings =
    copy(serdeSettings = serdeSettings.withProperty(f(KafkaJsonSchemaSerializerConfigKeys), value))
  def withJsonDeserializerConfig(
    f: KafkaJsonSchemaDeserializerConfigKeys => String,
    value: String): KafkaSettings =
    copy(serdeSettings = serdeSettings.withProperty(f(KafkaJsonSchemaDeserializerConfigKeys), value))

  def withProtobufSerializerConfig(
    f: KafkaProtobufSerializerConfigKeys => String,
    value: String): KafkaSettings =
    copy(serdeSettings = serdeSettings.withProperty(f(KafkaProtobufSerializerConfigKeys), value))
  def withProtobufDeserializerConfig(
    f: KafkaProtobufDeserializerConfigKeys => String,
    value: String): KafkaSettings =
    copy(serdeSettings = serdeSettings.withProperty(f(KafkaProtobufDeserializerConfigKeys), value))

  def withAdminClient(f: AdminClientSettings => AdminClientSettings): KafkaSettings =
    copy(adminSettings = f(adminSettings))

  def context[F[_]]: KafkaContext[F] = new KafkaContext[F](this)
}

object KafkaSettings {

  def apply(brokers: String, schemaRegistry: String): KafkaSettings =
    KafkaSettings(
      KafkaConsumerSettings(Map.empty),
      KafkaProducerSettings(Map.empty),
      AdminClientSettings(brokers),
      KafkaStreamSettings(Map.empty),
      SerdeSettings(Map.empty)
    ).withBrokers(brokers)
      .withSerdeProperty(_.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry)
      .withSecurityProtocol(SecurityProtocol.PLAINTEXT)

  val local: KafkaSettings = apply("localhost:9092", "http://localhost:8081")
}
