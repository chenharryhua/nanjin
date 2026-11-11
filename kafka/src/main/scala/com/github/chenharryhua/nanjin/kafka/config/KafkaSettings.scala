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

/** Substrings marking a property key whose value is a secret. Matched case-insensitively so the whole family
  * is covered: `sasl.jaas.config`, `ssl.*.password`, `basic.auth.user.info`, OAuth client secrets and tokens.
  */
private val SECRET_MARKERS: List[String] =
  List("password", "secret", "jaas", "user.info", "token", "credential")

private val REDACTED: String = "<redacted>"

/** Render properties in key order with secret values replaced, so a rendered settings value names every
  * property that is set without disclosing the sensitive ones.
  */
private def show_properties(properties: Map[String, String]): String =
  properties.toList
    .sortBy(_._1)
    .map { case (key, value) =>
      val shown = if (SECRET_MARKERS.exists(key.toLowerCase.contains)) REDACTED else value
      s"$key = $shown"
    }
    .mkString("{", ", ", "}")

final case class KafkaConsumerSettings(properties: Map[String, String])
    extends Settings[KafkaConsumerSettings] {
  override def withProperty(key: String, value: String): KafkaConsumerSettings =
    copy(properties = properties.updated(key, value))

  def withProperty(f: ConsumerConfigKeys => String, value: String): KafkaConsumerSettings =
    withProperty(f(ConsumerConfigKeys), value)
}
object KafkaConsumerSettings {
  given Show[KafkaConsumerSettings] = s => s"KafkaConsumerSettings(${show_properties(s.properties)})"
}

final case class KafkaProducerSettings(properties: Map[String, String])
    extends Settings[KafkaProducerSettings] {
  override def withProperty(key: String, value: String): KafkaProducerSettings =
    copy(properties = properties.updated(key, value))

  def withProperty(f: ProducerConfigKeys => String, value: String): KafkaProducerSettings =
    withProperty(f(ProducerConfigKeys), value)
}
object KafkaProducerSettings {
  given Show[KafkaProducerSettings] = s => s"KafkaProducerSettings(${show_properties(s.properties)})"
}

final case class KafkaStreamSettings(properties: Map[String, String]) extends Settings[KafkaStreamSettings] {
  override def withProperty(key: String, value: String): KafkaStreamSettings =
    copy(properties = properties.updated(key, value))

  def withProperty(f: StreamsConfigKeys => String, value: String): KafkaStreamSettings =
    withProperty(f(StreamsConfigKeys), value)
}
object KafkaStreamSettings {
  given Show[KafkaStreamSettings] = s => s"KafkaStreamSettings(${show_properties(s.properties)})"
}

final case class SerdeSettings(properties: Map[String, String]) extends Settings[SerdeSettings] {
  override def withProperty(key: String, value: String): SerdeSettings =
    copy(properties = properties.updated(key, value))

  def withProperty(f: AbstractKafkaSchemaSerDeConfigKeys => String, value: String): SerdeSettings =
    withProperty(f(AbstractKafkaSchemaSerDeConfigKeys), value)
}
object SerdeSettings {
  given Show[SerdeSettings] = s => s"SerdeSettings(${show_properties(s.properties)})"
}

/** Properties for every Kafka client family a `KafkaContext` builds: consumer, producer, admin, Kafka
  * Streams, and the Confluent Serdes.
  *
  * Transformations are immutable; `withBrokers`, `withSaslJaas`, and `withSecurityProtocol` update the
  * consumer, producer, admin, and Streams settings together, while the Serde properties are addressed on
  * their own.
  *
  * The property maps carry credentials verbatim, since that is what the Kafka clients are handed. `Show`
  * therefore redacts the value of any property whose key looks sensitive (a JAAS config, a password, a
  * `user.info`, a token, a secret, a credential) while still naming the key, so a rendered settings value is
  * safe to log. Reading `properties` yourself bypasses that, so do not print the maps directly.
  */
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

  /** Set `sasl.jaas.config` on the consumer, producer, admin, and Streams settings.
    *
    * `sasl` holds credentials, so it belongs in an environment variable or a secret store rather than in
    * source. `Show` redacts it, but the value stays readable through `properties`.
    */
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

  def context[F[_]]: KafkaContext[F] = KafkaContext[F](this)
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
