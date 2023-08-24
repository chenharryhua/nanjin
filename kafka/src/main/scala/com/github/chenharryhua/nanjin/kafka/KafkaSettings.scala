package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.effect.IO
import com.github.chenharryhua.nanjin.common.utils
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import monocle.Traversal
import monocle.macros.Lenses
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.streams.StreamsConfig
import org.typelevel.cats.time.instances.zoneid

import java.time.ZoneId
import java.util.Properties

/** [[https://kafka.apache.org/]]
  */

final case class KafkaGroupId(value: String) extends AnyVal
final case class KafkaAppId(value: String) extends AnyVal

@Lenses final case class KafkaConsumerSettings(config: Map[String, String]) {
  def javaProperties: Properties = utils.toProperties(config)
}

@Lenses final case class KafkaProducerSettings(config: Map[String, String])

@Lenses final case class KafkaStreamSettings(config: Map[String, String]) {
  def javaProperties: Properties = utils.toProperties(config)
}
object KafkaStreamSettings {
  implicit val showKafkaStreamSettings: Show[KafkaStreamSettings] =
    cats.derived.semiauto.show[KafkaStreamSettings]
}

@Lenses final case class KafkaAdminSettings(config: Map[String, String])

@Lenses final case class SchemaRegistrySettings(config: Map[String, String])

@Lenses final case class KafkaSettings private (
  zoneId: ZoneId,
  consumerSettings: KafkaConsumerSettings,
  producerSettings: KafkaProducerSettings,
  streamSettings: KafkaStreamSettings,
  adminSettings: KafkaAdminSettings,
  schemaRegistrySettings: SchemaRegistrySettings) {

  val appId: Option[KafkaAppId] =
    streamSettings.config.get(StreamsConfig.APPLICATION_ID_CONFIG).map(KafkaAppId)

  val groupId: Option[KafkaGroupId] =
    consumerSettings.config.get(ConsumerConfig.GROUP_ID_CONFIG).map(KafkaGroupId)

  private def updateAll(key: String, value: String): KafkaSettings =
    Traversal
      .applyN[KafkaSettings, Map[String, String]](
        KafkaSettings.consumerSettings.andThen(KafkaConsumerSettings.config),
        KafkaSettings.producerSettings.andThen(KafkaProducerSettings.config),
        KafkaSettings.streamSettings.andThen(KafkaStreamSettings.config),
        KafkaSettings.adminSettings.andThen(KafkaAdminSettings.config)
      )
      .modify(_.updatedWith(key)(_ => Some(value)))(this)

  def withZoneId(zoneId: ZoneId): KafkaSettings = KafkaSettings.zoneId.replace(zoneId)(this)

  def withBrokers(brokers: String): KafkaSettings =
    updateAll(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
  def withSaslJaas(sasl: String): KafkaSettings = updateAll(SaslConfigs.SASL_JAAS_CONFIG, sasl)
  def withClientID(clientID: String): KafkaSettings =
    updateAll(CommonClientConfigs.CLIENT_ID_CONFIG, clientID)

  def withSecurityProtocol(sp: SecurityProtocol): KafkaSettings =
    updateAll(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name)

  def withSchemaRegistryProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.schemaRegistrySettings
      .andThen(SchemaRegistrySettings.config)
      .modify(_.updatedWith(key)(_ => Some(value)))(this)

  def withProducerProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.producerSettings
      .andThen(KafkaProducerSettings.config)
      .modify(_.updatedWith(key)(_ => Some(value)))(this)

  def withConsumerProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.consumerSettings
      .andThen(KafkaConsumerSettings.config)
      .modify(_.updatedWith(key)(_ => Some(value)))(this)

  def withStreamingProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.streamSettings
      .andThen(KafkaStreamSettings.config)
      .modify(_.updatedWith(key)(_ => Some(value)))(this)

  private def auto_offset_reset(value: String): KafkaSettings =
    Traversal
      .applyN[KafkaSettings, Map[String, String]](
        KafkaSettings.consumerSettings.andThen(KafkaConsumerSettings.config),
        KafkaSettings.streamSettings.andThen(KafkaStreamSettings.config))
      .modify(_.updatedWith(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)(_ => Some(value)))(this)

  def withLatestAutoOffset: KafkaSettings   = auto_offset_reset("latest")
  def withEarliestAutoOffset: KafkaSettings = auto_offset_reset("earliest")

  def withGroupId(groupId: String): KafkaSettings =
    withConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

  def withApplicationId(appId: String): KafkaSettings =
    withStreamingProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId)

  def ioContext: KafkaContext[IO]    = new KafkaContext[IO](this)
  def context[F[_]]: KafkaContext[F] = new KafkaContext[F](this)
}

object KafkaSettings extends zoneid {
  implicit val showKafkaSettings: Show[KafkaSettings] = cats.derived.semiauto.show[KafkaSettings]

  val empty: KafkaSettings = KafkaSettings(
    ZoneId.systemDefault(),
    KafkaConsumerSettings(Map.empty),
    KafkaProducerSettings(Map.empty),
    KafkaStreamSettings(Map.empty),
    KafkaAdminSettings(Map.empty),
    SchemaRegistrySettings(Map.empty)
  )

  def apply(brokers: String, schemaRegistry: String): KafkaSettings =
    empty
      .withBrokers(brokers)
      .withSchemaRegistryProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry)
      .withSecurityProtocol(SecurityProtocol.PLAINTEXT)

  val local: KafkaSettings = apply("localhost:9092", "http://localhost:8081")
}
