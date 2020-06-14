package com.github.chenharryhua.nanjin.kafka

import java.util.Properties

import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.utils
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import monocle.Traversal
import monocle.function.At.at
import monocle.macros.Lenses
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.streams.StreamsConfig

final case class KafkaGroupId(value: String) extends AnyVal
final case class KafkaAppId(value: String) extends AnyVal

@Lenses final case class KafkaConsumerSettings(config: Map[String, String]) {
  def javaProperties: Properties = utils.toProperties(config)
}

@Lenses final case class KafkaProducerSettings(config: Map[String, String]) {
  def javaProperties: Properties = utils.toProperties(config)
}

@Lenses final case class KafkaStreamSettings(config: Map[String, String]) {
  def javaProperties: Properties = utils.toProperties(config)
}

@Lenses final case class KafkaAdminSettings(config: Map[String, String]) {
  def javaProperties: Properties = utils.toProperties(config)
}

@Lenses final case class SchemaRegistrySettings(config: Map[String, String]) {
  def javaProperties: Properties = utils.toProperties(config)
}

@Lenses final case class KafkaSettings private (
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
        KafkaSettings.consumerSettings.composeLens(KafkaConsumerSettings.config),
        KafkaSettings.producerSettings.composeLens(KafkaProducerSettings.config),
        KafkaSettings.streamSettings.composeLens(KafkaStreamSettings.config),
        KafkaSettings.adminSettings.composeLens(KafkaAdminSettings.config)
      )
      .composeLens(at(key))
      .set(Some(value))(this)

  def withBrokers(brokers: String): KafkaSettings =
    updateAll(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)

  def withSaslJaas(sasl: String): KafkaSettings =
    updateAll(SaslConfigs.SASL_JAAS_CONFIG, sasl)

  def withSecurityProtocol(sp: SecurityProtocol): KafkaSettings =
    updateAll(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name)

  def withSchemaRegistryProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.schemaRegistrySettings
      .composeLens(SchemaRegistrySettings.config)
      .composeLens(at(key))
      .set(Some(value))(this)

  def withProducerProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.producerSettings
      .composeLens(KafkaProducerSettings.config)
      .composeLens(at(key))
      .set(Some(value))(this)

  def withConsumerProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.consumerSettings
      .composeLens(KafkaConsumerSettings.config)
      .composeLens(at(key))
      .set(Some(value))(this)

  def withStreamingProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.streamSettings
      .composeLens(KafkaStreamSettings.config)
      .composeLens(at(key))
      .set(Some(value))(this)

  def withGroupId(groupId: String): KafkaSettings =
    withConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)

  def withApplicationId(appId: String): KafkaSettings =
    withStreamingProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId)

  def ioContext(implicit contextShift: ContextShift[IO], timer: Timer[IO]): IoKafkaContext =
    new IoKafkaContext(this)

  def zioContext(implicit
    contextShift: ContextShift[zio.Task],
    timer: Timer[zio.Task],
    ce: ConcurrentEffect[zio.Task]) =
    new ZioKafkaContext(this)

  def monixContext(implicit
    contextShift: ContextShift[monix.eval.Task],
    timer: Timer[monix.eval.Task],
    ce: ConcurrentEffect[monix.eval.Task]) =
    new MonixKafkaContext(this)
}

object KafkaSettings {

  val empty: KafkaSettings = KafkaSettings(
    KafkaConsumerSettings(Map.empty),
    KafkaProducerSettings(Map.empty),
    KafkaStreamSettings(Map.empty),
    KafkaAdminSettings(Map.empty),
    SchemaRegistrySettings(Map.empty)
  )

  def apply(brokers: String, schemaRegistry: String): KafkaSettings =
    empty
      .withBrokers(brokers)
      .withSchemaRegistryProperty(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistry)
      .withConsumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")
      .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withGroupId(s"nanjin.group.id-${utils.random4d.value}")
      .withApplicationId(s"nanjin.app.id-${utils.random4d.value}")
      .withSecurityProtocol(SecurityProtocol.PLAINTEXT)

  val local: KafkaSettings = apply("localhost:9092", "http://localhost:8081")
}
