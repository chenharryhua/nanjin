package com.github.chenharryhua.nanjin.kafka

import java.util.Properties

import cats.Show
import cats.implicits._
import monocle.Traversal
import monocle.function.At.at
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serializer}

@Lenses final case class Fs2Settings(
  consumerProps: Map[String, String],
  producerProps: Map[String, String]
) {
  import fs2.kafka.{ConsumerSettings, ProducerSettings}

  def consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]] =
    ConsumerSettings[Array[Byte], Array[Byte]](new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withProperties(consumerProps)

  def producerSettings[K, V](kser: Serializer[K], vser: Serializer[V]): ProducerSettings[K, V] =
    ProducerSettings[K, V](kser, vser).withProperties(producerProps)

  def show: String =
    s"""
       |fs2 kafka settings:
       |consumerPros: ${consumerProps.show}
       |producerPros: ${producerProps.show}
     """.stripMargin
}

@Lenses final case class AkkaSettings(
  consumerProps: Map[String, String],
  producerProps: Map[String, String]
) {
  import akka.actor.ActorSystem
  import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings}

  def consumerSettings(system: ActorSystem): ConsumerSettings[Array[Byte], Array[Byte]] =
    akka.kafka
      .ConsumerSettings[Array[Byte], Array[Byte]](
        system,
        new ByteArrayDeserializer,
        new ByteArrayDeserializer)
      .withProperties(consumerProps)

  def producerSettings[K, V](
    system: ActorSystem,
    kser: Serializer[K],
    vser: Serializer[V]): ProducerSettings[K, V] =
    ProducerSettings[K, V](system, kser, vser).withProperties(producerProps)

  def committerSettings(system: ActorSystem): CommitterSettings =
    CommitterSettings(system)

  def show: String =
    s"""
       |akka kafka settings:
       |consumerPros: ${consumerProps.show}
       |producerPros: ${producerProps.show}
     """.stripMargin
}

@Lenses final case class KafkaStreamSettings(settings: Map[String, String]) {

  val props: Properties = (new Properties() /: settings) { case (a, (k, v)) => a.put(k, v); a }

  def show: String =
    s"""
       |kafka streaming settings:
       |${settings.show}
     """.stripMargin
}

@Lenses final case class SharedProducerSettings(settings: Map[String, String]) {

  val props: Properties = (new Properties() /: settings) { case (a, (k, v)) => a.put(k, v); a }

  def show: String =
    s"""
       |kafka shared producer settings:
       |${settings.show}
     """.stripMargin
}

@Lenses final case class SharedConsumerSettings(settings: Map[String, String]) {

  val props: Properties = (new Properties() /: settings) { case (a, (k, v)) => a.put(k, v); a }

  def show: String =
    s"""
       |shared consumer settings:
       |${settings.show}
     """.stripMargin
}

@Lenses final case class SharedAdminSettings(settings: Map[String, String]) {

  val props: Properties = (new Properties() /: settings) { case (a, (k, v)) => a.put(k, v); a }

  def show: String =
    s"""
       |shared admin settings:
       |${settings.show}
     """.stripMargin
}

@Lenses final case class SchemaRegistrySettings(url: String, cacheSize: Int) {

  def show: String =
    s"""
       |schema registry settings:
       |url: $url
       |cache size: $cacheSize
     """.stripMargin
}

@Lenses final case class KafkaSettings(
  fs2Settings: Fs2Settings,
  akkaSettings: AkkaSettings,
  streamSettings: KafkaStreamSettings,
  sharedAdminSettings: SharedAdminSettings,
  sharedConsumerSettings: SharedConsumerSettings,
  sharedProducerSettings: SharedProducerSettings,
  schemaRegistrySettings: SchemaRegistrySettings) {

  def schemaRegistry(url: String, cacheSize: Int): KafkaSettings =
    copy(schemaRegistrySettings = SchemaRegistrySettings(url, cacheSize))

  private def updateAll(key: String, value: String): KafkaSettings = {
    Traversal
      .applyN[KafkaSettings, Map[String, String]](
        KafkaSettings.fs2Settings.composeLens(Fs2Settings.consumerProps),
        KafkaSettings.fs2Settings.composeLens(Fs2Settings.producerProps),
        KafkaSettings.akkaSettings.composeLens(AkkaSettings.consumerProps),
        KafkaSettings.akkaSettings.composeLens(AkkaSettings.producerProps),
        KafkaSettings.streamSettings.composeLens(KafkaStreamSettings.settings),
        KafkaSettings.sharedAdminSettings.composeLens(SharedAdminSettings.settings),
        KafkaSettings.sharedConsumerSettings.composeLens(SharedConsumerSettings.settings),
        KafkaSettings.sharedProducerSettings.composeLens(SharedProducerSettings.settings)
      )
      .composeLens(at(key))
      .set(Some(value))(this)
  }

  def brokers(brokers: String): KafkaSettings =
    updateAll(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)

  def show: String =
    s"""
       |kafka settings:
       |${fs2Settings.show}
       |${akkaSettings.show}
       |${streamSettings.show}
       |${schemaRegistrySettings.show}
       |${sharedAdminSettings.show}
       |${sharedConsumerSettings.show}
       |${sharedProducerSettings.show}
  """.stripMargin
}

object KafkaSettings {

  val empty: KafkaSettings = KafkaSettings(
    Fs2Settings(Map(), Map()),
    AkkaSettings(Map(), Map()),
    KafkaStreamSettings(Map()),
    SharedAdminSettings(Map()),
    SharedConsumerSettings(Map()),
    SharedProducerSettings(Map()),
    SchemaRegistrySettings("", 0)
  )

  implicit val showKafkaSettings: Show[KafkaSettings] = _.show
}
