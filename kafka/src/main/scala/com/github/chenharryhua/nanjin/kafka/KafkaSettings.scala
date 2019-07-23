package com.github.chenharryhua.nanjin.kafka

import java.util.Properties

import cats.Show
import monocle.macros.Lenses
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serializer}
import cats.implicits._

@Lenses final case class Fs2Settings(
  consumerProps: Map[String, String],
  producerProps: Map[String, String]
) {
  import fs2.kafka.ConsumerSettings
  import fs2.kafka.ProducerSettings

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
  import akka.kafka.ConsumerSettings
  import akka.kafka.ProducerSettings
  import akka.kafka.CommitterSettings

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
  schemaRegistrySettings: SchemaRegistrySettings,
  sharedAdminSettings: SharedAdminSettings,
  sharedConsumerSettings: SharedConsumerSettings,
  sharedProducerSettings: SharedProducerSettings) {

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
  implicit val showKafkaSettings: Show[KafkaSettings] = _.show
}
