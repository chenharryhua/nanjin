package com.github.chenharryhua.nanjin.kafka

import java.util.Properties

import cats.Show
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.implicits._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import monocle.Traversal
import monocle.function.At.at
import monocle.macros.Lenses
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serializer}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler

import scala.collection.JavaConverters._

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
    ConsumerSettings[Array[Byte], Array[Byte]](
      system,
      new ByteArrayDeserializer,
      new ByteArrayDeserializer).withProperties(consumerProps)

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

@Lenses final case class KafkaStreamSettings(props: Map[String, String]) {

  val settings: Properties = (new Properties() /: props) { case (a, (k, v)) => a.put(k, v); a }

  def show: String =
    s"""
       |kafka streaming settings:
       |${props.show}
     """.stripMargin
}

@Lenses final case class SharedProducerSettings(props: Map[String, String]) {

  val settings: Properties = (new Properties() /: props) { case (a, (k, v)) => a.put(k, v); a }

  def show: String =
    s"""
       |kafka shared producer settings:
       |${props.show}
     """.stripMargin
}

@Lenses final case class SharedConsumerSettings(props: Map[String, String]) {

  val settings: Properties = (new Properties() /: props) { case (a, (k, v)) => a.put(k, v); a }

  def show: String =
    s"""
       |shared consumer settings:
       |${props.show}
     """.stripMargin
}

@Lenses final case class SharedAdminSettings(props: Map[String, String]) {

  val settings: Properties = (new Properties() /: props) { case (a, (k, v)) => a.put(k, v); a }

  def show: String =
    s"""
       |shared admin settings:
       |${props.show}
     """.stripMargin
}

@Lenses final case class SchemaRegistrySettings(
  baseUrls: List[String],
  identityMapCapacity: Int,
  originals: Map[String, String],
  httpHeaders: Map[String, String]) {

  def this(url: String) = this(List(url), 500, Map.empty, Map.empty)
  def this()            = this("")

  def url(newUrl: String): SchemaRegistrySettings = copy(baseUrls = List(newUrl))

  def show: String =
    s"""
       |schema registry settings:
       |baseUrls:    ${baseUrls.show}
       |cache size:  $identityMapCapacity
       |originals:   ${originals.show}
       |httpHeaders: ${httpHeaders.show}
     """.stripMargin

  lazy val csrClient: CachedSchemaRegistryClient =
    new CachedSchemaRegistryClient(
      baseUrls.asJava,
      identityMapCapacity,
      originals.asJava,
      httpHeaders.asJava)
}

@Lenses final case class KafkaSettings(
  fs2Settings: Fs2Settings,
  akkaSettings: AkkaSettings,
  streamSettings: KafkaStreamSettings,
  sharedAdminSettings: SharedAdminSettings,
  sharedConsumerSettings: SharedConsumerSettings,
  sharedProducerSettings: SharedProducerSettings,
  schemaRegistrySettings: SchemaRegistrySettings) {

  private def updateAll(key: String, value: String): KafkaSettings = {
    Traversal
      .applyN[KafkaSettings, Map[String, String]](
        KafkaSettings.fs2Settings.composeLens(Fs2Settings.consumerProps),
        KafkaSettings.fs2Settings.composeLens(Fs2Settings.producerProps),
        KafkaSettings.akkaSettings.composeLens(AkkaSettings.consumerProps),
        KafkaSettings.akkaSettings.composeLens(AkkaSettings.producerProps),
        KafkaSettings.streamSettings.composeLens(KafkaStreamSettings.props),
        KafkaSettings.sharedAdminSettings.composeLens(SharedAdminSettings.props),
        KafkaSettings.sharedConsumerSettings.composeLens(SharedConsumerSettings.props),
        KafkaSettings.sharedProducerSettings.composeLens(SharedProducerSettings.props)
      )
      .composeLens(at(key))
      .set(Some(value))(this)
  }

  def brokers(bs: String): KafkaSettings =
    updateAll(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs)

  def saslJaas(sj: String): KafkaSettings =
    updateAll(SaslConfigs.SASL_JAAS_CONFIG, sj)

  def securityProtocol(sp: SecurityProtocol): KafkaSettings =
    updateAll(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name)

  def schemaRegistryUrl(newUrl: String): KafkaSettings =
    KafkaSettings.schemaRegistrySettings
      .composeLens(SchemaRegistrySettings.baseUrls)
      .set(List(newUrl))(this)

  def producerProperties(key: String, value: String): KafkaSettings =
    Traversal
      .applyN[KafkaSettings, Map[String, String]](
        KafkaSettings.fs2Settings.composeLens(Fs2Settings.producerProps),
        KafkaSettings.akkaSettings.composeLens(AkkaSettings.producerProps))
      .composeLens(at(key))
      .set(Some(value))(this)

  def consumerProperties(key: String, value: String): KafkaSettings =
    Traversal
      .applyN[KafkaSettings, Map[String, String]](
        KafkaSettings.fs2Settings.composeLens(Fs2Settings.consumerProps),
        KafkaSettings.akkaSettings.composeLens(AkkaSettings.consumerProps))
      .composeLens(at(key))
      .set(Some(value))(this)

  def streamProperties(key: String, value: String): KafkaSettings =
    KafkaSettings.streamSettings
      .composeLens(KafkaStreamSettings.props)
      .composeLens(at(key))
      .set(Some(value))(this)

  def groupId(gid: String): KafkaSettings =
    consumerProperties(ConsumerConfig.GROUP_ID_CONFIG, gid)

  def applicationId(appId: String): KafkaSettings =
    streamProperties(StreamsConfig.APPLICATION_ID_CONFIG, appId)

  def context[F[_]: ContextShift: Timer: ConcurrentEffect]: KafkaContext[F] =
    new KafkaContext[F](this)

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
    Fs2Settings(Map.empty, Map.empty),
    AkkaSettings(Map.empty, Map.empty),
    KafkaStreamSettings(Map.empty),
    SharedAdminSettings(Map.empty),
    SharedConsumerSettings(Map.empty),
    SharedProducerSettings(Map.empty),
    new SchemaRegistrySettings()
  )

  val predefined: KafkaSettings = KafkaSettings(
    Fs2Settings(
      Map(
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "500",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
      ),
      Map.empty
    ),
    AkkaSettings(
      Map(
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "100",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
      ),
      Map.empty
    ),
    KafkaStreamSettings(
      Map(
        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG ->
          classOf[LogAndContinueExceptionHandler].getName,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG -> "3"
      )),
    SharedAdminSettings(Map.empty),
    SharedConsumerSettings(Map.empty),
    SharedProducerSettings(Map.empty),
    new SchemaRegistrySettings("http://localhost:8081")
  )
  implicit val showKafkaSettings: Show[KafkaSettings] = _.show
}
