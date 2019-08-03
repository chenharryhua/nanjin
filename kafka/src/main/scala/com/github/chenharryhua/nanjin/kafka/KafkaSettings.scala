package com.github.chenharryhua.nanjin.kafka

import java.util.Properties

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import cats.{Eval, Show}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import monocle.Traversal
import monocle.function.At.at
import monocle.macros.Lenses
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serializer}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler

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

  val settings: Properties = utils.toProperties(props)

  def show: String =
    s"""
       |kafka streaming settings:
       |${props.show}
     """.stripMargin
}

@Lenses final case class SharedProducerSettings(props: Map[String, String]) {

  val settings: Properties = utils.toProperties(props)

  def show: String =
    s"""
       |kafka shared producer settings:
       |${props.show}
     """.stripMargin
}

@Lenses final case class SharedConsumerSettings(props: Map[String, String]) {

  val settings: Properties = utils.toProperties(props)

  def show: String =
    s"""
       |shared consumer settings:
       |${props.show}
     """.stripMargin
}

@Lenses final case class SharedAdminSettings(props: Map[String, String]) {

  val settings: Properties = utils.toProperties(props)

  def show: String =
    s"""
       |shared admin settings:
       |${props.show}
     """.stripMargin
}

@Lenses final case class SchemaRegistrySettings(props: Map[String, String]) {

  def show: String =
    s"""
       |schema registry settings:
       |${props.show}
     """.stripMargin

  private[this] val srTag: String = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG

  val csrClient: Eval[CachedSchemaRegistryClient] =
    Eval.later(props.get(srTag) match {
      case None =>
        sys.error(s"$srTag was not configured")
      case Some(url) => new CachedSchemaRegistryClient(url, 500)
    })
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

  def setBrokers(bs: String): KafkaSettings =
    updateAll(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs)

  def setSaslJaas(sj: String): KafkaSettings =
    updateAll(SaslConfigs.SASL_JAAS_CONFIG, sj)

  def setSecurityProtocol(sp: SecurityProtocol): KafkaSettings =
    updateAll(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name)

  def setSchemaRegistryProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.schemaRegistrySettings
      .composeLens(SchemaRegistrySettings.props)
      .composeLens(at(key))
      .set(Some(value))(this)

  def setSchemaRegistryUrl(url: String): KafkaSettings =
    setSchemaRegistryProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url)

  def setProducerProperty(key: String, value: String): KafkaSettings =
    Traversal
      .applyN[KafkaSettings, Map[String, String]](
        KafkaSettings.fs2Settings.composeLens(Fs2Settings.producerProps),
        KafkaSettings.akkaSettings.composeLens(AkkaSettings.producerProps))
      .composeLens(at(key))
      .set(Some(value))(this)

  def setConsumerProperty(key: String, value: String): KafkaSettings =
    Traversal
      .applyN[KafkaSettings, Map[String, String]](
        KafkaSettings.fs2Settings.composeLens(Fs2Settings.consumerProps),
        KafkaSettings.akkaSettings.composeLens(AkkaSettings.consumerProps))
      .composeLens(at(key))
      .set(Some(value))(this)

  def setStreamingProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.streamSettings
      .composeLens(KafkaStreamSettings.props)
      .composeLens(at(key))
      .set(Some(value))(this)

  def setGroupId(gid: String): KafkaSettings =
    setConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, gid)

  def setApplicationId(appId: String): KafkaSettings =
    setStreamingProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId)

  def ioContext(implicit contextShift: ContextShift[IO], timer: Timer[IO]): IoKafkaContext =
    new IoKafkaContext(this)

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
    SchemaRegistrySettings(Map.empty)
  )

  val local: KafkaSettings = {
    val s = KafkaSettings(
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
      SharedAdminSettings(
        Map(AdminClientConfig.CLIENT_ID_CONFIG -> s"shared-admin-${utils.random4d.value}")),
      SharedConsumerSettings(
        Map(ConsumerConfig.CLIENT_ID_CONFIG -> s"shared-consumer-${utils.random4d.value}")),
      SharedProducerSettings(
        Map(ProducerConfig.CLIENT_ID_CONFIG -> s"shared-producer-${utils.random4d.value}")),
      SchemaRegistrySettings(
        Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG ->
          "http://localhost:8081"))
    )
    s.setGroupId("nanjin-group")
      .setApplicationId("nanjin-app")
      .setBrokers("localhost:9092")
      .setSecurityProtocol(SecurityProtocol.PLAINTEXT)
  }
  implicit val showKafkaSettings: Show[KafkaSettings] = _.show
}
