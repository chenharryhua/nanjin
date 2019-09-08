package com.github.chenharryhua.nanjin.kafka

import java.util.Properties

import akka.actor.ActorSystem
import akka.kafka.{
  CommitterSettings => AkkaCommitterSettings,
  ConsumerSettings  => AkkaConsumerSettings,
  ProducerSettings  => AkkaProducerSettings
}
import cats.effect.{ConcurrentEffect, ContextShift, IO, Sync, Timer}
import cats.implicits._
import cats.{Eval, Show}
import com.github.chenharryhua.nanjin.codec.utils
import fs2.kafka.{
  ConsumerSettings => Fs2ConsumerSettings,
  Deserializer     => Fs2Deserializer,
  ProducerSettings => Fs2ProducerSettings,
  Serializer       => Fs2Serializer
}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
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

import scala.util.Try

@Lenses final case class KafkaConsumerSettings(props: Map[String, String]) {

  def fs2ConsumerSettings[F[_]: Sync]: Fs2ConsumerSettings[F, Array[Byte], Array[Byte]] =
    Fs2ConsumerSettings[F, Array[Byte], Array[Byte]](
      Fs2Deserializer.delegate(new ByteArrayDeserializer),
      Fs2Deserializer.delegate(new ByteArrayDeserializer)).withProperties(props)

  def akkaConsumerSettings(system: ActorSystem): AkkaConsumerSettings[Array[Byte], Array[Byte]] =
    AkkaConsumerSettings[Array[Byte], Array[Byte]](
      system,
      new ByteArrayDeserializer,
      new ByteArrayDeserializer).withProperties(props)

  def akkaCommitterSettings(system: ActorSystem): AkkaCommitterSettings =
    AkkaCommitterSettings(system)

  val sharedConsumerSettings: Properties = utils.toProperties(
    props ++ Map(ConsumerConfig.CLIENT_ID_CONFIG -> s"shared-consumer-${utils.random4d.value}"))

  def show: String =
    s"""
       |consumer settings:
       |${props.show}
       |""".stripMargin
}

@Lenses final case class KafkaProducerSettings(props: Map[String, String]) {

  def fs2ProducerSettings[F[_]: Sync, K, V](
    kser: Serializer[K],
    vser: Serializer[V]): Fs2ProducerSettings[F, K, V] =
    Fs2ProducerSettings[F, K, V](Fs2Serializer.delegate(kser), Fs2Serializer.delegate(vser))
      .withProperties(props)

  def akkaProducerSettings[K, V](
    system: ActorSystem,
    kser: Serializer[K],
    vser: Serializer[V]): AkkaProducerSettings[K, V] =
    AkkaProducerSettings[K, V](system, kser, vser).withProperties(props)

  val sharedProducerSettings: Properties = utils.toProperties(
    props ++ Map(ConsumerConfig.CLIENT_ID_CONFIG -> s"shared-producer-${utils.random4d.value}"))

  def show: String =
    s"""
       |producer settings:
       |${props.show}
       |""".stripMargin

}

@Lenses final case class KafkaStreamSettings(props: Map[String, String]) {

  val settings: Properties = utils.toProperties(props)

  def show: String =
    s"""
       |kafka streaming settings:
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
      case None => sys.error(s"$srTag was mandatory but not configured")
      case Some(url) =>
        val size: Int = props
          .get(AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DOC)
          .flatMap(n => Try(n.toInt).toOption)
          .getOrElse(AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)
        new CachedSchemaRegistryClient(url, size)
    })
}

@Lenses final case class KafkaSettings(
  consumerSettings: KafkaConsumerSettings,
  producerSettings: KafkaProducerSettings,
  streamSettings: KafkaStreamSettings,
  sharedAdminSettings: SharedAdminSettings,
  schemaRegistrySettings: SchemaRegistrySettings) {

  private def updateAll(key: String, value: String): KafkaSettings =
    Traversal
      .applyN[KafkaSettings, Map[String, String]](
        KafkaSettings.consumerSettings.composeLens(KafkaConsumerSettings.props),
        KafkaSettings.producerSettings.composeLens(KafkaProducerSettings.props),
        KafkaSettings.streamSettings.composeLens(KafkaStreamSettings.props),
        KafkaSettings.sharedAdminSettings.composeLens(SharedAdminSettings.props)
      )
      .composeLens(at(key))
      .set(Some(value))(this)

  def withBrokers(bs: String): KafkaSettings =
    updateAll(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs)

  def withSaslJaas(sj: String): KafkaSettings =
    updateAll(SaslConfigs.SASL_JAAS_CONFIG, sj)

  def withSecurityProtocol(sp: SecurityProtocol): KafkaSettings =
    updateAll(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name)

  def withSchemaRegistryProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.schemaRegistrySettings
      .composeLens(SchemaRegistrySettings.props)
      .composeLens(at(key))
      .set(Some(value))(this)

  def withSchemaRegistryUrl(url: String): KafkaSettings =
    withSchemaRegistryProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url)

  def withProducerProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.producerSettings
      .composeLens(KafkaProducerSettings.props)
      .composeLens(at(key))
      .set(Some(value))(this)

  def withConsumerProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.consumerSettings
      .composeLens(KafkaConsumerSettings.props)
      .composeLens(at(key))
      .set(Some(value))(this)

  def withStreamingProperty(key: String, value: String): KafkaSettings =
    KafkaSettings.streamSettings
      .composeLens(KafkaStreamSettings.props)
      .composeLens(at(key))
      .set(Some(value))(this)

  def withGroupId(gid: String): KafkaSettings =
    withConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, gid)

  def withApplicationId(appId: String): KafkaSettings =
    withStreamingProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId)

  def ioContext(implicit contextShift: ContextShift[IO], timer: Timer[IO]): IoKafkaContext =
    new IoKafkaContext(this)

  def zioContext(
    implicit contextShift: ContextShift[zio.Task],
    timer: Timer[zio.Task],
    ce: ConcurrentEffect[zio.Task]) = new ZioKafkaContext(this)

  def show: String =
    s"""
       |kafka settings:
       |${consumerSettings.show}
       |${producerSettings.show}
       |${streamSettings.show}
       |${schemaRegistrySettings.show}
       |${sharedAdminSettings.show}
  """.stripMargin
}

object KafkaSettings {

  val empty: KafkaSettings = KafkaSettings(
    KafkaConsumerSettings(Map.empty),
    KafkaProducerSettings(Map.empty),
    KafkaStreamSettings(Map.empty),
    SharedAdminSettings(Map.empty),
    SchemaRegistrySettings(Map.empty)
  )

  val local: KafkaSettings =
    KafkaSettings(
      KafkaConsumerSettings(
        Map(
          ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "100",
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
        )
      ),
      KafkaProducerSettings(Map.empty),
      KafkaStreamSettings(Map(
        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG ->
          classOf[LogAndContinueExceptionHandler].getName,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG -> "3"
      )),
      SharedAdminSettings(Map.empty),
      SchemaRegistrySettings(Map.empty)
    ).withGroupId("nanjin-group")
      .withApplicationId("nanjin-app")
      .withBrokers("localhost:9092")
      .withSchemaRegistryUrl("http://localhost:8081")
      .withSecurityProtocol(SecurityProtocol.PLAINTEXT)

  implicit val showKafkaSettings: Show[KafkaSettings] = _.show
}
