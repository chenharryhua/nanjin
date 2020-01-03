package com.github.chenharryhua.nanjin.kafka

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.api._
import com.github.chenharryhua.nanjin.kafka.codec.{
  KafkaCodec,
  KafkaGenericDecoder,
  KafkaSerde,
  NJConsumerMessage
}
import com.sksamuel.avro4s.Record
import io.circe.{Error, Json}
import monocle.function.At
import org.apache.avro.Schema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}
import cats.Show
import akka.actor.ActorSystem

final class TopicCodec[K, V] private[kafka] (
  val keyCodec: KafkaCodec.Key[K],
  val valueCodec: KafkaCodec.Value[V])
    extends Serializable {
  require(
    keyCodec.topicName === valueCodec.topicName,
    "key and value codec should have same topic name")
  val keySerde: KafkaSerde.Key[K]        = keyCodec.serde
  val valueSerde: KafkaSerde.Value[V]    = valueCodec.serde
  val keySchema: Schema                  = keySerde.schema
  val valueSchema: Schema                = valueSerde.schema
  val keySerializer: Serializer[K]       = keySerde.serializer
  val keyDeserializer: Deserializer[K]   = keySerde.deserializer
  val valueSerializer: Serializer[V]     = valueSerde.serializer
  val valueDeserializer: Deserializer[V] = valueSerde.deserializer
}

final class KafkaTopic[K, V] private[kafka] (
  val topicDef: TopicDef[K, V],
  val settings: KafkaSettings)
    extends TopicNameExtractor[K, V] with Serializable {
  import topicDef.{serdeOfKey, serdeOfValue, showKey, showValue}

  val topicName: String = topicDef.topicName

  val consumerGroupId: Option[KafkaConsumerGroupId] =
    KafkaConsumerSettings.config
      .composeLens(At.at(ConsumerConfig.GROUP_ID_CONFIG))
      .get(settings.consumerSettings)
      .map(KafkaConsumerGroupId)

  override def extract(key: K, value: V, rc: RecordContext): String = topicDef.topicName

  @transient lazy val codec: TopicCodec[K, V] = new TopicCodec(
    serdeOfKey.asKey(settings.schemaRegistrySettings.config).codec(topicDef.topicName),
    serdeOfValue.asValue(settings.schemaRegistrySettings.config).codec(topicDef.topicName)
  )

  def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valueCodec)

  def toAvro[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): Record =
    topicDef.toAvro(decoder(cr).record)

  def fromAvro(cr: Record): NJConsumerRecord[K, V] = topicDef.fromAvro(cr)

  def toJson[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): Json =
    topicDef.toJson(decoder(cr).record)

  def fromJsonStr(jsonString: String): Either[Error, NJConsumerRecord[K, V]] =
    topicDef.fromJson(jsonString)

  //channels
  def fs2Channel[F[_]: ConcurrentEffect: ContextShift: Timer]: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      topicDef.topicName,
      settings.producerSettings.fs2ProducerSettings(codec.keySerializer, codec.valueSerializer),
      settings.consumerSettings.fs2ConsumerSettings)

  def akkaResource[F[_]: ConcurrentEffect: ContextShift](
    akkaSystem: ActorSystem): Resource[F, KafkaChannels.AkkaChannel[F, K, V]] =
    Resource.make(
      ConcurrentEffect[F].delay(
        new KafkaChannels.AkkaChannel[F, K, V](
          topicDef.topicName,
          settings.producerSettings
            .akkaProducerSettings(akkaSystem, codec.keySerializer, codec.valueSerializer),
          settings.consumerSettings.akkaConsumerSettings(akkaSystem),
          settings.consumerSettings.akkaCommitterSettings(akkaSystem))))(_ =>
      ConcurrentEffect[F].unit)

  def kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](topicDef.topicName, codec.keySerde, codec.valueSerde)

  // APIs
  def schemaRegistry[F[_]: Sync]: KafkaSchemaRegistryApi[F] = api.KafkaSchemaRegistryApi[F](this)

  def admin[F[_]: Concurrent: ContextShift]: KafkaTopicAdminApi[F] =
    api.KafkaTopicAdminApi[F, K, V](this)

  def consumer[F[_]: Sync]: KafkaConsumerApi[F, K, V] = api.KafkaConsumerApi[F, K, V](this)

  def producer[F[_]: ConcurrentEffect]: KafkaProducerApi[F, K, V] =
    api.KafkaProducerApi[F, K, V](this)

  def monitor[F[_]: ConcurrentEffect: ContextShift: Timer]: KafkaMonitoringApi[F, K, V] =
    api.KafkaMonitoringApi[F, K, V](this, settings.rootPath)

  def show: String =
    s"""
       |topic: ${topicDef.topicName}
       |settings: 
       |${settings.toString}
       |key-schema: 
       |${codec.keySchema.toString(true)}
       |value-schema:
       |${codec.valueSchema.toString(true)}
  """.stripMargin
}

object KafkaTopic {
  implicit def showKafkaTopic[K, V]: Show[KafkaTopic[K, V]] = _.topicName
}
