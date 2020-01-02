package com.github.chenharryhua.nanjin.kafka

import cats.effect.Resource
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

final class TopicCodec[K, V] private[kafka] (
  val keyCodec: KafkaCodec.Key[K],
  val valueCodec: KafkaCodec.Value[V]) {
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

final class KafkaTopic[F[_], K, V] private[kafka] (
  val topicDef: TopicDef[K, V],
  val context: KafkaContext[F])
    extends TopicNameExtractor[K, V] {
  import context.{concurrentEffect, contextShift, timer}
  import topicDef.{serdeOfKey, serdeOfValue, showKey, showValue}

  val topicName: String = topicDef.topicName

  val consumerGroupId: Option[KafkaConsumerGroupId] =
    KafkaConsumerSettings.config
      .composeLens(At.at(ConsumerConfig.GROUP_ID_CONFIG))
      .get(context.settings.consumerSettings)
      .map(KafkaConsumerGroupId)

  override def extract(key: K, value: V, rc: RecordContext): String = topicDef.topicName

  val codec: TopicCodec[K, V] = new TopicCodec(
    serdeOfKey.asKey(context.settings.schemaRegistrySettings.config).codec(topicDef.topicName),
    serdeOfValue.asValue(context.settings.schemaRegistrySettings.config).codec(topicDef.topicName)
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
  val fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      topicDef.topicName,
      context.settings.producerSettings
        .fs2ProducerSettings(codec.keySerializer, codec.valueSerializer),
      context.settings.consumerSettings.fs2ConsumerSettings)

  val akkaResource: Resource[F, KafkaChannels.AkkaChannel[F, K, V]] = Resource.make(
    concurrentEffect.delay(
      new KafkaChannels.AkkaChannel[F, K, V](
        topicDef.topicName,
        context.settings.producerSettings.akkaProducerSettings(
          context.akkaSystem.value,
          codec.keySerializer,
          codec.valueSerializer),
        context.settings.consumerSettings.akkaConsumerSettings(context.akkaSystem.value),
        context.settings.consumerSettings.akkaCommitterSettings(context.akkaSystem.value))))(_ =>
    concurrentEffect.unit)

  val kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](topicDef.topicName, codec.keySerde, codec.valueSerde)

  // APIs
  val schemaRegistry: KafkaSchemaRegistryApi[F] = api.KafkaSchemaRegistryApi[F](this)
  val admin: KafkaTopicAdminApi[F]              = api.KafkaTopicAdminApi[F, K, V](this)
  val consumer: KafkaConsumerApi[F, K, V]       = api.KafkaConsumerApi[F, K, V](this)
  val producer: KafkaProducerApi[F, K, V]       = api.KafkaProducerApi[F, K, V](this)

  val monitor: KafkaMonitoringApi[F, K, V] =
    api.KafkaMonitoringApi[F, K, V](this, context.settings.rootPath)

  def show: String =
    s"""
       |topic: ${topicDef.topicName}
       |settings: 
       |${context.settings.toString}
       |key-schema: 
       |${codec.keySchema.toString(true)}
       |value-schema:
       |${codec.valueSchema.toString(true)}
  """.stripMargin
}

object KafkaTopic {
  implicit def showKafkaTopic[F[_], K, V]: Show[KafkaTopic[F, K, V]] = _.topicName
}
