package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.effect.Resource
import cats.implicits._
import com.github.chenharryhua.nanjin.codec._
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import monocle.function.At
import org.apache.avro.Schema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

final case class TopicDef[K, V](topicName: String)(
  implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfValue: SerdeOf[V],
  val showKey: Show[K],
  val showValue: Show[V],
  val jsonKeyEncoder: JsonEncoder[K],
  val jsonValueEncoder: JsonEncoder[V],
  val jsonKeyDecoder: JsonDecoder[K],
  val jsonValueDecoder: JsonDecoder[V]
) {
  val keySchemaLoc: String   = s"$topicName-key"
  val valueSchemaLoc: String = s"$topicName-value"

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)
}

object TopicDef {

  def apply[K: Show: JsonEncoder: JsonDecoder, V: Show: JsonEncoder: JsonDecoder](
    topicName: String,
    keySchema: ManualAvroSchema[K],
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(topicName)(
      SerdeOf(keySchema),
      SerdeOf(valueSchema),
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V])

  def apply[K: Show: SerdeOf: JsonEncoder: JsonDecoder, V: Show: JsonEncoder: JsonDecoder](
    topicName: String,
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(topicName)(
      SerdeOf[K],
      SerdeOf(valueSchema),
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V])

  def apply[K: Show: JsonEncoder: JsonDecoder, V: Show: SerdeOf: JsonEncoder: JsonDecoder](
    topicName: String,
    keySchema: ManualAvroSchema[K]): TopicDef[K, V] =
    new TopicDef(topicName)(
      SerdeOf(keySchema),
      SerdeOf[V],
      Show[K],
      Show[V],
      JsonEncoder[K],
      JsonEncoder[V],
      JsonDecoder[K],
      JsonDecoder[V])
}

final case class TopicCodec[K, V] private[kafka] (
  keyCodec: KafkaCodec.Key[K],
  valueCodec: KafkaCodec.Value[V]) {
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

final case class KafkaTopic[F[_], K, V] private[kafka] (
  topicDef: TopicDef[K, V],
  context: KafkaContext[F])
    extends TopicNameExtractor[K, V] {
  import context.{concurrentEffect, contextShift, timer}
  import topicDef.{serdeOfKey, serdeOfValue, showKey, showValue}

  val consumerGroupId: Option[KafkaConsumerGroupId] =
    KafkaConsumerSettings.config
      .composeLens(At.at(ConsumerConfig.GROUP_ID_CONFIG))
      .get(context.settings.consumerSettings)
      .map(KafkaConsumerGroupId)

  override def extract(key: K, value: V, rc: RecordContext): String = topicDef.topicName

  val codec: TopicCodec[K, V] = TopicCodec(
    serdeOfKey.asKey(context.settings.schemaRegistrySettings.config).codec(topicDef.topicName),
    serdeOfValue.asValue(context.settings.schemaRegistrySettings.config).codec(topicDef.topicName)
  )

  def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valueCodec)

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
  val schemaRegistry: KafkaSchemaRegistryApi[F] = KafkaSchemaRegistryApi[F](this)
  val admin: KafkaTopicAdminApi[F]              = KafkaTopicAdminApi[F, K, V](this)
  val consumer: KafkaConsumerApi[F, K, V]       = KafkaConsumerApi[F, K, V](this)
  val producer: KafkaProducerApi[F, K, V]       = KafkaProducerApi[F, K, V](this)
  val monitor: KafkaMonitoringApi[F, K, V]      = KafkaMonitoringApi[F, K, V](this)
}
