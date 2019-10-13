package com.github.chenharryhua.nanjin.kafka

import akka.stream.ActorMaterializer
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.{Bitraverse, Eval, Show}
import com.github.chenharryhua.nanjin.codec._
import fs2.kafka.{AdminClientSettings, KafkaByteConsumer, KafkaByteProducer}
import monocle.function.At
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}
import cats.implicits._
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

final case class TopicDef[K, V](topicName: String)(
  implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfValue: SerdeOf[V],
  val showKey: Show[K],
  val showValue: Show[V]) {

  val keySchemaLoc: String   = s"$topicName-key"
  val valueSchemaLoc: String = s"$topicName-value"

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)
}

object TopicDef {

  def apply[K: Show, V: Show](
    topicName: String,
    keySchema: ManualAvroSchema[K],
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf(keySchema), SerdeOf(valueSchema), Show[K], Show[V])

  def apply[K: Show: SerdeOf, V: Show](
    topicName: String,
    valueSchema: ManualAvroSchema[V]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf[K], SerdeOf(valueSchema), Show[K], Show[V])

  def apply[K: Show, V: Show: SerdeOf](
    topicName: String,
    keySchema: ManualAvroSchema[K]): TopicDef[K, V] =
    new TopicDef(topicName)(SerdeOf(keySchema), SerdeOf[V], Show[K], Show[V])
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
  schemaRegistrySettings: SchemaRegistrySettings,
  kafkaConsumerSettings: KafkaConsumerSettings,
  kafkaProducerSettings: KafkaProducerSettings,
  adminSettings: AdminClientSettings[F],
  sharedConsumer: Eval[MVar[F, KafkaByteConsumer]],
  sharedProducer: Eval[KafkaByteProducer],
  materializer: Eval[ActorMaterializer],
  sparkafkaParams: SparkafkaParams)(
  implicit
  val concurrentEffect: ConcurrentEffect[F],
  val contextShift: ContextShift[F],
  val timer: Timer[F])
    extends TopicNameExtractor[K, V] with SparkafkaModule[F, K, V] {
  import topicDef.{serdeOfKey, serdeOfValue, showKey, showValue}

  val consumerGroupId: Option[KafkaConsumerGroupId] =
    KafkaConsumerSettings.props
      .composeLens(At.at(ConsumerConfig.GROUP_ID_CONFIG))
      .get(kafkaConsumerSettings)
      .map(KafkaConsumerGroupId)

  override def extract(key: K, value: V, rc: RecordContext): String = topicDef.topicName

  val codec: TopicCodec[K, V] = TopicCodec(
    serdeOfKey.asKey(schemaRegistrySettings.props).codec(topicDef.topicName),
    serdeOfValue.asValue(schemaRegistrySettings.props).codec(topicDef.topicName))

  def decoder[G[_, _]: Bitraverse](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valueCodec)

  //channels
  val fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      topicDef.topicName,
      kafkaProducerSettings.fs2ProducerSettings(codec.keySerializer, codec.valueSerializer),
      kafkaConsumerSettings.fs2ConsumerSettings)

  val akkaResource: Resource[F, KafkaChannels.AkkaChannel[F, K, V]] = Resource.make(
    ConcurrentEffect[F].delay(
      new KafkaChannels.AkkaChannel[F, K, V](
        topicDef.topicName,
        kafkaProducerSettings.akkaProducerSettings(
          materializer.value.system,
          codec.keySerializer,
          codec.valueSerializer),
        kafkaConsumerSettings.akkaConsumerSettings(materializer.value.system),
        kafkaConsumerSettings.akkaCommitterSettings(materializer.value.system),
        materializer.value)))(_ => ConcurrentEffect[F].unit)

  val kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](topicDef.topicName, codec.keySerde, codec.valueSerde)

  // APIs
  val schemaRegistry: KafkaSchemaRegistry[F] = KafkaSchemaRegistry[F](this)
  val admin: KafkaTopicAdminApi[F]           = KafkaTopicAdminApi(this)
  val consumer: KafkaConsumerApi[F, K, V]    = KafkaConsumerApi[F, K, V](this)
  val producer: KafkaProducerApi[F, K, V]    = KafkaProducerApi[F, K, V](this)
  val monitor: KafkaMonitoringApi[F, K, V]   = KafkaMonitoringApi(this)
}
