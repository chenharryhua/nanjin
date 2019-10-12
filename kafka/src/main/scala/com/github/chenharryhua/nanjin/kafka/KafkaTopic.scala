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

final case class KafkaTopic[F[_], K, V] private[kafka] (
  topicDef: TopicDef[K, V],
  schemaRegistrySettings: SchemaRegistrySettings,
  kafkaConsumerSettings: KafkaConsumerSettings,
  kafkaProducerSettings: KafkaProducerSettings,
  adminSettings: AdminClientSettings[F],
  sharedConsumer: Eval[MVar[F, KafkaByteConsumer]],
  sharedProducer: Eval[KafkaByteProducer],
  materializer: Eval[ActorMaterializer],
  sparkafkaConf: SparkafkaConf)(
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

  val topicName: String = topicDef.topicName

  override def extract(key: K, value: V, rc: RecordContext): String = topicName

  override def toString: String = topicName

  val keyCodec: KafkaCodec[K] =
    serdeOfKey.asKey(schemaRegistrySettings.props).codec(topicName)

  val valueCodec: KafkaCodec[V] =
    serdeOfValue.asValue(schemaRegistrySettings.props).codec(topicName)

  def decoder[G[_, _]: Bitraverse](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, keyCodec, valueCodec)

  //channels
  val fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      topicName,
      kafkaProducerSettings.fs2ProducerSettings(keyCodec, valueCodec),
      kafkaConsumerSettings.fs2ConsumerSettings)

  val akkaResource: Resource[F, KafkaChannels.AkkaChannel[F, K, V]] = Resource.make(
    ConcurrentEffect[F].delay(
      new KafkaChannels.AkkaChannel[F, K, V](
        topicName,
        kafkaProducerSettings.akkaProducerSettings(materializer.value.system, keyCodec, valueCodec),
        kafkaConsumerSettings.akkaConsumerSettings(materializer.value.system),
        kafkaConsumerSettings.akkaCommitterSettings(materializer.value.system),
        materializer.value)))(_ => ConcurrentEffect[F].unit)

  val kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](keyCodec, valueCodec)

  // apis
  val schemaRegistry: KafkaSchemaRegistry[F] = KafkaSchemaRegistry[F](this)
  val admin: KafkaTopicAdminApi[F]           = KafkaTopicAdminApi(this)
  val consumer: KafkaConsumerApi[F, K, V]    = KafkaConsumerApi[F, K, V](this)
  val producer: KafkaProducerApi[F, K, V]    = KafkaProducerApi[F, K, V](this)
  val monitor: KafkaMonitoringApi[F, K, V]   = KafkaMonitoringApi(this)
}
