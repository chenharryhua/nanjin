package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaCommittableMessage}
import akka.stream.ActorMaterializer
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.{Eval, Show}
import com.github.chenharryhua.nanjin.codec.BitraverseMessage._
import com.github.chenharryhua.nanjin.codec._
import fs2.kafka.{
  AdminClientSettings,
  KafkaByteConsumer,
  KafkaByteProducer,
  CommittableConsumerRecord => Fs2CommittableConsumerRecord
}
import monocle.function.At
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
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

final class KafkaTopic[F[_]: ConcurrentEffect: ContextShift: Timer, K, V] private[kafka] (
  val topicDef: TopicDef[K, V],
  val schemaRegistrySettings: SchemaRegistrySettings,
  val kafkaConsumerSettings: KafkaConsumerSettings,
  val kafkaProducerSettings: KafkaProducerSettings,
  val adminSettings: AdminClientSettings[F],
  val sharedConsumer: Eval[MVar[F, KafkaByteConsumer]],
  val sharedProducer: Eval[KafkaByteProducer],
  val materializer: Eval[ActorMaterializer])
    extends TopicNameExtractor[K, V] {
  import topicDef.{serdeOfKey, serdeOfValue, showKey, showValue}

  val consumerGroupId: Option[KafkaConsumerGroupId] =
    KafkaConsumerSettings.props
      .composeLens(At.at(ConsumerConfig.GROUP_ID_CONFIG))
      .get(kafkaConsumerSettings)
      .map(KafkaConsumerGroupId)

  val topicName: String = topicDef.topicName

  override def extract(key: K, value: V, rc: RecordContext): String = topicName

  override def toString: String = topicName

  val keySerde: KeySerde[K]     = serdeOfKey.asKey(schemaRegistrySettings.props)
  val valueSerde: ValueSerde[V] = serdeOfValue.asValue(schemaRegistrySettings.props)

  val keyCodec: KafkaCodec[K]   = keySerde.codec(topicName)
  val valueCodec: KafkaCodec[V] = valueSerde.codec(topicName)

  def decoder(
    cr: ConsumerRecord[Array[Byte], Array[Byte]]): KafkaGenericDecoder[ConsumerRecord, K, V] =
    new KafkaGenericDecoder[ConsumerRecord, K, V](cr, keyCodec, valueCodec)

  def decoder(msg: AkkaCommittableMessage[Array[Byte], Array[Byte]])
    : KafkaGenericDecoder[AkkaCommittableMessage, K, V] =
    new KafkaGenericDecoder[AkkaCommittableMessage, K, V](msg, keyCodec, valueCodec)

  def decoder(msg: Fs2CommittableConsumerRecord[F, Array[Byte], Array[Byte]])
    : KafkaGenericDecoder[Fs2CommittableConsumerRecord[F, *, *], K, V] =
    new KafkaGenericDecoder[Fs2CommittableConsumerRecord[F, *, *], K, V](msg, keyCodec, valueCodec)

  val fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      topicName,
      kafkaProducerSettings.fs2ProducerSettings(keySerde.serializer, valueSerde.serializer),
      kafkaConsumerSettings.fs2ConsumerSettings,
      keyCodec,
      valueCodec)

  val akkaResource: Resource[F, KafkaChannels.AkkaChannel[F, K, V]] = Resource.make(
    ConcurrentEffect[F].delay(
      new KafkaChannels.AkkaChannel[F, K, V](
        topicName,
        kafkaProducerSettings.akkaProducerSettings(
          materializer.value.system,
          keySerde.serializer,
          valueSerde.serializer),
        kafkaConsumerSettings.akkaConsumerSettings(materializer.value.system),
        kafkaConsumerSettings.akkaCommitterSettings(materializer.value.system),
        keyCodec,
        valueCodec,
        materializer.value)))(_ => ConcurrentEffect[F].unit)

  val kafkaStream: KafkaChannels.StreamingChannel[K, V] =
    new KafkaChannels.StreamingChannel[K, V](topicName, keySerde, valueSerde)

  val schemaRegistry: KafkaSchemaRegistry[F] =
    KafkaSchemaRegistry[F](
      schemaRegistrySettings,
      topicName,
      topicDef.keySchemaLoc,
      topicDef.valueSchemaLoc,
      keySerde.schema,
      valueSerde.schema)

  val admin: KafkaTopicAdminApi[F]         = KafkaTopicAdminApi(this)
  val consumer: KafkaConsumerApi[F, K, V]  = KafkaConsumerApi[F, K, V](this)
  val producer: KafkaProducerApi[F, K, V]  = KafkaProducerApi[F, K, V](this)
  val monitor: KafkaMonitoringApi[F, K, V] = KafkaMonitoringApi(this)

  def show: String =
    s"""
       |kafka topic:
       |group id: ${consumerGroupId.map(_.value).getOrElse("not configured")}
       |${topicDef.topicName}
       |${schemaRegistrySettings.show}
       |${kafkaConsumerSettings.show}
       |${kafkaProducerSettings.show}""".stripMargin
}

object KafkaTopic {
  implicit def showTopic[F[_], K, V]: Show[KafkaTopic[F, K, V]] = _.show
}
