package com.github.chenharryhua.nanjin.kafka

import akka.stream.ActorMaterializer
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.{Eval, Show}
import fs2.kafka.{KafkaByteConsumer, KafkaByteProducer}
import monocle.Iso
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

final case class TopicDef[K: SerdeOf, V: SerdeOf](topicName: String) {
  val keySchemaLoc: String   = s"$topicName-key"
  val valueSchemaLoc: String = s"$topicName-value"

  def in[F[_]](ctx: KafkaContext[F]): KafkaTopic[F, K, V] =
    ctx.topic[K, V](this)
}

final class KafkaTopic[F[_]: ConcurrentEffect: ContextShift: Timer, K, V] private[kafka] (
  val topicDef: TopicDef[K, V],
  val keySerde: KeySerde[K],
  val valueSerde: ValueSerde[V],
  val schemaRegistrySettings: SchemaRegistrySettings,
  val kafkaConsumerSettings: KafkaConsumerSettings,
  val kafkaProducerSettings: KafkaProducerSettings,
  sharedConsumer: Eval[MVar[F, KafkaByteConsumer]],
  sharedProducer: Eval[KafkaByteProducer],
  materializer: Eval[ActorMaterializer])
    extends TopicNameExtractor[K, V] with codec.KafkaRecordCodec[K, V] {

  val topicName: String = topicDef.topicName

  override def extract(key: K, value: V, rc: RecordContext): String = topicName

  override def toString: String = topicName

  val keyIso: Iso[Array[Byte], K]   = keySerde.iso(topicName)
  val valueIso: Iso[Array[Byte], V] = valueSerde.iso(topicName)

  val fs2Channel: KafkaChannels.Fs2Channel[F, K, V] =
    new KafkaChannels.Fs2Channel[F, K, V](
      topicName,
      kafkaProducerSettings.fs2ProducerSettings(keySerde.serializer, valueSerde.serializer),
      kafkaConsumerSettings.fs2ConsumerSettings,
      keyIso,
      valueIso)

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
        keyIso,
        valueIso,
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

  val consumer: KafkaConsumerApi[F, K, V] =
    KafkaConsumerApi[F, K, V](topicName, sharedConsumer)

  val producer: KafkaProducerApi[F, K, V] =
    KafkaProducerApi[F, K, V](topicName, keyIso, valueIso, sharedProducer)

  val monitor: KafkaMonitoringApi[F, K, V] =
    KafkaMonitoringApi(fs2Channel, akkaResource, consumer)

  val stats: KafkaStatistics[F] =
    KafkaStatistics[F, K, V](consumer, akkaResource)

  val show: String =
    s"""
       |kafka topic: 
       |${topicDef.topicName}
       |${schemaRegistrySettings.show}
       |${kafkaConsumerSettings.show}
       |${kafkaProducerSettings.show}""".stripMargin
}

object KafkaTopic {
  implicit def showTopic[F[_], K, V]: Show[KafkaTopic[F, K, V]] = _.show
}
