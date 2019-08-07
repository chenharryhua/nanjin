package com.github.chenharryhua.nanjin.kafka

import akka.stream.ActorMaterializer
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.{Eval, Show}
import fs2.kafka.{KafkaByteConsumer, KafkaByteProducer}
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord
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
  fs2Settings: Fs2Settings,
  akkaSettings: AkkaSettings,
  sharedConsumer: Eval[MVar[F, KafkaByteConsumer]],
  sharedProducer: Eval[KafkaByteProducer],
  materializer: Eval[ActorMaterializer])
    extends TopicNameExtractor[K, V] with Serializable {
  val topicName: String = topicDef.topicName

  override def extract(key: K, value: V, rc: RecordContext): String = topicName

  override def toString: String = topicName

  val keyIso: Iso[Array[Byte], K]   = keySerde.iso(topicName)
  val valueIso: Iso[Array[Byte], V] = valueSerde.iso(topicName)

  val fs2Channel: Fs2Channel[F, K, V] =
    new Fs2Channel[F, K, V](
      topicName,
      fs2Settings.producerSettings(keySerde.serializer, valueSerde.serializer),
      fs2Settings.consumerSettings,
      keyIso,
      valueIso)

  val akkaResource: Resource[F, AkkaChannel[F, K, V]] = Resource.make(
    ConcurrentEffect[F].delay(
      new AkkaChannel[F, K, V](
        topicName,
        akkaSettings
          .producerSettings(materializer.value.system, keySerde.serializer, valueSerde.serializer),
        akkaSettings.consumerSettings(materializer.value.system),
        akkaSettings.committerSettings(materializer.value.system),
        keyIso,
        valueIso,
        materializer.value)))(_ => ConcurrentEffect[F].unit)

  val kafkaStream: StreamingChannel[K, V] =
    new StreamingChannel[K, V](topicDef, keySerde, valueSerde)

  val recordEncoder: encoders.ProducerRecordEncoder[K, V] =
    encoders.producerRecordEncoder[K, V](topicDef.topicName, keySerde, valueSerde)

  val schemaRegistry: KafkaSchemaRegistry[F] =
    KafkaSchemaRegistry[F](
      schemaRegistrySettings,
      topicDef.topicName,
      topicDef.keySchemaLoc,
      topicDef.valueSchemaLoc,
      keySerde.schema,
      valueSerde.schema)

  val consumer: KafkaConsumerApi[F, K, V] =
    KafkaConsumerApi[F, K, V](topicDef, sharedConsumer)

  val producer: KafkaProducerApi[F, K, V] =
    KafkaProducerApi[F, K, V](sharedProducer, recordEncoder)

  val show: String =
    s"""
       |kafka topic: 
       |${topicDef.topicName}
       |${fs2Settings.show}
       |${akkaSettings.show}""".stripMargin
}

object KafkaTopic {
  implicit def showTopic[F[_], K, V]: Show[KafkaTopic[F, K, V]] = _.show
}
