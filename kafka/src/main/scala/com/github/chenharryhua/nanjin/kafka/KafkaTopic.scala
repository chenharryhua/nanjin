package com.github.chenharryhua.nanjin.kafka

import akka.stream.ActorMaterializer
import cats.Show
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

final case class KafkaTopicName(value: String) extends AnyVal {
  def keySchemaLoc: String   = s"$value-key"
  def valueSchemaLoc: String = s"$value-value"

  def in[K: ctx.SerdeOf, V: ctx.SerdeOf](ctx: KafkaContext): KafkaTopic[K, V] =
    ctx.topic[K, V](this)
}

final class KafkaTopic[K, V](
  topicName: KafkaTopicName,
  fs2Settings: Fs2Settings,
  akkaSettings: AkkaSettings,
  srClient: CachedSchemaRegistryClient,
  keySerde: KeySerde[K],
  valueSerde: ValueSerde[V]
) extends TopicNameExtractor[K, V] with Serializable {
  override def extract(key: K, value: V, rc: RecordContext): String =
    topicName.value
  override def toString: String = topicName.value

  def fs2Stream[F[_]: ConcurrentEffect: ContextShift: Timer]: Fs2Channel[F, K, V] =
    new Fs2Channel[F, K, V](topicName, fs2Settings, keySerde, valueSerde)

  def akkaStream(implicit materializer: ActorMaterializer): AkkaChannel[K, V] =
    new AkkaChannel[K, V](topicName, akkaSettings, keySerde, valueSerde)

  val kafkaStream: StreamingChannel[K, V] =
    new StreamingChannel[K, V](topicName, keySerde, valueSerde)

  val recordDecoder: KafkaMessageDecoder[ConsumerRecord, K, V] =
    decoders.consumerRecordDecoder[K, V](topicName, keySerde, valueSerde)

  val recordEncoder: encoders.ProducerRecordEncoder[K, V] =
    encoders.producerRecordEncoder[K, V](topicName, keySerde, valueSerde)

  def schemaRegistry[F[_]: Sync]: KafkaSchemaRegistry[F] =
    KafkaSchemaRegistry[F](srClient, topicName, keySerde.schema, valueSerde.schema)

  def show: String =
    s"""
       |kafka topic: 
       |${topicName.value}
       |${fs2Settings.show}
       |${akkaSettings.show}""".stripMargin
}

object KafkaTopic {
  implicit def showTopic[K, V]: Show[KafkaTopic[K, V]] = _.show
}
