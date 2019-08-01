package com.github.chenharryhua.nanjin.kafka

import akka.stream.ActorMaterializer
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.{Eval, Show}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

final class KafkaTopic[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
  val topicName: KafkaTopicName[K, V],
  fs2Settings: Fs2Settings,
  akkaSettings: AkkaSettings,
  srSettings: SchemaRegistrySettings,
  sharedConsumer: Eval[MVar[F, KafkaConsumer[Array[Byte], Array[Byte]]]],
  sharedProducer: Eval[KafkaProducer[Array[Byte], Array[Byte]]],
  materializer: Eval[ActorMaterializer],
  val keySerde: KeySerde[K],
  val valueSerde: ValueSerde[V]
) extends TopicNameExtractor[K, V] with Serializable {
  override def extract(key: K, value: V, rc: RecordContext): String =
    topicName.value
  override def toString: String = topicName.value

  val fs2Stream: Fs2Channel[F, K, V] =
    new Fs2Channel[F, K, V](topicName.value, fs2Settings, keySerde, valueSerde)

  val akkaStream: AkkaChannel[K, V] =
    new AkkaChannel[K, V](topicName.value, akkaSettings, keySerde, valueSerde)(materializer.value)

  val kafkaStream: StreamingChannel[K, V] =
    new StreamingChannel[K, V](topicName.value, keySerde, valueSerde)

  val recordDecoder: KafkaMessageDecoder[ConsumerRecord, K, V] =
    decoders.consumerRecordDecoder[K, V](topicName.value, keySerde, valueSerde)

  val recordEncoder: encoders.ProducerRecordEncoder[K, V] =
    encoders.producerRecordEncoder[K, V](topicName.value, keySerde, valueSerde)

  val schemaRegistry: KafkaSchemaRegistry[F] =
    KafkaSchemaRegistry[F](
      srSettings,
      topicName.value,
      topicName.keySchemaLoc,
      topicName.valueSchemaLoc,
      keySerde.schema,
      valueSerde.schema)

  val show: String =
    s"""
       |kafka topic: 
       |${topicName.value}
       |${fs2Settings.show}
       |${akkaSettings.show}""".stripMargin
}

object KafkaTopic {
  implicit def showTopic[F[_], K, V]: Show[KafkaTopic[F, K, V]] = _.show
}
