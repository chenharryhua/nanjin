package com.github.chenharryhua.nanjin.kafka

import akka.stream.ActorMaterializer
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.{Eval, Show}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

final case class TopicDef[K, V](topicName: String) {
  val keySchemaLoc: String   = s"$topicName-key"
  val valueSchemaLoc: String = s"$topicName-value"
}

final class KafkaTopic[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
  topicDef: TopicDef[K, V],
  fs2Settings: Fs2Settings,
  akkaSettings: AkkaSettings,
  srSettings: SchemaRegistrySettings,
  sharedConsumer: Eval[MVar[F, KafkaConsumer[Array[Byte], Array[Byte]]]],
  sharedProducer: Eval[KafkaProducer[Array[Byte], Array[Byte]]],
  materializer: ActorMaterializer,
  val keySerde: KeySerde[K],
  val valueSerde: ValueSerde[V]
) extends TopicNameExtractor[K, V] with Serializable {
  val topicName: String = topicDef.topicName

  override def extract(key: K, value: V, rc: RecordContext): String = topicName

  override def toString: String = topicName

  val fs2Stream: Fs2Channel[F, K, V] =
    new Fs2Channel[F, K, V](topicDef, fs2Settings, keySerde, valueSerde)

  val akkaStream: AkkaChannel[F, K, V] =
    new AkkaChannel[F, K, V](topicDef, akkaSettings, keySerde, valueSerde, materializer)

  val kafkaStream: StreamingChannel[K, V] =
    new StreamingChannel[K, V](topicDef, keySerde, valueSerde)

  val recordDecoder: KafkaMessageDecoder[ConsumerRecord, K, V] =
    decoders.consumerRecordDecoder[K, V](topicDef.topicName, keySerde, valueSerde)

  val recordEncoder: encoders.ProducerRecordEncoder[K, V] =
    encoders.producerRecordEncoder[K, V](topicDef.topicName, keySerde, valueSerde)

  val schemaRegistry: KafkaSchemaRegistry[F] =
    KafkaSchemaRegistry[F](
      srSettings,
      topicDef.topicName,
      topicDef.keySchemaLoc,
      topicDef.valueSchemaLoc,
      keySerde.schema,
      valueSerde.schema)

  val consumer: KafkaConsumerApi[F, K, V] =
    KafkaConsumerApi[F, K, V](topicDef, sharedConsumer, recordDecoder)

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
