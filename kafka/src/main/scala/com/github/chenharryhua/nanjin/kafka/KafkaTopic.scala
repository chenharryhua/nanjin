package com.github.chenharryhua.nanjin.kafka

import akka.stream.ActorMaterializer
import cats.effect.concurrent.MVar
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.{Eval, Show}
import fs2.kafka.{KafkaByteConsumer, KafkaByteProducer}
import org.apache.kafka.clients.consumer.ConsumerRecord
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
  sharedConsumer: Eval[MVar[F, KafkaByteConsumer]],
  sharedProducer: Eval[KafkaByteProducer],
  val keySerde: KeySerde[K],
  val valueSerde: ValueSerde[V]
)(implicit materializer: ActorMaterializer)
    extends TopicNameExtractor[K, V] with Serializable {
  val topicName: String = topicDef.topicName

  override def extract(key: K, value: V, rc: RecordContext): String = topicName

  override def toString: String = topicName

  val fs2Channel: Fs2Channel[F, K, V] =
    new Fs2Channel[F, K, V](
      topicDef,
      fs2Settings.producerSettings(keySerde.serializer, valueSerde.serializer),
      fs2Settings.consumerSettings,
      keySerde,
      valueSerde)

  val akkaChannel: AkkaChannel[F, K, V] =
    new AkkaChannel[F, K, V](
      topicDef,
      akkaSettings
        .producerSettings(materializer.system, keySerde.serializer, valueSerde.serializer),
      akkaSettings.consumerSettings(materializer.system),
      akkaSettings.committerSettings(materializer.system),
      keySerde,
      valueSerde)

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
