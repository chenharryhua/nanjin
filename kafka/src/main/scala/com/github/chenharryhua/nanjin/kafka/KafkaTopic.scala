package com.github.chenharryhua.nanjin.kafka

import akka.stream.ActorMaterializer
import cats.Show
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

final case class KafkaTopicName(value: String) extends AnyVal {
  def keySchemaLoc: String   = s"$value-key"
  def valueSchemaLoc: String = s"$value-value"

  def in[K: SerdeOf, V: SerdeOf](ctx: KafkaContext): KafkaTopic[K, V] =
    ctx.topic[K, V](this)
}

final case class KafkaTopic[K: SerdeOf, V: SerdeOf](
  topicName: KafkaTopicName,
  fs2Settings: Fs2Settings,
  akkaSettings: AkkaSettings)
    extends TopicNameExtractor[K, V] {
  override def extract(key: K, value: V, rc: RecordContext): String = topicName.value
  override def toString: String                                     = topicName.value

  def fs2Stream[F[_]: ConcurrentEffect: ContextShift: Timer]: Fs2Channel[F, K, V] =
    new Fs2Channel[F, K, V](topicName, fs2Settings)

  def akkaStream(implicit materializer: ActorMaterializer): AkkaChannel[K, V] =
    new AkkaChannel[K, V](topicName, akkaSettings)

  val kafkaStream: StreamingChannel[K, V] =
    new StreamingChannel[K, V](topicName)

  val recordDecoder: KafkaMessageDecoder[ConsumerRecord, K, V] =
    decoders.consumerRecordDecoder[K, V](topicName)

  val recordEncoder: encoders.ProducerRecordEncoder[K, V] =
    encoders.producerRecordEncoder[K, V](topicName)

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
