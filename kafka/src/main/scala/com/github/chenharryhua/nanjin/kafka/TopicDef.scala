package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.{Endo, Show}
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.messages.kafka.codec.*
import fs2.kafka.{ConsumerSettings, ProducerRecord, ProducerSettings}

final case class TopicSerde[K, V](topicName: TopicName, key: KafkaSerde[K], value: KafkaSerde[V])
    extends KafkaGenericSerde(key, value)

sealed trait KafkaTopic[K, V] extends Serializable {
  def topicName: TopicName
  def consumerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    cs: KafkaConsumerSettings): ConsumerSettings[F, K, V]
  def producerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    ps: KafkaProducerSettings): ProducerSettings[F, K, V]

  def register(srs: SchemaRegistrySettings): TopicSerde[K, V]
}

final class AvroTopic[K, V] private (val topicName: TopicName, val pair: AvroForPair[K, V])
    extends KafkaTopic[K, V] {

  override def toString: String = topicName.value

  def withTopicName(tn: TopicNameL): AvroTopic[K, V] = new AvroTopic[K, V](TopicName(tn), pair)
  def modifyTopicName(f: Endo[String]): AvroTopic[K, V] =
    withTopicName(TopicName.unsafeFrom(f(topicName.value)).name)

  def producerRecord(k: K, v: V): ProducerRecord[K, V] = ProducerRecord(topicName.value, k, v)

  override def consumerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    cs: KafkaConsumerSettings): ConsumerSettings[F, K, V] =
    pair.consumerSettings[F](srs, cs)

  override def producerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    ps: KafkaProducerSettings): ProducerSettings[F, K, V] =
    pair.producerSettings[F](srs, ps)

  override def register(srs: SchemaRegistrySettings): TopicSerde[K, V] =
    TopicSerde(
      topicName,
      pair.key.asKey(srs.config).withTopic(topicName),
      pair.value.asValue(srs.config).withTopic(topicName))
}

object AvroTopic {

  implicit def showTopicDef[K, V]: Show[AvroTopic[K, V]] = Show.fromToString

  def apply[K, V](key: AvroFor[K], value: AvroFor[V], topicName: TopicName): AvroTopic[K, V] =
    new AvroTopic(topicName, AvroForPair(key, value))

  def apply[K: AvroFor, V: AvroFor](topicName: TopicName): AvroTopic[K, V] =
    apply[K, V](AvroFor[K], AvroFor[V], topicName)

  def apply[K: AvroFor, V: AvroFor](topicName: TopicNameL): AvroTopic[K, V] =
    apply[K, V](TopicName(topicName))
}

final class ProtobufTopic[K, V] private (val topicName: TopicName, val pair: ProtobufForPair[K, V])
    extends KafkaTopic[K, V] {
  override def consumerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    cs: KafkaConsumerSettings): ConsumerSettings[F, K, V] =
    pair.consumerSettings[F](srs, cs)

  override def producerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    ps: KafkaProducerSettings): ProducerSettings[F, K, V] =
    pair.producerSettings[F](srs, ps)

  override def register(srs: SchemaRegistrySettings): TopicSerde[K, V] =
    TopicSerde(
      topicName,
      pair.key.asKey(srs.config).withTopic(topicName),
      pair.value.asValue(srs.config).withTopic(topicName))
}

object ProtobufTopic {
  def apply[K, V](key: ProtobufFor[K], value: ProtobufFor[V], topicName: TopicName): ProtobufTopic[K, V] =
    new ProtobufTopic[K, V](topicName, ProtobufForPair[K, V](key, value))

  def apply[K: ProtobufFor, V: ProtobufFor](topicName: TopicName): ProtobufTopic[K, V] =
    apply[K, V](ProtobufFor[K], ProtobufFor[V], topicName)

  def apply[K: ProtobufFor, V: ProtobufFor](topicName: TopicNameL): ProtobufTopic[K, V] =
    apply[K, V](TopicName(topicName))
}

final class JsonSchemaTopic[K, V] private (val topicName: TopicName, val pair: JsonSchemaForPair[K, V])
    extends KafkaTopic[K, V] {
  override def consumerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    cs: KafkaConsumerSettings): ConsumerSettings[F, K, V] =
    pair.consumerSettings[F](srs, cs)
  override def producerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    ps: KafkaProducerSettings): ProducerSettings[F, K, V] =
    pair.producerSettings[F](srs, ps)

  override def register(srs: SchemaRegistrySettings): TopicSerde[K, V] =
    TopicSerde(
      topicName,
      pair.key.asKey(srs.config).withTopic(topicName),
      pair.value.asValue(srs.config).withTopic(topicName))
}
object JsonSchemaTopic {
  def apply[K, V](key: JsonFor[K], value: JsonFor[V], topicName: TopicName) =
    new JsonSchemaTopic[K, V](topicName, JsonSchemaForPair(key, value))

  def apply[K: JsonFor, V: JsonFor](topicName: TopicName): JsonSchemaTopic[K, V] =
    apply[K, V](JsonFor[K], JsonFor[V], topicName)

  def apply[K: JsonFor, V: JsonFor](topicName: TopicNameL): JsonSchemaTopic[K, V] =
    apply[K, V](TopicName(topicName))
}
