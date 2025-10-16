package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.{Endo, Show}
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.messages.kafka.codec.*
import fs2.kafka.{ConsumerSettings, ProducerRecord, ProducerSettings}

final case class TopicSerde[K, V](topicName: TopicName, key: KafkaSerde[K], value: KafkaSerde[V])
    extends KafkaGenericSerde(key, value)

sealed trait KafkaTopic[K, V] {
  def topicName: TopicName
  def consumerSettings[F[_]](srs: SchemaRegistrySettings, cs: KafkaConsumerSettings)(implicit
    F: Sync[F]): ConsumerSettings[F, K, V]
  def producerSettings[F[_]](srs: SchemaRegistrySettings, ps: KafkaProducerSettings)(implicit
    F: Sync[F]): ProducerSettings[F, K, V]

  def register(srs: SchemaRegistrySettings): TopicSerde[K, V]
}

final case class AvroTopic[K, V] private (topicName: TopicName, pair: AvroForPair[K, V])
    extends KafkaTopic[K, V] {

  override def toString: String = topicName.name.value

  def withTopicName(tn: TopicNameL): AvroTopic[K, V] = new AvroTopic[K, V](TopicName(tn), pair)
  def modifyTopicName(f: Endo[String]): AvroTopic[K, V] =
    withTopicName(TopicName.unsafeFrom(f(topicName.name.value)).name)

  def producerRecord(k: K, v: V): ProducerRecord[K, V] = ProducerRecord(topicName.name.value, k, v)

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

final case class ProtoTopic[K, V] private (topicName: TopicName, pair: ProtoForPair[K, V])
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

object ProtoTopic {
  def apply[K, V](key: ProtoFor[K], value: ProtoFor[V], topicName: TopicName): ProtoTopic[K, V] =
    new ProtoTopic[K, V](topicName, ProtoForPair[K, V](key, value))

  def apply[K: ProtoFor, V: ProtoFor](topicName: TopicName): ProtoTopic[K, V] =
    apply[K, V](ProtoFor[K], ProtoFor[V], topicName)

  def apply[K: ProtoFor, V: ProtoFor](topicName: TopicNameL): ProtoTopic[K, V] =
    apply[K, V](TopicName(topicName))
}

final case class JsonTopic[K, V] private (topicName: TopicName, pair: JsonForPair[K, V])
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
object JsonTopic {
  def apply[K, V](key: JsonFor[K], value: JsonFor[V], topicName: TopicName) =
    new JsonTopic[K, V](topicName, JsonForPair(key, value))

  def apply[K: JsonFor, V: JsonFor](topicName: TopicName): JsonTopic[K, V] =
    apply[K, V](JsonFor[K], JsonFor[V], topicName)

  def apply[K: JsonFor, V: JsonFor](topicName: TopicNameL): JsonTopic[K, V] =
    apply[K, V](TopicName(topicName))
}
