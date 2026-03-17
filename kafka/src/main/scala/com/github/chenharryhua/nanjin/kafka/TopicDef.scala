package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.kafka.serdes.{KafkaGenericSerde, KafkaSerde, Unregistered}
import fs2.kafka.{ConsumerSettings, ProducerSettings}

final case class TopicSerde[K, V](topicName: TopicName, key: KafkaSerde[K], value: KafkaSerde[V])
    extends KafkaGenericSerde(key, value)

final class TopicDef[K, V](val topicName: TopicName, key: Unregistered[K], value: Unregistered[V]) {
  def withTopicName(tn: TopicName): TopicDef[K, V] = new TopicDef[K, V](tn, key, value)
  def consumerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    cs: KafkaConsumerSettings): ConsumerSettings[F, K, V] = {
    val k = key.asKey(srs.config).deserializer[F]
    val v = value.asValue(srs.config).deserializer[F]
    ConsumerSettings[F, K, V](k, v).withProperties(cs.properties)
  }

  def producerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    ps: KafkaProducerSettings): ProducerSettings[F, K, V] = {
    val k = key.asKey(srs.config).serializer[F]
    val v = value.asValue(srs.config).serializer[F]
    ProducerSettings[F, K, V](k, v).withProperties(ps.properties)
  }

  def register(srs: SchemaRegistrySettings): TopicSerde[K, V] = {
    val k = key.asKey(srs.config).serde
    val v = value.asValue(srs.config).serde
    TopicSerde(topicName = topicName, key = KafkaSerde(k, topicName), value = KafkaSerde(v, topicName))
  }

}
