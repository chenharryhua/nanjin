package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.kafka.serdes.{KafkaRecordSerde, KafkaSerde, Unregistered}
import fs2.kafka.{ConsumerSettings, ProducerSettings}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

final case class TopicSerde[K, V](topicName: TopicName, key: KafkaSerde[K], value: KafkaSerde[V])
    extends KafkaRecordSerde(key, value)

final case class TopicDef[K, V](topicName: TopicName, key: Unregistered[K], value: Unregistered[V]) {
  def withTopicName(tn: TopicName): TopicDef[K, V] = new TopicDef[K, V](tn, key, value)
  def consumerSettings[F[_]: Sync](
    srClient: SchemaRegistryClient,
    srs: SchemaRegistrySettings,
    cs: KafkaConsumerSettings): ConsumerSettings[F, K, V] = {
    val k = key.asKey(srClient, srs.config).deserializer[F]
    val v = value.asValue(srClient, srs.config).deserializer[F]
    ConsumerSettings[F, K, V](using k, v).withProperties(cs.properties)
  }

  def producerSettings[F[_]: Sync](
    srClient: SchemaRegistryClient,
    srs: SchemaRegistrySettings,
    ps: KafkaProducerSettings): ProducerSettings[F, K, V] = {
    val k = key.asKey(srClient, srs.config).serializer[F]
    val v = value.asValue(srClient, srs.config).serializer[F]
    ProducerSettings[F, K, V](using k, v).withProperties(ps.properties)
  }

  def register(srClient: SchemaRegistryClient, srs: SchemaRegistrySettings): TopicSerde[K, V] = {
    val k = key.asKey(srClient, srs.config).serde
    val v = value.asValue(srClient, srs.config).serde
    TopicSerde(topicName = topicName, key = KafkaSerde(k, topicName), value = KafkaSerde(v, topicName))
  }

}
