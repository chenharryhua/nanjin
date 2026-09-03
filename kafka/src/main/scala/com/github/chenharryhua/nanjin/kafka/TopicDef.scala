package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.kafka.config.{
  KafkaConsumerSettings,
  KafkaProducerSettings,
  SerdeSettings
}
import com.github.chenharryhua.nanjin.kafka.serdes.{KafkaRecordSerde, KafkaSerde, Unregistered}
import fs2.kafka.{ConsumerSettings, ProducerSettings}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

final case class TopicSerde[K, V](topicName: TopicName, key: KafkaSerde[K], value: KafkaSerde[V])
    extends KafkaRecordSerde(key, value)

/** A topic definition pairing a topic name with key and value serde specifications.
  *
  * `TopicDef` is the schema-level description of a Kafka topic. It carries unregistered serdes that become
  * fully configured once a `SchemaRegistryClient` is available. Use it to derive consumer settings, producer
  * settings, and registered `TopicSerde` instances.
  */
final class TopicDef[K, V] private (
  val topicName: TopicName,
  val key: Unregistered[K],
  val value: Unregistered[V]) {
  // Not a case class: comparing serde-carrying defs is meaningless, so structural equality is
  // intentionally omitted. A readable toString is still useful for logs.
  override def toString: String = s"TopicDef(${topicName.value})"

  def withTopicName(tn: String): TopicDef[K, V] = new TopicDef[K, V](TopicName(tn), key, value)
  def consumerSettings[F[_]: Sync](
    srClient: SchemaRegistryClient,
    srs: SerdeSettings,
    cs: KafkaConsumerSettings): ConsumerSettings[F, K, V] = {
    val k = key.asKey(srClient, srs.properties).deserializer[F]
    val v = value.asValue(srClient, srs.properties).deserializer[F]
    ConsumerSettings[F, K, V](using k, v).withProperties(cs.properties)
  }

  def attemptConsumerSettings[F[_]: Sync](
    srClient: SchemaRegistryClient,
    srs: SerdeSettings,
    cs: KafkaConsumerSettings): ConsumerSettings[F, Either[Throwable, K], Either[Throwable, V]] = {
    val k = key.asKey(srClient, srs.properties).deserializer[F].map(_.attempt)
    val v = value.asValue(srClient, srs.properties).deserializer[F].map(_.attempt)
    ConsumerSettings[F, Either[Throwable, K], Either[Throwable, V]](using k, v).withProperties(cs.properties)
  }

  def producerSettings[F[_]: Sync](
    srClient: SchemaRegistryClient,
    srs: SerdeSettings,
    ps: KafkaProducerSettings): ProducerSettings[F, K, V] = {
    val k = key.asKey(srClient, srs.properties).serializer[F]
    val v = value.asValue(srClient, srs.properties).serializer[F]
    ProducerSettings[F, K, V](using k, v).withProperties(ps.properties)
  }

  def register(srClient: SchemaRegistryClient, srs: SerdeSettings): TopicSerde[K, V] = {
    val k = key.asKey(srClient, srs.properties).serde
    val v = value.asValue(srClient, srs.properties).serde
    TopicSerde(topicName = topicName, key = KafkaSerde(k, topicName), value = KafkaSerde(v, topicName))
  }

}

object TopicDef {

  /** Construct from a bare topic name (String at the door); the name is wrapped in [[TopicName]].
    */
  def apply[K, V](topicName: String, key: Unregistered[K], value: Unregistered[V]): TopicDef[K, V] =
    new TopicDef(TopicName(topicName), key, value)
}
