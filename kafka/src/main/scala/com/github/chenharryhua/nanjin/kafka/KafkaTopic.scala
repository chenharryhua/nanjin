package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.kafka.streaming.{KafkaStreamingConsumer, StateStores}
import fs2.kafka.*
import org.apache.kafka.streams.scala.kstream.Produced

final class KafkaTopic[F[_], K, V] private[kafka] (val topicDef: TopicDef[K, V], val settings: KafkaSettings)
    extends Serializable {

  override def toString: String = topicName.value

  val topicName: TopicName = topicDef.topicName

  def withTopicName(tn: TopicName): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef.withTopicName(tn), settings)

  def withTopicName(tn: TopicNameL): KafkaTopic[F, K, V] =
    withTopicName(TopicName(tn))

  // need to reconstruct codec when working in spark
  @transient lazy val serdePair: KeyValueSerdePair[K, V] =
    topicDef.rawSerdes.register(settings.schemaRegistrySettings, topicName)

  object serde extends KafkaGenericSerde[K, V](serdePair.key, serdePair.value)

  // consumer and producer

  def consume(implicit F: Sync[F]): KafkaConsume[F, K, V] =
    new KafkaConsume[F, K, V](
      topicName,
      ConsumerSettings[F, K, V](
        Deserializer.delegate[F, K](serdePair.key.serde.deserializer()),
        Deserializer.delegate[F, V](serdePair.value.serde.deserializer()))
        .withProperties(settings.consumerSettings.properties)
    )

  // Streaming

  def asConsumer: KafkaStreamingConsumer[F, K, V] =
    new KafkaStreamingConsumer[F, K, V](topicName, serdePair, None, None, None)

  def asProduced: Produced[K, V] =
    Produced.`with`[K, V](serdePair.key.serde, serdePair.value.serde)

  def asStateStore(storeName: TopicName): StateStores[K, V] = {
    require(storeName.value =!= topicName.value, "should provide a name other than the topic name")
    StateStores[K, V](storeName, KeyValueSerdePair(serdePair.key, serdePair.value))
  }
  def asStateStore(storeName: TopicNameL): StateStores[K, V] =
    asStateStore(TopicName(storeName))

}
