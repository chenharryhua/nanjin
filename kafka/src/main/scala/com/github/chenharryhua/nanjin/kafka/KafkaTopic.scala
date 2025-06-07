package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.kafka.streaming.KafkaStreamingConsumer

final class KafkaTopic[F[_], K, V] private[kafka] (val topicDef: TopicDef[K, V], val settings: KafkaSettings)
    extends Serializable {

  override def toString: String = topicName.value

  val topicName: TopicName = topicDef.topicName

  def withTopicName(tn: TopicName): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef.withTopicName(tn), settings)

  def withTopicName(tn: TopicNameL): KafkaTopic[F, K, V] =
    withTopicName(TopicName(tn))

  // need to reconstruct codec when working in spark
  @transient lazy val serdePair: RegisteredSerdePair[K, V] =
    topicDef.rawSerdes.register(settings.schemaRegistrySettings, topicName)

  object serde extends KafkaGenericSerde[K, V](serdePair.key, serdePair.value)

  // Streaming

  def asConsumer: KafkaStreamingConsumer[F, K, V] =
    new KafkaStreamingConsumer[F, K, V](topicName, serdePair, None, None, None)

//  def asProduced: Produced[K, V] =
//    Produced.`with`[K, V](serdePair.key.serde, serdePair.value.serde)

}
