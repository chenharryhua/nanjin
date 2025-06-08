package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}

final class KafkaTopic[F[_], K, V] private[kafka] (val topicDef: TopicDef[K, V], val settings: KafkaSettings)
    extends Serializable {

  override def toString: String = topicName.value

  val topicName: TopicName = topicDef.topicName

  def withTopicName(tn: TopicName): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef.withTopicName(tn), settings)

  def withTopicName(tn: TopicNameL): KafkaTopic[F, K, V] =
    withTopicName(TopicName(tn))

  // need to reconstruct codec when working in spark
  @transient private lazy val registeredSerdePair: RegisteredSerdePair[K, V] =
    topicDef.serdePair.register(settings.schemaRegistrySettings, topicName)

  lazy val serde: KafkaGenericSerde[K, V] = new KafkaGenericSerde[K, V](registeredSerdePair.key, registeredSerdePair.value)

}
