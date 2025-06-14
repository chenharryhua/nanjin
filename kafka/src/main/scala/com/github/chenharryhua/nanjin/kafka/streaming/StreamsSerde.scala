package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{AvroCodecPair, SchemaRegistrySettings, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodecOf, KafkaSerde}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.*

final class StreamsSerde private[kafka] (schemaRegistrySettings: SchemaRegistrySettings)
    extends Serializable {
  object implicits {
    private def asKey[K: AvroCodecOf]: Serde[K] =
      AvroCodecOf[K].asKey(schemaRegistrySettings.config).serde
    private def asValue[V: AvroCodecOf]: Serde[V] =
      AvroCodecOf[V].asValue(schemaRegistrySettings.config).serde

    implicit def consumed[K: AvroCodecOf, V: AvroCodecOf]: Consumed[K, V] =
      Consumed.`with`[K, V](asKey[K], asValue[V])

    implicit def produced[K: AvroCodecOf, V: AvroCodecOf]: Produced[K, V] =
      Produced.`with`[K, V](asKey[K], asValue[V])

    implicit def joined[K: AvroCodecOf, V: AvroCodecOf, VO: AvroCodecOf]: Joined[K, V, VO] =
      Joined.`with`(asKey[K], asValue[V], asValue[VO])

    implicit def grouped[K: AvroCodecOf, V: AvroCodecOf]: Grouped[K, V] =
      Grouped.`with`(asKey[K], asValue[V])

    implicit def repartitioned[K: AvroCodecOf, V: AvroCodecOf]: Repartitioned[K, V] =
      Repartitioned.`with`(asKey[K], asValue[V])

    implicit def streamJoined[K: AvroCodecOf, V: AvroCodecOf, VO: AvroCodecOf]: StreamJoined[K, V, VO] =
      StreamJoined.`with`(asKey[K], asValue[V], asValue[VO])
  }

  def store[K: AvroCodecOf, V: AvroCodecOf](storeName: TopicName): StateStores[K, V] =
    StateStores[K, V](
      AvroCodecPair[K, V](AvroCodecOf[K], AvroCodecOf[V]).register(schemaRegistrySettings, storeName))

  def store[K, V](topic: TopicDef[K, V]): StateStores[K, V] =
    store(topic.topicName)(topic.codecPair.key, topic.codecPair.value)

  def keySerde[K: AvroCodecOf](topicName: TopicName): KafkaSerde[K] =
    AvroCodecOf[K].asKey(schemaRegistrySettings.config).withTopic(topicName)

  def valueSerde[V: AvroCodecOf](topicName: TopicName): KafkaSerde[V] =
    AvroCodecOf[V].asValue(schemaRegistrySettings.config).withTopic(topicName)
}
