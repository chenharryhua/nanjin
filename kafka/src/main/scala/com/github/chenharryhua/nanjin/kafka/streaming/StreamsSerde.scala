package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, SchemaRegistrySettings}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroFor, KafkaSerde}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.processor.StateStore

final class StreamsSerde private[kafka] (schemaRegistrySettings: SchemaRegistrySettings) {
  private def asKey[K: AvroFor]: Serde[K] =
    AvroFor[K].asKey(schemaRegistrySettings.config).serde
  private def asValue[V: AvroFor]: Serde[V] =
    AvroFor[V].asValue(schemaRegistrySettings.config).serde

  def consumed[K: AvroFor, V: AvroFor]: Consumed[K, V] =
    Consumed.`with`[K, V](asKey[K], asValue[V])

  def produced[K: AvroFor, V: AvroFor]: Produced[K, V] =
    Produced.`with`[K, V](asKey[K], asValue[V])

  def joined[K: AvroFor, V: AvroFor, VO: AvroFor]: Joined[K, V, VO] =
    Joined.`with`(asKey[K], asValue[V], asValue[VO])

  def grouped[K: AvroFor, V: AvroFor]: Grouped[K, V] =
    Grouped.`with`(asKey[K], asValue[V])

  def repartitioned[K: AvroFor, V: AvroFor]: Repartitioned[K, V] =
    Repartitioned.`with`(asKey[K], asValue[V])

  def streamJoined[K: AvroFor, V: AvroFor, VO: AvroFor]: StreamJoined[K, V, VO] =
    StreamJoined.`with`(asKey[K], asValue[V], asValue[VO])

  def materialized[K: AvroFor, V: AvroFor, S <: StateStore]: Materialized[K, V, S] =
    Materialized.`with`[K, V, S](asKey[K], asValue[V])

  def store[K: AvroFor, V: AvroFor](storeName: TopicName): StateStores[K, V] =
    StateStores[K, V](AvroTopic[K, V](AvroFor[K], AvroFor[V], storeName).register(schemaRegistrySettings))

  def store[K, V](topic: AvroTopic[K, V]): StateStores[K, V] =
    store[K, V](topic.topicName)(using topic.pair.key, topic.pair.value)

  def keySerde[K: AvroFor](topicName: TopicName): KafkaSerde[K] =
    AvroFor[K].asKey(schemaRegistrySettings.config).withTopic(topicName.value)

  def valueSerde[V: AvroFor](topicName: TopicName): KafkaSerde[V] =
    AvroFor[V].asValue(schemaRegistrySettings.config).withTopic(topicName.value)
}
