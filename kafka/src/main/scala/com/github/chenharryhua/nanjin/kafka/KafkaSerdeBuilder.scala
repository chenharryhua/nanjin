package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.streaming.StateStores
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, SerdeOf}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ByteArrayKeyValueStore
import org.apache.kafka.streams.scala.kstream.{Consumed, Materialized, Produced}

final class KafkaSerdeBuilder private[kafka] (schemaRegistrySettings: SchemaRegistrySettings)
    extends Serializable {
  def asKey[K: SerdeOf]: Serde[K] = SerdeOf[K].asKey(schemaRegistrySettings.config).serde
  def asValue[V: SerdeOf]: Serde[V] = SerdeOf[V].asValue(schemaRegistrySettings.config).serde

  def asKey[K](avroCodec: AvroCodec[K]): Serde[K] =
    asKey(SerdeOf[K](avroCodec))
  def asValue[V](avroCodec: AvroCodec[V]): Serde[V] =
    asValue(SerdeOf[V](avroCodec))

  def consumed[K: SerdeOf, V: SerdeOf]: Consumed[K, V] = Consumed.`with`[K, V](asKey[K], asValue[V])
  def consumed[K, V](raw: RawKeyValueSerdePair[K, V]): Consumed[K, V] = consumed[K, V](raw.key, raw.value)

  def produced[K: SerdeOf, V: SerdeOf]: Produced[K, V] = Produced.`with`[K, V](asKey[K], asValue[V])
  def produced[K, V](raw: RawKeyValueSerdePair[K, V]): Produced[K, V] = produced[K, V](raw.key, raw.value)

  def store[K: SerdeOf, V: SerdeOf](storeName: TopicName): StateStores[K, V] =
    StateStores[K, V](
      RawKeyValueSerdePair[K, V](SerdeOf[K], SerdeOf[V]).register(schemaRegistrySettings, storeName))

  def store[K, V](topic: TopicDef[K, V]): StateStores[K, V] =
    store(topic.topicName)(topic.rawSerdes.key, topic.rawSerdes.value)

  def genericSerde[K: SerdeOf, V: SerdeOf](topicName: TopicName): KafkaGenericSerde[K, V] =
    new KafkaGenericSerde[K, V](
      SerdeOf[K].asKey(schemaRegistrySettings.config).withTopic(topicName),
      SerdeOf[V].asValue(schemaRegistrySettings.config).withTopic(topicName)) {}

  def materialized[K: SerdeOf, V: SerdeOf](topicName: TopicName): Materialized[K, V, ByteArrayKeyValueStore] =
    Materialized.as[K, V, ByteArrayKeyValueStore](topicName.value)(asKey[K], asValue[V])

}
