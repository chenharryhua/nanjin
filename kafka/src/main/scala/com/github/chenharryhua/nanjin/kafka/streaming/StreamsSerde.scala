package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaGenericSerde, SchemaRegistrySettings, SerdePair, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, SerdeOf}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}

final class StreamsSerde private[kafka](schemaRegistrySettings: SchemaRegistrySettings)
    extends Serializable {
  def asKey[K: SerdeOf]: Serde[K] = SerdeOf[K].asKey(schemaRegistrySettings.config).serde
  def asValue[V: SerdeOf]: Serde[V] = SerdeOf[V].asValue(schemaRegistrySettings.config).serde

  def asKey[K](avroCodec: AvroCodec[K]): Serde[K] = asKey(SerdeOf[K](avroCodec))
  def asValue[V](avroCodec: AvroCodec[V]): Serde[V] = asValue(SerdeOf[V](avroCodec))

  def consumed[K: SerdeOf, V: SerdeOf]: Consumed[K, V] = Consumed.`with`[K, V](asKey[K], asValue[V])
  def consumed[K, V](raw: SerdePair[K, V]): Consumed[K, V] = consumed[K, V](raw.key, raw.value)

  def produced[K: SerdeOf, V: SerdeOf]: Produced[K, V] = Produced.`with`[K, V](asKey[K], asValue[V])
  def produced[K, V](raw: SerdePair[K, V]): Produced[K, V] = produced[K, V](raw.key, raw.value)

  def store[K: SerdeOf, V: SerdeOf](storeName: TopicName): StateStores[K, V] =
    StateStores[K, V](SerdePair[K, V](SerdeOf[K], SerdeOf[V]).register(schemaRegistrySettings, storeName))

  def store[K, V](topic: TopicDef[K, V]): StateStores[K, V] =
    store(topic.topicName)(topic.serdePair.key, topic.serdePair.value)

  def serde[K: SerdeOf, V: SerdeOf](topicName: TopicName): KafkaGenericSerde[K, V] =
    new KafkaGenericSerde[K, V](
      SerdeOf[K].asKey(schemaRegistrySettings.config).withTopic(topicName),
      SerdeOf[V].asValue(schemaRegistrySettings.config).withTopic(topicName))

  def serde[K, V](topicDef: TopicDef[K, V]): KafkaGenericSerde[K, V] =
    serde[K, V](topicDef.topicName)(topicDef.serdePair.key, topicDef.serdePair.value)

}
