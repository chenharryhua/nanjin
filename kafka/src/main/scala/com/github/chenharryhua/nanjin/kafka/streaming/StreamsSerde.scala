package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{
  AvroCodecPair,
  KafkaGenericSerde,
  SchemaRegistrySettings,
  TopicDef
}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, AvroCodecOf}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}

final class StreamsSerde private[kafka] (schemaRegistrySettings: SchemaRegistrySettings)
    extends Serializable {
  def asKey[K: AvroCodecOf]: Serde[K] = AvroCodecOf[K].asKey(schemaRegistrySettings.config).serde
  def asValue[V: AvroCodecOf]: Serde[V] = AvroCodecOf[V].asValue(schemaRegistrySettings.config).serde

  def asKey[K](avroCodec: AvroCodec[K]): Serde[K] = asKey(AvroCodecOf[K](avroCodec))
  def asValue[V](avroCodec: AvroCodec[V]): Serde[V] = asValue(AvroCodecOf[V](avroCodec))

  def consumed[K: AvroCodecOf, V: AvroCodecOf]: Consumed[K, V] = Consumed.`with`[K, V](asKey[K], asValue[V])
  def consumed[K, V](pair: AvroCodecPair[K, V]): Consumed[K, V] = consumed[K, V](pair.key, pair.value)

  def produced[K: AvroCodecOf, V: AvroCodecOf]: Produced[K, V] = Produced.`with`[K, V](asKey[K], asValue[V])
  def produced[K, V](pair: AvroCodecPair[K, V]): Produced[K, V] = produced[K, V](pair.key, pair.value)

  def store[K: AvroCodecOf, V: AvroCodecOf](storeName: TopicName): StateStores[K, V] =
    StateStores[K, V](
      AvroCodecPair[K, V](AvroCodecOf[K], AvroCodecOf[V]).register(schemaRegistrySettings, storeName))

  def store[K, V](topic: TopicDef[K, V]): StateStores[K, V] =
    store(topic.topicName)(topic.codecPair.key, topic.codecPair.value)

  def serde[K: AvroCodecOf, V: AvroCodecOf](topicName: TopicName): KafkaGenericSerde[K, V] =
    new KafkaGenericSerde[K, V](
      AvroCodecOf[K].asKey(schemaRegistrySettings.config).withTopic(topicName),
      AvroCodecOf[V].asValue(schemaRegistrySettings.config).withTopic(topicName))

  def serde[K, V](topicDef: TopicDef[K, V]): KafkaGenericSerde[K, V] =
    serde[K, V](topicDef.topicName)(topicDef.codecPair.key, topicDef.codecPair.value)

}
