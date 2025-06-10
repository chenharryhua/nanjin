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
import org.apache.kafka.streams.scala.kstream.*

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

  def joined[K: AvroCodecOf, V: AvroCodecOf, VO: AvroCodecOf]: Joined[K, V, VO] =
    Joined.`with`(asKey[K], asValue[V], asValue[VO])

  def grouped[K: AvroCodecOf, V: AvroCodecOf]: Grouped[K, V] = Grouped.`with`(asKey[K], asValue[V])

  def repartitioned[K: AvroCodecOf, V: AvroCodecOf]: Repartitioned[K, V] =
    Repartitioned.`with`(asKey[K], asValue[V])

  def streamJoined[K: AvroCodecOf, V: AvroCodecOf, VO: AvroCodecOf]: StreamJoined[K, V, VO] =
    StreamJoined.`with`(asKey[K], asValue[V], asValue[VO])

  def store[K: AvroCodecOf, V: AvroCodecOf](storeName: TopicName): StateStores[K, V] =
    StateStores[K, V](
      AvroCodecPair[K, V](AvroCodecOf[K], AvroCodecOf[V]).register(schemaRegistrySettings, storeName))

  def store[K, V](topic: TopicDef[K, V]): StateStores[K, V] =
    store(topic.topicName)(topic.codecPair.key, topic.codecPair.value)

  def serde[K, V](topicDef: TopicDef[K, V]): KafkaGenericSerde[K, V] = {
    val pair = topicDef.codecPair.register(schemaRegistrySettings, topicDef.topicName)
    new KafkaGenericSerde[K, V](pair.key, pair.value)
  }

  def serde[K: AvroCodecOf, V: AvroCodecOf](topicName: TopicName): KafkaGenericSerde[K, V] =
    serde[K, V](TopicDef(topicName)(AvroCodecOf[K], AvroCodecOf[V]))

}
