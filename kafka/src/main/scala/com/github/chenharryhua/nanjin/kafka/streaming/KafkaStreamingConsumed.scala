package com.github.chenharryhua.nanjin.kafka.streaming

import cats.data.Reader
import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, RegisteredKeyValueSerdePair}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.scala.kstream.*
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, StateSerdes}

final class KafkaStreamingConsumed[F[_], K, V] private[kafka] (topic: KafkaTopic[F, K, V], consumed: Consumed[K, V])
    extends Consumed[K, V](consumed) {

  val serdeVal: Serde[V]   = valueSerde
  val serdeKey: Serde[K]   = keySerde
  val topicName: TopicName = topic.topicName

  def withTopicName(topicName: String): KafkaStreamingConsumed[F, K, V] =
    new KafkaStreamingConsumed[F, K, V](topic.withTopicName(topicName), consumed)

  private def update(consumed: Consumed[K, V]) = new KafkaStreamingConsumed[F, K, V](topic, consumed)

  override def withOffsetResetPolicy(resetPolicy: Topology.AutoOffsetReset): KafkaStreamingConsumed[F, K, V] =
    update(consumed.withOffsetResetPolicy(resetPolicy))

  override def withName(processorName: String): KafkaStreamingConsumed[F, K, V] =
    update(consumed.withName(processorName))

  override def withTimestampExtractor(timestampExtractor: TimestampExtractor): KafkaStreamingConsumed[F, K, V] =
    update(consumed.withTimestampExtractor(timestampExtractor))

  override def withKeySerde(keySerde: Serde[K]): KafkaStreamingConsumed[F, K, V] =
    update(consumed.withKeySerde(keySerde))

  override def withValueSerde(valueSerde: Serde[V]): KafkaStreamingConsumed[F, K, V] =
    update(consumed.withValueSerde(valueSerde))

  val stateSerdes: StateSerdes[K, V] =
    new StateSerdes[K, V](topic.topicName.value, keySerde, valueSerde)

  def asStateStore(storeName: String): NJStateStore[K, V] = {
    require(storeName =!= topic.topicName.value, "should provide a name other than the topic name")
    NJStateStore[K, V](storeName, RegisteredKeyValueSerdePair(keySerde, valueSerde))
  }

  val kstream: Reader[StreamsBuilder, KStream[K, V]] =
    Reader(builder => builder.stream[K, V](topic.topicName.value)(this))

  val ktable: Reader[StreamsBuilder, KTable[K, V]] =
    Reader(builder => builder.table[K, V](topic.topicName.value)(this))

  def ktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, KTable[K, V]] =
    Reader(builder => builder.table[K, V](topic.topicName.value, mat)(this))

  def ktable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, KTable[K, V]] =
    ktable(Materialized.as[K, V](supplier)(keySerde, valueSerde))

  val gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder => builder.globalTable[K, V](topic.topicName.value)(this))

  def gktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder => builder.globalTable[K, V](topic.topicName.value, mat)(this))

  def gktable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, GlobalKTable[K, V]] =
    gktable(Materialized.as[K, V](supplier)(keySerde, valueSerde))
}
