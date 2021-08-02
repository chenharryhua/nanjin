package com.github.chenharryhua.nanjin.kafka.streaming

import cats.data.Reader
import com.github.chenharryhua.nanjin.kafka.TopicDef
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.ByteArrayKeyValueStore
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier

final class StreamingChannel[K, V] private[kafka] (topicDef: TopicDef[K, V]) {
  import org.apache.kafka.streams.scala.StreamsBuilder
  import org.apache.kafka.streams.scala.kstream.{KStream, KTable}

  val kstream: Reader[StreamsBuilder, KStream[K, V]] =
    Reader(builder => builder.stream[K, V](topicDef.topicName.value)(topicDef.consumed))

  val ktable: Reader[StreamsBuilder, KTable[K, V]] =
    Reader(builder => builder.table[K, V](topicDef.topicName.value)(topicDef.consumed))

  def ktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, KTable[K, V]] =
    Reader(builder => builder.table[K, V](topicDef.topicName.value, mat)(topicDef.consumed))

  def ktable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, KTable[K, V]] = {
    val mat: Materialized[K, V, ByteArrayKeyValueStore] =
      Materialized.as[K, V](supplier)(topicDef.serdeOfKey, topicDef.serdeOfVal)
    Reader((builder: StreamsBuilder) => builder.table[K, V](topicDef.topicName.value, mat)(topicDef.consumed))
  }

  val gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder => builder.globalTable[K, V](topicDef.topicName.value)(topicDef.consumed))

  def gktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder => builder.globalTable[K, V](topicDef.topicName.value, mat)(topicDef.consumed))

  def gktable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, GlobalKTable[K, V]] = {
    val mat: Materialized[K, V, ByteArrayKeyValueStore] =
      Materialized.as[K, V](supplier)(topicDef.serdeOfKey, topicDef.serdeOfVal)
    Reader((builder: StreamsBuilder) => builder.globalTable[K, V](topicDef.topicName.value, mat)(topicDef.consumed))
  }
}
