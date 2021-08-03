package com.github.chenharryhua.nanjin.kafka.streaming

import cats.data.Reader
import com.github.chenharryhua.nanjin.kafka.TopicDef
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.ByteArrayKeyValueStore
import org.apache.kafka.streams.scala.kstream.{Consumed, Materialized}
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier

final class StreamingChannel[K, V] private[kafka] (topicDef: TopicDef[K, V]) {
  import org.apache.kafka.streams.scala.StreamsBuilder
  import org.apache.kafka.streams.scala.kstream.{KStream, KTable}

  private val consumed: Consumed[K, V] = Consumed.`with`(topicDef.serdeOfKey, topicDef.serdeOfVal)

  val kstream: Reader[StreamsBuilder, KStream[K, V]] =
    Reader(builder => builder.stream[K, V](topicDef.topicName.value)(consumed))

  val ktable: Reader[StreamsBuilder, KTable[K, V]] =
    Reader(builder => builder.table[K, V](topicDef.topicName.value)(consumed))

  def ktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, KTable[K, V]] =
    Reader(builder => builder.table[K, V](topicDef.topicName.value, mat)(consumed))

  def ktable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, KTable[K, V]] =
    ktable(Materialized.as[K, V](supplier)(topicDef.serdeOfKey, topicDef.serdeOfVal))

  val gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder => builder.globalTable[K, V](topicDef.topicName.value)(consumed))

  def gktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder => builder.globalTable[K, V](topicDef.topicName.value, mat)(consumed))

  def gktable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, GlobalKTable[K, V]] =
    gktable(Materialized.as[K, V](supplier)(topicDef.serdeOfKey, topicDef.serdeOfVal))
}
