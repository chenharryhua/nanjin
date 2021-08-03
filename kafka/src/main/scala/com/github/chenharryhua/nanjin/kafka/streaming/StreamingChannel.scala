package com.github.chenharryhua.nanjin.kafka.streaming

import cats.data.Reader
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.*
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier

final class StreamingChannel[F[_], K, V] private[kafka] (topic: KafkaTopic[F, K, V]) {

  val consumed: Consumed[K, V] = Consumed.`with`[K, V](topic.codec.keySerde, topic.codec.valSerde)
  val produced: Produced[K, V] = Produced.`with`[K, V](topic.codec.keySerde, topic.codec.valSerde)

  val kstream: Reader[StreamsBuilder, KStream[K, V]] =
    Reader(builder => builder.stream[K, V](topic.topicName.value)(consumed))

  val ktable: Reader[StreamsBuilder, KTable[K, V]] =
    Reader(builder => builder.table[K, V](topic.topicName.value)(consumed))

  def ktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, KTable[K, V]] =
    Reader(builder => builder.table[K, V](topic.topicName.value, mat)(consumed))

  def ktable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, KTable[K, V]] =
    ktable(Materialized.as[K, V](supplier)(topic.codec.keySerde, topic.codec.valSerde))

  val gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder => builder.globalTable[K, V](topic.topicName.value)(consumed))

  def gktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder => builder.globalTable[K, V](topic.topicName.value, mat)(consumed))

  def gktable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, GlobalKTable[K, V]] =
    gktable(Materialized.as[K, V](supplier)(topic.codec.keySerde, topic.codec.valSerde))
}
