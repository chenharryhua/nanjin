package com.github.chenharryhua.nanjin.kafka.streaming

import cats.data.{Cont, Reader}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.RegisteredSerdePair
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.scala.kstream.*
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, StateSerdes}

final class KafkaStreamingConsumer[F[_], K, V] private[kafka] (
  topicName: TopicName,
  serdePair: RegisteredSerdePair[K, V],
  resetPolicy: Option[Topology.AutoOffsetReset],
  processorName: Option[String],
  timestampExtractor: Option[TimestampExtractor]) {

  private def copy(
    resetPolicy: Option[Topology.AutoOffsetReset] = resetPolicy,
    processorName: Option[String] = processorName,
    timestampExtractor: Option[TimestampExtractor] = timestampExtractor): KafkaStreamingConsumer[F, K, V] =
    new KafkaStreamingConsumer[F, K, V](topicName, serdePair, resetPolicy, processorName, timestampExtractor)

  def withOffsetResetPolicy(resetPolicy: Topology.AutoOffsetReset): KafkaStreamingConsumer[F, K, V] =
    copy(resetPolicy = Some(resetPolicy))

  def withProcessorName(processorName: String): KafkaStreamingConsumer[F, K, V] =
    copy(processorName = Some(processorName))

  def withTimestampExtractor(timestampExtractor: TimestampExtractor): KafkaStreamingConsumer[F, K, V] =
    copy(timestampExtractor = Some(timestampExtractor))

  def stateSerdes: StateSerdes[K, V] =
    new StateSerdes[K, V](topicName.value, serdePair.key.serde, serdePair.value.serde)

  private lazy val consumed: Consumed[K, V] =
    Cont
      .pure(Consumed.`with`[K, V](serdePair.key.serde, serdePair.value.serde))
      .map(c => resetPolicy.fold(c)(c.withOffsetResetPolicy))
      .map(c => timestampExtractor.fold(c)(c.withTimestampExtractor))
      .map(c => processorName.fold(c)(c.withName))
      .eval
      .value

  def kstream: Reader[StreamsBuilder, KStream[K, V]] =
    Reader(builder => builder.stream[K, V](topicName.value)(consumed))

  def ktable: Reader[StreamsBuilder, KTable[K, V]] =
    Reader(builder => builder.table[K, V](topicName.value)(consumed))

  def ktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, KTable[K, V]] =
    Reader(builder => builder.table[K, V](topicName.value, mat)(consumed))

  def ktable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, KTable[K, V]] =
    ktable(Materialized.as[K, V](supplier)(serdePair.key.serde, serdePair.value.serde))

  def gktable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder => builder.globalTable[K, V](topicName.value)(consumed))

  def gktable(mat: Materialized[K, V, ByteArrayKeyValueStore]): Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader(builder => builder.globalTable[K, V](topicName.value, mat)(consumed))

  def gktable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, GlobalKTable[K, V]] =
    gktable(Materialized.as[K, V](supplier)(serdePair.key.serde, serdePair.value.serde))
}
