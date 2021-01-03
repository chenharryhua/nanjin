package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.kstream.{Consumed, KTable, Materialized}
import org.apache.kafka.streams.scala.{
  ByteArrayKeyValueStore,
  ByteArraySessionStore,
  ByteArrayWindowStore,
  StreamsBuilder
}
import org.apache.kafka.streams.state._

import java.time.Duration

final class KafkaStateStore[K, V] private[kafka] (storeName: StoreName, keySerde: SerdeOf[K], valSerde: SerdeOf[V]) {

  def materialized[S <: StateStore]: Materialized[K, V, S] =
    Materialized.as[K, V, S](storeName.value)(keySerde, valSerde)

  def materialized(supplier: WindowBytesStoreSupplier): Materialized[K, V, ByteArrayWindowStore] =
    Materialized.as[K, V](supplier)(keySerde, valSerde)

  def materialized(supplier: SessionBytesStoreSupplier): Materialized[K, V, ByteArraySessionStore] =
    Materialized.as[K, V](supplier)(keySerde, valSerde)

  def materialized(supplier: KeyValueBytesStoreSupplier): Materialized[K, V, ByteArrayKeyValueStore] =
    Materialized.as[K, V](supplier)(keySerde, valSerde)

  def table: Reader[StreamsBuilder, KTable[K, V]] =
    Reader[StreamsBuilder, KTable[K, V]](_.table[K, V](storeName.value)(Consumed.`with`(keySerde, valSerde)))

  def table(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, KTable[K, V]] =
    Reader[StreamsBuilder, KTable[K, V]](
      _.table[K, V](storeName.value, materialized(supplier))(Consumed.`with`(keySerde, valSerde)))

  def gtable: Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader[StreamsBuilder, GlobalKTable[K, V]](_.globalTable(storeName.value)(Consumed.`with`(keySerde, valSerde)))

  def gtable(supplier: KeyValueBytesStoreSupplier): Reader[StreamsBuilder, GlobalKTable[K, V]] =
    Reader[StreamsBuilder, GlobalKTable[K, V]](
      _.globalTable(storeName.value, materialized(supplier))(Consumed.`with`(keySerde, valSerde)))

  def inMemoryKeyValue: StoreBuilder[KeyValueStore[K, V]] =
    Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName.value), keySerde, valSerde)

  def persistKeyValue: StoreBuilder[KeyValueStore[K, V]] =
    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName.value), keySerde, valSerde)

  def inMemoryWindow(
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean): StoreBuilder[WindowStore[K, V]] =
    Stores.windowStoreBuilder(
      Stores.inMemoryWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates),
      keySerde,
      valSerde)

  def persistWindow(
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean): StoreBuilder[WindowStore[K, V]] =
    Stores.windowStoreBuilder(
      Stores.persistentWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates),
      keySerde,
      valSerde)
}

object KafkaStateStore {

  def apply[K: SerdeOf, V: SerdeOf](storeName: StoreName): KafkaStateStore[K, V] =
    new KafkaStateStore[K, V](storeName, SerdeOf[K], SerdeOf[V])
}
