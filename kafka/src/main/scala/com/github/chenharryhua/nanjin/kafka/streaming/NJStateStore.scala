package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.common.kafka.StoreName
import com.github.chenharryhua.nanjin.kafka.{
  RawKeyValueSerdePair,
  RegisteredKeyValueSerdePair,
  SchemaRegistrySettings,
  TopicDef
}
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.*

import java.time.Duration
import scala.compat.java8.DurationConverters.FiniteDurationops
import scala.concurrent.duration.FiniteDuration

final class KeyValueBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: KeyValueBytesStoreSupplier,
  codec: RegisteredKeyValueSerdePair[K, V]) {
  def keyValueStoreBuilder: StoreBuilder[KeyValueStore[K, V]] =
    Stores.keyValueStoreBuilder(supplier, codec.keySerde, codec.valSerde)

  def timestampedKeyValueStoreBuilder: StoreBuilder[TimestampedKeyValueStore[K, V]] =
    Stores.timestampedKeyValueStoreBuilder(supplier, codec.keySerde, codec.valSerde)
}

final class WindowBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: WindowBytesStoreSupplier,
  codec: RegisteredKeyValueSerdePair[K, V]) {
  def windowStoreBuilder: StoreBuilder[WindowStore[K, V]] =
    Stores.windowStoreBuilder(supplier, codec.keySerde, codec.valSerde)

  def timestampedWindowStoreBuilder: StoreBuilder[TimestampedWindowStore[K, V]] =
    Stores.timestampedWindowStoreBuilder(supplier, codec.keySerde, codec.valSerde)
}

final class SessionBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: SessionBytesStoreSupplier,
  codec: RegisteredKeyValueSerdePair[K, V]) {
  def sessionStoreBuilder: StoreBuilder[SessionStore[K, V]] =
    Stores.sessionStoreBuilder(supplier, codec.keySerde, codec.valSerde)
}

final class NJStateStore[K, V] private (storeName: StoreName, codec: RegisteredKeyValueSerdePair[K, V])
    extends Serializable {

  def name: String = storeName.value

  def persistentKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.persistentKeyValueStore(storeName.value), codec)

  def persistentTimestampedKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.persistentTimestampedKeyValueStore(storeName.value), codec)

  def inMemoryKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.inMemoryKeyValueStore(storeName.value), codec)

  def lruMap(maxCacheSize: Int): KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.lruMap(storeName.value, maxCacheSize), codec)

  def persistentWindowStore(
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    new WindowBytesStoreSupplierHelper(
      Stores.persistentWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates),
      codec)

  def persistentWindowStore(
    retentionPeriod: FiniteDuration,
    windowSize: FiniteDuration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    persistentWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

  def persistentTimestampedWindowStore(
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    new WindowBytesStoreSupplierHelper(
      Stores.persistentTimestampedWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates),
      codec)

  def persistentTimestampedWindowStore(
    retentionPeriod: FiniteDuration,
    windowSize: FiniteDuration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    persistentTimestampedWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

  def inMemoryWindowStore(
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    new WindowBytesStoreSupplierHelper(
      Stores.inMemoryWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates),
      codec)

  def inMemoryWindowStore(
    retentionPeriod: FiniteDuration,
    windowSize: FiniteDuration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    inMemoryWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

  def persistentSessionStore(retentionPeriod: Duration): SessionBytesStoreSupplierHelper[K, V] =
    new SessionBytesStoreSupplierHelper(Stores.persistentSessionStore(storeName.value, retentionPeriod), codec)

  def inMemorySessionStore(retentionPeriod: Duration): SessionBytesStoreSupplierHelper[K, V] =
    new SessionBytesStoreSupplierHelper(Stores.inMemorySessionStore(storeName.value, retentionPeriod), codec)

  def inMemorySessionStore(retentionPeriod: FiniteDuration): SessionBytesStoreSupplierHelper[K, V] =
    inMemorySessionStore(retentionPeriod.toJava)

  object query {
    def keyValueStore: StoreQueryParameters[ReadOnlyKeyValueStore[K, V]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.keyValueStore[K, V])

    def timestampedKeyValueStore: StoreQueryParameters[ReadOnlyKeyValueStore[K, ValueAndTimestamp[V]]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.timestampedKeyValueStore[K, V])

    def windowStore: StoreQueryParameters[ReadOnlyWindowStore[K, V]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.windowStore[K, V])

    def timestampedWindowStore: StoreQueryParameters[ReadOnlyWindowStore[K, ValueAndTimestamp[V]]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.timestampedWindowStore[K, V])

    def sessionStore: StoreQueryParameters[ReadOnlySessionStore[K, V]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.sessionStore[K, V])
  }
}

object NJStateStore {
  def apply[K, V](name: String, codec: RegisteredKeyValueSerdePair[K, V]): NJStateStore[K, V] =
    new NJStateStore[K, V](StoreName.unsafeFrom(name), codec)

  def apply[K, V](name: String, srs: SchemaRegistrySettings, serdes: RawKeyValueSerdePair[K, V]) =
    new NJStateStore[K, V](StoreName.unsafeFrom(name), serdes.register(srs, name))

}
