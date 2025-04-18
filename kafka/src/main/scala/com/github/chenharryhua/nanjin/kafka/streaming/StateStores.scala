package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KeyValueSerdePair, RawKeyValueSerdePair, SchemaRegistrySettings}
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.*

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

final class KeyValueBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: KeyValueBytesStoreSupplier,
  registered: KeyValueSerdePair[K, V]) {
  def keyValueStoreBuilder: StoreBuilder[KeyValueStore[K, V]] =
    Stores.keyValueStoreBuilder(supplier, registered.key.serde, registered.value.serde)

  def timestampedKeyValueStoreBuilder: StoreBuilder[TimestampedKeyValueStore[K, V]] =
    Stores.timestampedKeyValueStoreBuilder(supplier, registered.key.serde, registered.value.serde)
}

final class WindowBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: WindowBytesStoreSupplier,
  registered: KeyValueSerdePair[K, V]) {
  def windowStoreBuilder: StoreBuilder[WindowStore[K, V]] =
    Stores.windowStoreBuilder(supplier, registered.key.serde, registered.value.serde)

  def timestampedWindowStoreBuilder: StoreBuilder[TimestampedWindowStore[K, V]] =
    Stores.timestampedWindowStoreBuilder(supplier, registered.key.serde, registered.value.serde)
}

final class SessionBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: SessionBytesStoreSupplier,
  registered: KeyValueSerdePair[K, V]) {
  def sessionStoreBuilder: StoreBuilder[SessionStore[K, V]] =
    Stores.sessionStoreBuilder(supplier, registered.key.serde, registered.value.serde)
}

final class StateStores[K, V] private (storeName: TopicName, registered: KeyValueSerdePair[K, V])
    extends Serializable {

  def name: String = storeName.value

  def persistentKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.persistentKeyValueStore(storeName.value), registered)

  def persistentTimestampedKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(
      Stores.persistentTimestampedKeyValueStore(storeName.value),
      registered)

  def inMemoryKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.inMemoryKeyValueStore(storeName.value), registered)

  def lruMap(maxCacheSize: Int): KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.lruMap(storeName.value, maxCacheSize), registered)

  def persistentWindowStore(
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    new WindowBytesStoreSupplierHelper(
      Stores.persistentWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates),
      registered)

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
      registered)

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
      registered)

  def inMemoryWindowStore(
    retentionPeriod: FiniteDuration,
    windowSize: FiniteDuration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    inMemoryWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

  def persistentSessionStore(retentionPeriod: Duration): SessionBytesStoreSupplierHelper[K, V] =
    new SessionBytesStoreSupplierHelper(
      Stores.persistentSessionStore(storeName.value, retentionPeriod),
      registered)

  def inMemorySessionStore(retentionPeriod: Duration): SessionBytesStoreSupplierHelper[K, V] =
    new SessionBytesStoreSupplierHelper(
      Stores.inMemorySessionStore(storeName.value, retentionPeriod),
      registered)

  def inMemorySessionStore(retentionPeriod: FiniteDuration): SessionBytesStoreSupplierHelper[K, V] =
    inMemorySessionStore(retentionPeriod.toJava)

  object query {
    def keyValueStore: StoreQueryParameters[ReadOnlyKeyValueStore[K, V]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.keyValueStore[K, V])

    def timestampedKeyValueStore: StoreQueryParameters[ReadOnlyKeyValueStore[K, ValueAndTimestamp[V]]] =
      StoreQueryParameters.fromNameAndType(
        storeName.value,
        QueryableStoreTypes.timestampedKeyValueStore[K, V])

    def windowStore: StoreQueryParameters[ReadOnlyWindowStore[K, V]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.windowStore[K, V])

    def timestampedWindowStore: StoreQueryParameters[ReadOnlyWindowStore[K, ValueAndTimestamp[V]]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.timestampedWindowStore[K, V])

    def sessionStore: StoreQueryParameters[ReadOnlySessionStore[K, V]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.sessionStore[K, V])
  }
}

private[kafka] object StateStores {
  def apply[K, V](storeName: TopicName, registered: KeyValueSerdePair[K, V]): StateStores[K, V] =
    new StateStores[K, V](storeName, registered)

  def apply[K, V](
    storeName: TopicName,
    srs: SchemaRegistrySettings,
    rawSerde: RawKeyValueSerdePair[K, V]): StateStores[K, V] =
    apply[K, V](storeName, rawSerde.register(srs, storeName))
}
