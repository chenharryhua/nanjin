package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.kafka.RegisteredSerdePair
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.*

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

final class KeyValueBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: KeyValueBytesStoreSupplier,
  registered: RegisteredSerdePair[K, V]) {
  def keyValueStoreBuilder: StoreBuilder[KeyValueStore[K, V]] =
    Stores.keyValueStoreBuilder(supplier, registered.key.serde, registered.value.serde)

  def timestampedKeyValueStoreBuilder: StoreBuilder[TimestampedKeyValueStore[K, V]] =
    Stores.timestampedKeyValueStoreBuilder(supplier, registered.key.serde, registered.value.serde)
}

final class WindowBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: WindowBytesStoreSupplier,
  registered: RegisteredSerdePair[K, V]) {
  def windowStoreBuilder: StoreBuilder[WindowStore[K, V]] =
    Stores.windowStoreBuilder(supplier, registered.key.serde, registered.value.serde)

  def timestampedWindowStoreBuilder: StoreBuilder[TimestampedWindowStore[K, V]] =
    Stores.timestampedWindowStoreBuilder(supplier, registered.key.serde, registered.value.serde)
}

final class SessionBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: SessionBytesStoreSupplier,
  registered: RegisteredSerdePair[K, V]) {
  def sessionStoreBuilder: StoreBuilder[SessionStore[K, V]] =
    Stores.sessionStoreBuilder(supplier, registered.key.serde, registered.value.serde)
}

final class StateStores[K, V] private (registered: RegisteredSerdePair[K, V]) extends Serializable {

  val name: String = registered.name.value

  def persistentKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.persistentKeyValueStore(name), registered)

  def persistentTimestampedKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.persistentTimestampedKeyValueStore(name), registered)

  def inMemoryKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.inMemoryKeyValueStore(name), registered)

  def lruMap(maxCacheSize: Int): KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.lruMap(name, maxCacheSize), registered)

  def persistentWindowStore(
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    new WindowBytesStoreSupplierHelper(
      Stores.persistentWindowStore(name, retentionPeriod, windowSize, retainDuplicates),
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
      Stores.persistentTimestampedWindowStore(name, retentionPeriod, windowSize, retainDuplicates),
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
      Stores.inMemoryWindowStore(name, retentionPeriod, windowSize, retainDuplicates),
      registered)

  def inMemoryWindowStore(
    retentionPeriod: FiniteDuration,
    windowSize: FiniteDuration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    inMemoryWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

  def persistentSessionStore(retentionPeriod: Duration): SessionBytesStoreSupplierHelper[K, V] =
    new SessionBytesStoreSupplierHelper(Stores.persistentSessionStore(name, retentionPeriod), registered)

  def inMemorySessionStore(retentionPeriod: Duration): SessionBytesStoreSupplierHelper[K, V] =
    new SessionBytesStoreSupplierHelper(Stores.inMemorySessionStore(name, retentionPeriod), registered)

  def inMemorySessionStore(retentionPeriod: FiniteDuration): SessionBytesStoreSupplierHelper[K, V] =
    inMemorySessionStore(retentionPeriod.toJava)

  object query {
    def keyValueStore: StoreQueryParameters[ReadOnlyKeyValueStore[K, V]] =
      StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.keyValueStore[K, V])

    def timestampedKeyValueStore: StoreQueryParameters[ReadOnlyKeyValueStore[K, ValueAndTimestamp[V]]] =
      StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.timestampedKeyValueStore[K, V])

    def windowStore: StoreQueryParameters[ReadOnlyWindowStore[K, V]] =
      StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.windowStore[K, V])

    def timestampedWindowStore: StoreQueryParameters[ReadOnlyWindowStore[K, ValueAndTimestamp[V]]] =
      StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.timestampedWindowStore[K, V])

    def sessionStore: StoreQueryParameters[ReadOnlySessionStore[K, V]] =
      StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.sessionStore[K, V])
  }
}

private[kafka] object StateStores {
  def apply[K, V](registered: RegisteredSerdePair[K, V]): StateStores[K, V] =
    new StateStores[K, V](registered)
}
