package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.common.kafka.{StoreName, TopicName}
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.*

import java.time.Duration
import scala.compat.java8.DurationConverters.FiniteDurationops
import scala.concurrent.duration.FiniteDuration

final class KeyValueBytesStoreSupplierHelper[K, V](
  supplier: KeyValueBytesStoreSupplier,
  serdeOfKey: SerdeOf[K],
  serdeOfVal: SerdeOf[V]) {
  def keyValueStoreBuilder: StoreBuilder[KeyValueStore[K, V]] =
    Stores.keyValueStoreBuilder(supplier, serdeOfKey, serdeOfVal)

  def timestampedKeyValueStoreBuilder: StoreBuilder[TimestampedKeyValueStore[K, V]] =
    Stores.timestampedKeyValueStoreBuilder(supplier, serdeOfKey, serdeOfVal)
}

final class WindowBytesStoreSupplierHelper[K, V](
  supplier: WindowBytesStoreSupplier,
  serdeOfKey: SerdeOf[K],
  serdeOfVal: SerdeOf[V]) {
  def windowStoreBuilder: StoreBuilder[WindowStore[K, V]] =
    Stores.windowStoreBuilder(supplier, serdeOfKey, serdeOfVal)

  def timestampedWindowStoreBuilder: StoreBuilder[TimestampedWindowStore[K, V]] =
    Stores.timestampedWindowStoreBuilder(supplier, serdeOfKey, serdeOfVal)
}

final class SessionBytesStoreSupplierHelper[K, V](
  supplier: SessionBytesStoreSupplier,
  serdeOfKey: SerdeOf[K],
  serdeOfVal: SerdeOf[V]) {
  def sessionStoreBuilder: StoreBuilder[SessionStore[K, V]] =
    Stores.sessionStoreBuilder(supplier, serdeOfKey, serdeOfVal)
}

final class StateStore[K, V] private (storeName: StoreName, serdeOfKey: SerdeOf[K], serdeOfVal: SerdeOf[V])
    extends Serializable {

  def name: String = storeName.value

  def asTopicDef(name: String): TopicDef[K, V] = TopicDef[K, V](TopicName.unsafeFrom(name))(serdeOfKey, serdeOfVal)

  object supplier {

    def persistentKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
      new KeyValueBytesStoreSupplierHelper(Stores.persistentKeyValueStore(storeName.value), serdeOfKey, serdeOfVal)

    def persistentTimestampedKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
      new KeyValueBytesStoreSupplierHelper(
        Stores.persistentTimestampedKeyValueStore(storeName.value),
        serdeOfKey,
        serdeOfVal)

    def inMemoryKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
      new KeyValueBytesStoreSupplierHelper(Stores.inMemoryKeyValueStore(storeName.value), serdeOfKey, serdeOfVal)

    def lruMap(maxCacheSize: Int): KeyValueBytesStoreSupplierHelper[K, V] =
      new KeyValueBytesStoreSupplierHelper(Stores.lruMap(storeName.value, maxCacheSize), serdeOfKey, serdeOfVal)

    def persistentWindowStore(
      retentionPeriod: Duration,
      windowSize: Duration,
      retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
      new WindowBytesStoreSupplierHelper(
        Stores.persistentWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates),
        serdeOfKey,
        serdeOfVal)

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
        serdeOfKey,
        serdeOfVal)

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
        serdeOfKey,
        serdeOfVal)

    def inMemoryWindowStore(
      retentionPeriod: FiniteDuration,
      windowSize: FiniteDuration,
      retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
      inMemoryWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

    def persistentSessionStore(retentionPeriod: Duration): SessionBytesStoreSupplierHelper[K, V] =
      new SessionBytesStoreSupplierHelper(
        Stores.persistentSessionStore(storeName.value, retentionPeriod),
        serdeOfKey,
        serdeOfVal)

    def inMemorySessionStore(retentionPeriod: Duration): SessionBytesStoreSupplierHelper[K, V] =
      new SessionBytesStoreSupplierHelper(
        Stores.inMemorySessionStore(storeName.value, retentionPeriod),
        serdeOfKey,
        serdeOfVal)

    def inMemorySessionStore(retentionPeriod: FiniteDuration): SessionBytesStoreSupplierHelper[K, V] =
      inMemorySessionStore(retentionPeriod.toJava)
  }

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

object StateStore {

  def apply[K, V](name: String)(implicit serdeOfKey: SerdeOf[K], serdeOfVal: SerdeOf[V]): StateStore[K, V] =
    new StateStore[K, V](StoreName.unsafeFrom(name), serdeOfKey, serdeOfVal)
}
