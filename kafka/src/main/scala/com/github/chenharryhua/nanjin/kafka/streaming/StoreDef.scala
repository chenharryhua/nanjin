package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.common.kafka.StoreName
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.*

import java.time.Duration
import scala.compat.java8.DurationConverters.FiniteDurationops
import scala.concurrent.duration.FiniteDuration

final class StoreDef[K, V] private (val storeName: StoreName)(implicit
  val serdeOfKey: SerdeOf[K],
  val serdeOfVal: SerdeOf[V])
    extends Serializable {

  object supplier {
    def persistent: KeyValueBytesStoreSupplier            = Stores.persistentKeyValueStore(storeName.value)
    def persistentTimestamped: KeyValueBytesStoreSupplier = Stores.persistentTimestampedKeyValueStore(storeName.value)
    def inMemory: KeyValueBytesStoreSupplier              = Stores.inMemoryKeyValueStore(storeName.value)
    def lruMap(maxCacheSize: Int): KeyValueBytesStoreSupplier = Stores.lruMap(storeName.value, maxCacheSize)
    def persistentWindowStore(
      retentionPeriod: Duration,
      windowSize: Duration,
      retainDuplicates: Boolean): WindowBytesStoreSupplier =
      Stores.persistentWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates)

    def persistentWindowStore(
      retentionPeriod: FiniteDuration,
      windowSize: FiniteDuration,
      retainDuplicates: Boolean): WindowBytesStoreSupplier =
      persistentWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

    def persistentTimestampedWindowStore(
      retentionPeriod: Duration,
      windowSize: Duration,
      retainDuplicates: Boolean): WindowBytesStoreSupplier =
      Stores.persistentTimestampedWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates)

    def persistentTimestampedWindowStore(
      retentionPeriod: FiniteDuration,
      windowSize: FiniteDuration,
      retainDuplicates: Boolean): WindowBytesStoreSupplier =
      persistentTimestampedWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

    def inMemoryWindowStore(
      retentionPeriod: Duration,
      windowSize: Duration,
      retainDuplicates: Boolean): WindowBytesStoreSupplier =
      Stores.inMemoryWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates)

    def inMemoryWindowStore(
      retentionPeriod: FiniteDuration,
      windowSize: FiniteDuration,
      retainDuplicates: Boolean): WindowBytesStoreSupplier =
      inMemoryWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

    def persistentSessionStore(retentionPeriod: Duration): SessionBytesStoreSupplier =
      Stores.persistentSessionStore(storeName.value, retentionPeriod)

    def inMemorySessionStore(retentionPeriod: Duration): SessionBytesStoreSupplier =
      Stores.inMemorySessionStore(storeName.value, retentionPeriod)

    def inMemorySessionStore(retentionPeriod: FiniteDuration): SessionBytesStoreSupplier =
      inMemorySessionStore(retentionPeriod.toJava)
  }

  object builder {
    def keyValueStoreBuilder(supplier: KeyValueBytesStoreSupplier): StoreBuilder[KeyValueStore[K, V]] =
      Stores.keyValueStoreBuilder(supplier, serdeOfKey, serdeOfVal)

    def timestampedKeyValueStoreBuilder(
      supplier: KeyValueBytesStoreSupplier): StoreBuilder[TimestampedKeyValueStore[K, V]] =
      Stores.timestampedKeyValueStoreBuilder(supplier, serdeOfKey, serdeOfVal)

    def windowStoreBuilder(supplier: WindowBytesStoreSupplier): StoreBuilder[WindowStore[K, V]] =
      Stores.windowStoreBuilder(supplier, serdeOfKey, serdeOfVal)

    def timestampedWindowStoreBuilder(supplier: WindowBytesStoreSupplier): StoreBuilder[TimestampedWindowStore[K, V]] =
      Stores.timestampedWindowStoreBuilder(supplier, serdeOfKey, serdeOfVal)

    def sessionStoreBuilder(supplier: SessionBytesStoreSupplier): StoreBuilder[SessionStore[K, V]] =
      Stores.sessionStoreBuilder(supplier, serdeOfKey, serdeOfVal)

  }

  object query {
    def keyValueStore: StoreQueryParameters[ReadOnlyKeyValueStore[K, V]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.keyValueStore[K, V])

    def timestampedKeyValueStore: QueryableStoreType[ReadOnlyKeyValueStore[K, ValueAndTimestamp[V]]] =
      QueryableStoreTypes.timestampedKeyValueStore[K, V]

    def windowStore: StoreQueryParameters[ReadOnlyWindowStore[K, V]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.windowStore[K, V])

    def timestampedWindowStore: StoreQueryParameters[ReadOnlyWindowStore[K, ValueAndTimestamp[V]]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.timestampedWindowStore[K, V])

    def sessionStore: StoreQueryParameters[ReadOnlySessionStore[K, V]] =
      StoreQueryParameters.fromNameAndType(storeName.value, QueryableStoreTypes.sessionStore[K, V])

  }
}
