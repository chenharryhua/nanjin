package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.kafka.TopicSerde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.*

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

/** Wraps a key-value `KeyValueBytesStoreSupplier` with the topic's serdes, exposing store `StoreBuilder`s
  * (for adding to a topology) and a `Materialized` (for materializing a KTable/aggregation).
  */
final class KeyValueBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: KeyValueBytesStoreSupplier,
  topic: TopicSerde[K, V]) {

  /** Builder for a plain key-value store. */
  def keyValueStoreBuilder: StoreBuilder[KeyValueStore[K, V]] =
    Stores.keyValueStoreBuilder(supplier, topic.key.serde, topic.value.serde)

  /** Builder for a key-value store that also tracks each value's timestamp. */
  def timestampedKeyValueStoreBuilder: StoreBuilder[TimestampedKeyValueStore[K, V]] =
    Stores.timestampedKeyValueStoreBuilder(supplier, topic.key.serde, topic.value.serde)

  /** `Materialized` view backed by this supplier, with the topic's key/value serdes. */
  def materialized: Materialized[K, V, KeyValueStore[Bytes, Array[Byte]]] =
    Materialized.as(supplier).withKeySerde(topic.key.serde).withValueSerde(topic.value.serde)
}

/** Wraps a `WindowBytesStoreSupplier` with the topic's serdes; see `KeyValueBytesStoreSupplierHelper`. */
final class WindowBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: WindowBytesStoreSupplier,
  topic: TopicSerde[K, V]) {

  /** Builder for a windowed store. */
  def windowStoreBuilder: StoreBuilder[WindowStore[K, V]] =
    Stores.windowStoreBuilder(supplier, topic.key.serde, topic.value.serde)

  /** Builder for a windowed store that also tracks each value's timestamp. */
  def timestampedWindowStoreBuilder: StoreBuilder[TimestampedWindowStore[K, V]] =
    Stores.timestampedWindowStoreBuilder(supplier, topic.key.serde, topic.value.serde)

  /** `Materialized` view backed by this windowed supplier. */
  def materialized: Materialized[K, V, WindowStore[Bytes, Array[Byte]]] =
    Materialized.as(supplier).withKeySerde(topic.key.serde).withValueSerde(topic.value.serde)
}

/** Wraps a `SessionBytesStoreSupplier` with the topic's serdes; see `KeyValueBytesStoreSupplierHelper`. */
final class SessionBytesStoreSupplierHelper[K, V] private[streaming] (
  val supplier: SessionBytesStoreSupplier,
  topic: TopicSerde[K, V]) {

  /** Builder for a session store. */
  def sessionStoreBuilder: StoreBuilder[SessionStore[K, V]] =
    Stores.sessionStoreBuilder(supplier, topic.key.serde, topic.value.serde)

  /** `Materialized` view backed by this session supplier. */
  def materialized: Materialized[K, V, SessionStore[Bytes, Array[Byte]]] =
    Materialized.as(supplier).withKeySerde(topic.key.serde).withValueSerde(topic.value.serde)
}

/** Factory for Kafka Streams state stores keyed to a topic, using that topic's key/value serdes.
  *
  * The store name is the topic name. Each method returns a `*Helper` carrying the chosen store supplier plus
  * the serdes, from which a `StoreBuilder` or `Materialized` can be obtained. The nested `queries` object
  * provides `StoreQueryParameters` for interactive queries against a running app. Obtain an instance via
  * `StateStores.apply`.
  */
final class StateStores[K, V] private (topic: TopicSerde[K, V]) {

  /** The state-store name, taken from the topic name. */
  val name: String = topic.topicName.value

  /** Persistent (RocksDB) key-value store. */
  def persistentKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.persistentKeyValueStore(name), topic)

  /** Persistent key-value store that also tracks each value's timestamp. */
  def persistentTimestampedKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.persistentTimestampedKeyValueStore(name), topic)

  /** In-memory key-value store (not backed by disk). */
  def inMemoryKeyValueStore: KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.inMemoryKeyValueStore(name), topic)

  /** In-memory LRU key-value store holding at most `maxCacheSize` entries. */
  def lruMap(maxCacheSize: Int): KeyValueBytesStoreSupplierHelper[K, V] =
    new KeyValueBytesStoreSupplierHelper(Stores.lruMap(name, maxCacheSize), topic)

  /** Persistent windowed store retaining windows for `retentionPeriod`, each window of `windowSize`;
    * `retainDuplicates` keeps multiple values per key/window (needed for stream-stream joins).
    */
  def persistentWindowStore(
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    new WindowBytesStoreSupplierHelper(
      Stores.persistentWindowStore(name, retentionPeriod, windowSize, retainDuplicates),
      topic)

  /** `FiniteDuration` overload of `persistentWindowStore`. */
  def persistentWindowStore(
    retentionPeriod: FiniteDuration,
    windowSize: FiniteDuration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    persistentWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

  /** Persistent windowed store (as `persistentWindowStore`) that also tracks each value's timestamp. */
  def persistentTimestampedWindowStore(
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    new WindowBytesStoreSupplierHelper(
      Stores.persistentTimestampedWindowStore(name, retentionPeriod, windowSize, retainDuplicates),
      topic)

  /** `FiniteDuration` overload of `persistentTimestampedWindowStore`. */
  def persistentTimestampedWindowStore(
    retentionPeriod: FiniteDuration,
    windowSize: FiniteDuration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    persistentTimestampedWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

  /** In-memory windowed store (as `persistentWindowStore`, but not disk-backed). */
  def inMemoryWindowStore(
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    new WindowBytesStoreSupplierHelper(
      Stores.inMemoryWindowStore(name, retentionPeriod, windowSize, retainDuplicates),
      topic)

  /** `FiniteDuration` overload of `inMemoryWindowStore`. */
  def inMemoryWindowStore(
    retentionPeriod: FiniteDuration,
    windowSize: FiniteDuration,
    retainDuplicates: Boolean): WindowBytesStoreSupplierHelper[K, V] =
    inMemoryWindowStore(retentionPeriod.toJava, windowSize.toJava, retainDuplicates)

  /** Persistent session store retaining sessions for `retentionPeriod`. */
  def persistentSessionStore(retentionPeriod: Duration): SessionBytesStoreSupplierHelper[K, V] =
    new SessionBytesStoreSupplierHelper(Stores.persistentSessionStore(name, retentionPeriod), topic)

  /** In-memory session store retaining sessions for `retentionPeriod`. */
  def inMemorySessionStore(retentionPeriod: Duration): SessionBytesStoreSupplierHelper[K, V] =
    new SessionBytesStoreSupplierHelper(Stores.inMemorySessionStore(name, retentionPeriod), topic)

  /** `FiniteDuration` overload of `inMemorySessionStore`. */
  def inMemorySessionStore(retentionPeriod: FiniteDuration): SessionBytesStoreSupplierHelper[K, V] =
    inMemorySessionStore(retentionPeriod.toJava)

  /** `StoreQueryParameters` for interactive queries against a running app's read-only view of this store, one
    * accessor per store shape.
    */
  object queries {

    /** Query parameters for a read-only key-value store view. */
    def keyValueStore: StoreQueryParameters[ReadOnlyKeyValueStore[K, V]] =
      StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.keyValueStore[K, V])

    /** Query parameters for a read-only timestamped key-value store view. */
    def timestampedKeyValueStore: StoreQueryParameters[ReadOnlyKeyValueStore[K, ValueAndTimestamp[V]]] =
      StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.timestampedKeyValueStore[K, V])

    /** Query parameters for a read-only windowed store view. */
    def windowStore: StoreQueryParameters[ReadOnlyWindowStore[K, V]] =
      StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.windowStore[K, V])

    /** Query parameters for a read-only timestamped windowed store view. */
    def timestampedWindowStore: StoreQueryParameters[ReadOnlyWindowStore[K, ValueAndTimestamp[V]]] =
      StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.timestampedWindowStore[K, V])

    /** Query parameters for a read-only session store view. */
    def sessionStore: StoreQueryParameters[ReadOnlySessionStore[K, V]] =
      StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.sessionStore[K, V])
  }
}

private[kafka] object StateStores {

  /** Create a `StateStores` for the given topic, using its key/value serdes and topic name. */
  def apply[K, V](topic: TopicSerde[K, V]): StateStores[K, V] =
    new StateStores[K, V](topic)
}
