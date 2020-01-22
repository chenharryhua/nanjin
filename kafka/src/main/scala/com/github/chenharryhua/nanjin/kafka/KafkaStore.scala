package com.github.chenharryhua.nanjin.kafka

import java.time.Duration

import com.github.chenharryhua.nanjin.kafka.codec.NJSerde
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.{scala, KafkaStreams}

final case class KafkaStoreName(value: String) extends AnyVal

sealed abstract class KafkaStore[K, V, Q[_, _], S <: StateStore](
  storeName: KafkaStoreName,
  keySerde: NJSerde[K],
  valueSerde: NJSerde[V])
    extends Serializable {
  def materialized: Materialized[K, V, S]
  protected def queryableStoreType: QueryableStoreType[Q[K, V]]

  final def monitor(streams: KafkaStreams): Q[K, V] =
    streams.store(storeName.value, queryableStoreType)
}

object KafkaStore {

  final class Persistent[K, V](
    storeName: KafkaStoreName,
    keySerde: NJSerde[K],
    valueSerde: NJSerde[V])
      extends KafkaStore[K, V, ReadOnlyKeyValueStore, scala.ByteArrayKeyValueStore](
        storeName,
        keySerde,
        valueSerde) {

    override protected val queryableStoreType: QueryableStoreType[ReadOnlyKeyValueStore[K, V]] =
      QueryableStoreTypes.keyValueStore[K, V]

    override val materialized: Materialized[K, V, scala.ByteArrayKeyValueStore] =
      Materialized.as(Stores.persistentKeyValueStore(storeName.value))(keySerde, valueSerde)
  }

  final class InMemory[K, V](
    storeName: KafkaStoreName,
    keySerde: NJSerde[K],
    valueSerde: NJSerde[V])
      extends KafkaStore[K, V, ReadOnlyKeyValueStore, scala.ByteArrayKeyValueStore](
        storeName,
        keySerde,
        valueSerde) {

    override protected val queryableStoreType: QueryableStoreType[ReadOnlyKeyValueStore[K, V]] =
      QueryableStoreTypes.keyValueStore[K, V]

    override val materialized: Materialized[K, V, scala.ByteArrayKeyValueStore] =
      Materialized.as(Stores.inMemoryKeyValueStore(storeName.value))(keySerde, valueSerde)
  }

  final class Session[K, V](
    storeName: KafkaStoreName,
    retentionPeriod: Duration,
    keySerde: NJSerde[K],
    valueSerde: NJSerde[V])
      extends KafkaStore[K, V, ReadOnlySessionStore, scala.ByteArraySessionStore](
        storeName,
        keySerde,
        valueSerde) {

    override protected val queryableStoreType: QueryableStoreType[ReadOnlySessionStore[K, V]] =
      QueryableStoreTypes.sessionStore[K, V]

    override val materialized: Materialized[K, V, scala.ByteArraySessionStore] =
      Materialized.as(Stores.persistentSessionStore(storeName.value, retentionPeriod))(
        keySerde,
        valueSerde)
  }

  final class Window[K, V](
    storeName: KafkaStoreName,
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean,
    keySerde: NJSerde[K],
    valueSerde: NJSerde[V]
  ) extends KafkaStore[K, V, ReadOnlyWindowStore, scala.ByteArrayWindowStore](
        storeName,
        keySerde,
        valueSerde) {

    override protected val queryableStoreType: QueryableStoreType[ReadOnlyWindowStore[K, V]] =
      QueryableStoreTypes.windowStore[K, V]

    override val materialized: Materialized[K, V, scala.ByteArrayWindowStore] =
      Materialized.as(
        Stores.persistentWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates)
      )(keySerde, valueSerde)
  }
}
