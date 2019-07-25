package com.github.chenharryhua.nanjin.kafka

import java.time.Duration

import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.{scala, KafkaStreams}

final case class KafkaStoreName(value: String) extends AnyVal

sealed abstract class KafkaStore[K: SerdeOf, V: SerdeOf, Q[_, _], S <: StateStore](
  storeName: KafkaStoreName)
    extends Serializable {
  def materialized: Materialized[K, V, S]
  protected def queryableStoreType: QueryableStoreType[Q[K, V]]
  final def monitor(streams: KafkaStreams): Q[K, V] =
    streams.store(storeName.value, queryableStoreType)
}

object KafkaStore {

  final class Persistent[K: SerdeOf, V: SerdeOf](storeName: KafkaStoreName)
      extends KafkaStore[K, V, ReadOnlyKeyValueStore, scala.ByteArrayKeyValueStore](storeName) {

    override protected val queryableStoreType: QueryableStoreType[ReadOnlyKeyValueStore[K, V]] =
      QueryableStoreTypes.keyValueStore[K, V]

    override val materialized: Materialized[K, V, scala.ByteArrayKeyValueStore] =
      Materialized.as(Stores.persistentKeyValueStore(storeName.value))(SerdeOf[K], SerdeOf[V])
  }

  final class InMemory[K: SerdeOf, V: SerdeOf](storeName: KafkaStoreName)
      extends KafkaStore[K, V, ReadOnlyKeyValueStore, scala.ByteArrayKeyValueStore](storeName) {

    override protected val queryableStoreType: QueryableStoreType[ReadOnlyKeyValueStore[K, V]] =
      QueryableStoreTypes.keyValueStore[K, V]

    override val materialized: Materialized[K, V, scala.ByteArrayKeyValueStore] =
      Materialized.as(Stores.inMemoryKeyValueStore(storeName.value))(SerdeOf[K], SerdeOf[V])
  }

  final class Session[K: SerdeOf, V: SerdeOf](storeName: KafkaStoreName, retentionPeriod: Duration)
      extends KafkaStore[K, V, ReadOnlySessionStore, scala.ByteArraySessionStore](storeName) {

    override protected val queryableStoreType: QueryableStoreType[ReadOnlySessionStore[K, V]] =
      QueryableStoreTypes.sessionStore[K, V]

    override val materialized: Materialized[K, V, scala.ByteArraySessionStore] =
      Materialized.as(Stores.persistentSessionStore(storeName.value, retentionPeriod))(
        SerdeOf[K],
        SerdeOf[V])
  }

  final class Window[K: SerdeOf, V: SerdeOf](
    storeName: KafkaStoreName,
    retentionPeriod: Duration,
    windowSize: Duration,
    retainDuplicates: Boolean
  ) extends KafkaStore[K, V, ReadOnlyWindowStore, scala.ByteArrayWindowStore](storeName) {

    override protected val queryableStoreType: QueryableStoreType[ReadOnlyWindowStore[K, V]] =
      QueryableStoreTypes.windowStore[K, V]

    override val materialized: Materialized[K, V, scala.ByteArrayWindowStore] =
      Materialized.as(
        Stores.persistentWindowStore(storeName.value, retentionPeriod, windowSize, retainDuplicates)
      )(SerdeOf[K], SerdeOf[V])
  }
}
