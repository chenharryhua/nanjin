package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.kafka.serdes.Unregistered
import com.github.chenharryhua.nanjin.kafka.SchemaRegistrySettings
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.processor.StateStore

final class StreamsSerde private[kafka] (schemaRegistrySettings: SchemaRegistrySettings) {
  private def asKey[K: Unregistered]: Serde[K] =
    summon[Unregistered[K]].asKey(schemaRegistrySettings.config).serde
  private def asValue[V: Unregistered]: Serde[V] =
    summon[Unregistered[V]].asValue(schemaRegistrySettings.config).serde

  def consumed[K: Unregistered, V: Unregistered]: Consumed[K, V] =
    Consumed.`with`[K, V](asKey[K], asValue[V])

  def produced[K: Unregistered, V: Unregistered]: Produced[K, V] =
    Produced.`with`[K, V](asKey[K], asValue[V])

  def joined[K: Unregistered, V: Unregistered, VO: Unregistered]: Joined[K, V, VO] =
    Joined.`with`(asKey[K], asValue[V], asValue[VO])

  def grouped[K: Unregistered, V: Unregistered]: Grouped[K, V] =
    Grouped.`with`(asKey[K], asValue[V])

  def repartitioned[K: Unregistered, V: Unregistered]: Repartitioned[K, V] =
    Repartitioned.`with`(asKey[K], asValue[V])

  def streamJoined[K: Unregistered, V: Unregistered, VO: Unregistered]: StreamJoined[K, V, VO] =
    StreamJoined.`with`(asKey[K], asValue[V], asValue[VO])

  def materialized[K: Unregistered, V: Unregistered, S <: StateStore]: Materialized[K, V, S] =
    Materialized.`with`[K, V, S](asKey[K], asValue[V])

}
