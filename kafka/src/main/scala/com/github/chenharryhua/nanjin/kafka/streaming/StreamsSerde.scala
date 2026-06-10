package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.kafka.SerdeSettings
import com.github.chenharryhua.nanjin.kafka.serdes.Unregistered
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{
  Consumed,
  Grouped,
  Joined,
  Materialized,
  Produced,
  Repartitioned,
  StreamJoined
}
import org.apache.kafka.streams.processor.StateStore

final class StreamsSerde private[kafka] (srClient: SchemaRegistryClient, serdeSettings: SerdeSettings) {
  private val properties: Map[String, String] = serdeSettings.properties

  private def asKey[K](key: Unregistered[K]): Serde[K] =
    key.asKey(srClient, properties).serde
  private def asValue[V](value: Unregistered[V]): Serde[V] =
    value.asValue(srClient, properties).serde

  /*
   * Streams Serde
   */

  def consumed[K, V](key: Unregistered[K], value: Unregistered[V]): Consumed[K, V] =
    Consumed.`with`(asKey(key), asValue(value))

  def produced[K, V](key: Unregistered[K], value: Unregistered[V]): Produced[K, V] =
    Produced.`with`(asKey(key), asValue(value))

  def joined[K, VL, VR](key: Unregistered[K], vl: Unregistered[VL], vr: Unregistered[VR]): Joined[K, VL, VR] =
    Joined.`with`(asKey(key), asValue(vl), asValue(vr))

  def streamJoined[K, V1, V2](
    key: Unregistered[K],
    v1: Unregistered[V1],
    v2: Unregistered[V2]): StreamJoined[K, V1, V2] =
    StreamJoined.`with`(asKey(key), asValue(v1), asValue(v2))

  def grouped[K, V](key: Unregistered[K], value: Unregistered[V]): Grouped[K, V] =
    Grouped.`with`(asKey(key), asValue(value))

  def repartitioned[K, V](key: Unregistered[K], value: Unregistered[V]): Repartitioned[K, V] =
    Repartitioned.`with`(asKey(key), asValue(value))

  def materialized[K, V, S <: StateStore](
    key: Unregistered[K],
    value: Unregistered[V]): Materialized[K, V, S] =
    Materialized.`with`(asKey(key), asValue(value))

}
