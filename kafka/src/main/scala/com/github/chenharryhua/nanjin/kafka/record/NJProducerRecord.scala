package com.github.chenharryhua.nanjin.kafka.record

import cats.Bitraverse
import cats.data.Cont
import cats.derived.derived
import cats.kernel.Eq
import com.sksamuel.avro4s.*
import fs2.kafka.{Header, Headers, ProducerRecord}
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder}
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.into
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord
import org.apache.kafka.common.header.Header as JavaHeader

import scala.jdk.CollectionConverters.*

/** An immutable, serialization-friendly view of a Kafka producer record.
  *
  * All positioning fields are optional: `partition`, `timestamp`, and `key`/`value` may be absent, letting
  * Kafka assign a partition/timestamp and permitting null keys/values. `offset` is retained only for sorting
  * (a producer record has no real offset). It derives `Bitraverse` (over key and value), `Eq`, and JSON
  * `Encoder`/`Decoder`, with Avro support available separately. Conversions to and from the Java client
  * record, the fs2 record, and Avro `GenericRecord` are provided, along with a fluent `with*`/`no*` builder
  * API.
  */
@AvroDoc("kafka producer record, optional Key and optional Value")
@AvroNamespace("nanjin.kafka")
@AvroName("NJProducerRecord")
final case class NJProducerRecord[K, V](
  topic: String,
  partition: Option[Int],
  offset: Option[Long], // for sort
  timestamp: Option[Long],
  headers: List[NJHeader],
  key: Option[K],
  value: Option[V]
) derives Bitraverse, Eq, JsonEncoder, JsonDecoder {

  /** Set the target topic. */
  def withTopicName(name: String): NJProducerRecord[K, V] = copy(topic = name)

  /** Set an explicit partition (otherwise Kafka assigns one). */
  def withPartition(pt: Int): NJProducerRecord[K, V] = copy(partition = Some(pt))

  /** Set an explicit timestamp in epoch millis (otherwise Kafka assigns one). */
  def withTimestamp(ts: Long): NJProducerRecord[K, V] = copy(timestamp = Some(ts))

  /** Set the key. */
  def withKey(k: K): NJProducerRecord[K, V] = copy(key = Some(k))

  /** Set the value. */
  def withValue(v: V): NJProducerRecord[K, V] = copy(value = Some(v))

  /** Replace the headers. */
  def withHeaders(headers: List[NJHeader]): NJProducerRecord[K, V] = copy(headers = headers)

  /** Clear the partition, letting Kafka assign one. */
  def noPartition: NJProducerRecord[K, V] = copy(partition = None)

  /** Clear the timestamp, letting Kafka assign one. */
  def noTimestamp: NJProducerRecord[K, V] = copy(timestamp = None)

  /** Clear all headers. */
  def noHeaders: NJProducerRecord[K, V] = copy(headers = Nil)

  /** Clear the key (produces a null-keyed record). */
  def noKey: NJProducerRecord[K, V] = copy(key = None)

  /** Clear the value (produces a tombstone). */
  def noValue: NJProducerRecord[K, V] = copy(value = None)

  /** Clear partition, timestamp, and headers in one step, keeping only key and value. */
  def noMeta: NJProducerRecord[K, V] = copy(partition = None, timestamp = None, headers = Nil)

  /** Convert to the fs2-kafka `ProducerRecord`. A `None` key or value becomes `null`. */
  def toProducerRecord(using Null <:< K, Null <:< V): ProducerRecord[K, V] =
    this.into[ProducerRecord[K, V]].transform

  /** Convert to the Java Kafka client's `ProducerRecord`. A `None` key/value/partition/timestamp becomes
    * `null`, letting the client apply its defaults.
    */
  def toJavaProducerRecord(using Null <:< K, Null <:< V): JavaProducerRecord[K, V] =
    this.into[JavaProducerRecord[K, V]].transform

  /** Encode into an Avro `GenericRecord`, given Avro `Encoder`/`SchemaFor` for the key and value types. */
  def toGenericRecord(using Encoder[K], Encoder[V], SchemaFor[K], SchemaFor[V]): GenericRecord = {
    val schema = summon[SchemaFor[NJProducerRecord[K, V]]].schema
    ToRecord[NJProducerRecord[K, V]](schema).to(this)
  }
}

object NJProducerRecord {

  /** Build an `NJProducerRecord` from the Java Kafka client's `ProducerRecord`. */
  def apply[K, V](pr: JavaProducerRecord[K, V]): NJProducerRecord[K, V] =
    pr.into[NJProducerRecord[K, V]].transform

  /** Build an `NJProducerRecord` from the fs2-kafka `ProducerRecord`. */
  def apply[K, V](pr: ProducerRecord[K, V]): NJProducerRecord[K, V] =
    pr.into[NJProducerRecord[K, V]].transform

  /** Build a minimal `NJProducerRecord` for `topicName` with the given key and value and no metadata; a null
    * `k` or `v` becomes `None`.
    */
  def apply[K, V](topicName: String, k: K, v: V): NJProducerRecord[K, V] =
    NJProducerRecord(
      topic = topicName,
      partition = None,
      offset = None,
      timestamp = None,
      headers = Nil,
      key = Option(k),
      value = Option(v))

  /** Build the Avro schema for `NJProducerRecord` from the key and value schemas, without needing static
    * `SchemaFor` instances for `K` and `V`.
    */
  def schema(keySchema: Schema, valSchema: Schema): Schema = {
    class KEY
    class VAL
    given SchemaFor[KEY] = SchemaFor[KEY](keySchema)
    given SchemaFor[VAL] = SchemaFor[VAL](valSchema)
    SchemaFor[NJProducerRecord[KEY, VAL]].schema
  }

  /** Chimney transformer from the Java client record; null key/value/partition/timestamp become `None` and
    * `offset` is `None` (a producer record has none).
    */
  given [K, V]: Transformer[JavaProducerRecord[K, V], NJProducerRecord[K, V]] =
    (src: JavaProducerRecord[K, V]) =>
      NJProducerRecord(
        topic = src.topic(),
        partition = Option(src.partition()).map(_.toInt),
        offset = None,
        timestamp = Option(src.timestamp()).map(_.toLong),
        key = Option(src.key()),
        value = Option(src.value()),
        headers = src.headers().toArray.map(_.into[NJHeader].transform).toList
      )

  /** Chimney transformer to the Java client record; `None` key/value become `null` via the `Null <:<`
    * evidence, and `None` partition/timestamp become `null` so the client applies its defaults.
    */
  given [K, V](using
    ek: Null <:< K,
    ev: Null <:< V): Transformer[NJProducerRecord[K, V], JavaProducerRecord[K, V]] =
    (src: NJProducerRecord[K, V]) =>
      new JavaProducerRecord[K, V](
        src.topic,
        src.partition.map(Integer.valueOf).orNull,
        src.timestamp.map(java.lang.Long.valueOf).orNull,
        src.key.getOrElse(ek(null)),
        src.value.getOrElse(ev(null)),
        src.headers.map(_.into[JavaHeader].transform).asJava
      )

  /** Chimney transformer from the fs2-kafka record; null key/value become `None` and `offset` is `None`. */
  given [K, V]: Transformer[ProducerRecord[K, V], NJProducerRecord[K, V]] =
    (src: ProducerRecord[K, V]) =>
      NJProducerRecord(
        topic = src.topic,
        partition = src.partition,
        offset = None,
        timestamp = src.timestamp,
        key = Option(src.key),
        value = Option(src.value),
        headers = src.headers.toChain.map(_.into[NJHeader].transform).toList
      )

  /** Chimney transformer to the fs2-kafka record; `None` key/value become `null`, and partition/timestamp are
    * set only when present.
    */
  given [K, V](using Null <:< K, Null <:< V): Transformer[NJProducerRecord[K, V], ProducerRecord[K, V]] =
    (src: NJProducerRecord[K, V]) =>
      Cont
        .pure(
          ProducerRecord[K, V](
            src.topic,
            src.key.getOrElse(summon[Null <:< K](null)),
            src.value.getOrElse(summon[Null <:< V](null))
          ).withHeaders(Headers.fromSeq(src.headers.map(_.into[Header].transform))))
        .map(pr => src.partition.fold(pr)(pr.withPartition))
        .map(pr => src.timestamp.fold(pr)(pr.withTimestamp))
        .eval
        .value
}
