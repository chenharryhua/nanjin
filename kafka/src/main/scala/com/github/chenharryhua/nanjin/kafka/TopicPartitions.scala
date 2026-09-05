package com.github.chenharryhua.nanjin.kafka

import cats.data.NonEmptySet
import cats.syntax.apply.given
import cats.syntax.traverse.given
import io.circe.{Codec, Decoder, Encoder, HCursor, Json}
import org.apache.kafka.common.TopicPartition

import java.time.Instant
import scala.collection.immutable.{TreeMap, TreeSet}
import scala.jdk.CollectionConverters.*

/** A sorted map from `TopicPartition` to `V`, backed by a `TreeMap` so iteration order is stable (by topic
  * then partition).
  *
  * Used throughout the consumer connectors to carry per-partition data such as offsets or per-partition
  * streams. Beyond the usual map accessors it offers combinators for aligning two maps by partition
  * (`intersectCombine`, `leftCombine`) and dropping absent entries (`flatten`), plus a JSON codec that
  * encodes each entry as `{topic, partition, value}`.
  */
opaque type TopicPartitionMap[V] = TreeMap[TopicPartition, V]
object TopicPartitionMap:

  /** Wrap an existing sorted `TreeMap`. */
  def apply[V](value: TreeMap[TopicPartition, V]): TopicPartitionMap[V] = value

  /** Build from any iterable of `(TopicPartition, V)` pairs, sorting by partition. */
  def apply[V](it: IterableOnce[(TopicPartition, V)]): TopicPartitionMap[V] =
    TreeMap.from(it)

  /** The empty map. */
  def empty[V]: TopicPartitionMap[V] = TreeMap.empty

  /** The empty offset map, a common starting point. */
  val emptyOffset: TopicPartitionMap[Offset] = empty[Offset]

  extension [V](m: TopicPartitionMap[V])

    /** The underlying `TreeMap`. */
    inline def treeMap: TreeMap[TopicPartition, V] = m

    /** The sorted set of partitions present. */
    def keySet: TreeSet[TopicPartition] = m.keySet

    /** The values in partition order. */
    def values: List[V] = m.values.toList

    /** The partition set as a `NonEmptySet`, or `None` if empty. */
    def nonEmptyKeySet: Option[NonEmptySet[TopicPartition]] = NonEmptySet.fromSet(keySet)

    /** The entries as a partition-ordered list. */
    def toList: List[(TopicPartition, V)] = treeMap.toList

    def nonEmpty: Boolean = m.nonEmpty
    def isEmpty: Boolean = m.isEmpty

    /** Look up the value for a partition. */
    def get(tp: TopicPartition): Option[V] = m.get(tp)

    /** Look up the value by topic name and partition number. */
    def get(topic: String, partition: Int): Option[V] =
      m.get(new TopicPartition(topic, partition))

    /** Transform each value, keeping the partitions. */
    def mapValues[W](f: V => W): TopicPartitionMap[W] =
      m.view.mapValues(f).to(TreeMap)

    /** Transform each value with access to its partition key. */
    def map[W](f: (TopicPartition, V) => W): TopicPartitionMap[W] =
      m.iterator.map { case (k, v) => k -> f(k, v) }.to(TreeMap)

    /** Combine with `other` on the partitions present in '''both''' maps, applying `fn` to the paired values;
      * partitions in only one map are dropped.
      */
    def intersectCombine[U, W](other: TopicPartitionMap[U])(fn: (V, U) => W): TopicPartitionMap[W] =
      val res = m.keySet.intersect(other.keySet).toList.flatMap { tp =>
        (m.get(tp), other.get(tp)).mapN((f, s) => tp -> fn(f, s))
      }
      TreeMap.from(res)

    /** Keep every partition of '''this''' map, combining with `other`'s value when present; the result is
      * optional per partition (`None` where `other` lacks the partition or `fn` returns `None`).
      */
    def leftCombine[U, W](other: TopicPartitionMap[U])(
      fn: (V, U) => Option[W]): TopicPartitionMap[Option[W]] =
      TreeMap.from(m.map { case (tp, v) =>
        tp -> other.get(tp).flatMap(fn(v, _))
      })

    /** Drop partitions whose value is `None`, unwrapping the rest (for a map of optional values). */
    def flatten[W](using ev: V <:< Option[W]): TopicPartitionMap[W] =
      m.iterator.flatMap { case (k, v) => ev(v).map(k -> _) }.to(TreeMap)

    /** The partitions as a `TopicPartitionList`. */
    def topicPartitions: TopicPartitionList =
      TopicPartitionList(keySet.toList)

  /** Encode as a JSON array of `{topic, partition, value}` objects. */
  given [V: Encoder]: Encoder[TopicPartitionMap[V]] =
    (a: TopicPartitionMap[V]) =>
      Encoder.encodeList[Json].apply(
        a.iterator.map { case (tp, v) =>
          Json.obj(
            "topic" -> Json.fromString(tp.topic()),
            "partition" -> Json.fromInt(tp.partition()),
            "value" -> Encoder[V].apply(v)
          )
        }.toList
      )

  /** Decode the `{topic, partition, value}` array form produced by the encoder. */
  given [V: Decoder]: Decoder[TopicPartitionMap[V]] =
    (c: HCursor) =>
      Decoder.decodeList[Json].flatMap { jsons =>
        Decoder.instance(_ =>
          jsons.traverse { json =>
            val hc = json.hcursor
            for
              t <- hc.downField("topic").as[String]
              p <- hc.downField("partition").as[Int]
              v <- hc.downField("value").as[V]
            yield new TopicPartition(t, p) -> v
          }.map(lst => TreeMap.from(lst)))
      }.apply(c)

end TopicPartitionMap

/** A list of `TopicPartition`s, with conversions to the Java shapes the Kafka client expects and a JSON codec
  * (sorted by topic then partition on encode).
  */
opaque type TopicPartitionList = List[TopicPartition]
object TopicPartitionList:

  /** Wrap an existing list of partitions. */
  def apply(value: List[TopicPartition]): TopicPartitionList = value

  extension (tpl: TopicPartitionList)

    /** The underlying list. */
    inline def value: List[TopicPartition] = tpl

    /** The partitions as a set. */
    def toSet: Set[TopicPartition] = tpl.toSet

    /** A Java map pairing each partition with `ldt`'s epoch-millis, for `offsetsForTimes`-style lookups. */
    def javaTimed(ldt: Instant): java.util.Map[TopicPartition, java.lang.Long] =
      tpl.map(tp => tp -> java.lang.Long.valueOf(ldt.toEpochMilli)).toMap.asJava

    /** The partitions as a Java list, for Kafka client APIs. */
    def javaList: java.util.List[TopicPartition] = tpl.asJava

  /** JSON codec: encodes as an array of partitions sorted by topic then partition; decodes the array back. */
  given Codec[TopicPartitionList] = new Codec[TopicPartitionList] {
    override def apply(a: TopicPartitionList): Json =
      Encoder.encodeList[TopicPartition].apply(a.sortBy(tp => (tp.topic(), tp.partition())))
    override def apply(c: HCursor): Decoder.Result[TopicPartitionList] =
      Decoder.decodeList[TopicPartition].apply(c).map(TopicPartitionList(_))
  }
end TopicPartitionList
