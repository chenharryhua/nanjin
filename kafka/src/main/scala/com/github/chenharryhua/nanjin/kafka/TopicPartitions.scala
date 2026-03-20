package com.github.chenharryhua.nanjin.kafka

import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import io.circe.{Codec, Decoder, Encoder, HCursor, Json}
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.TreeMap
import scala.jdk.CollectionConverters.*

opaque type TopicPartitionMap[V] = TreeMap[TopicPartition, V]
object TopicPartitionMap:
  def apply[V](value: TreeMap[TopicPartition, V]): TopicPartitionMap[V] = value
  def apply[V](it: IterableOnce[(TopicPartition, V)]): TopicPartitionMap[V] =
    TreeMap.from(it)
  def empty[V]: TopicPartitionMap[V] = TreeMap.empty
  val emptyOffset: TopicPartitionMap[Offset] = empty[Offset]

  extension [V](m: TopicPartitionMap[V])
    inline def value: TreeMap[TopicPartition, V] = m
    def nonEmpty: Boolean = m.nonEmpty
    def isEmpty: Boolean = m.isEmpty
    def get(tp: TopicPartition): Option[V] = m.get(tp)
    def get(topic: String, partition: Int): Option[V] =
      m.get(new TopicPartition(topic, partition))

    def mapValues[W](f: V => W): TopicPartitionMap[W] =
      TreeMap.from(m.map { case (k, v) => k -> f(v) })

    def map[W](f: (TopicPartition, V) => W): TopicPartitionMap[W] =
      TreeMap.from(m.map { case (k, v) => k -> f(k, v) })

    def intersectCombine[U, W](other: TopicPartitionMap[U])(fn: (V, U) => W): TopicPartitionMap[W] =
      val res = m.keySet.intersect(other.value.keySet).toList.flatMap { tp =>
        (m.get(tp), other.get(tp)).mapN((f, s) => tp -> fn(f, s))
      }
      TreeMap.from(res)

    def leftCombine[U, W](other: TopicPartitionMap[U])(
      fn: (V, U) => Option[W]): TopicPartitionMap[Option[W]] =
      TreeMap.from(m.map { case (tp, v) =>
        tp -> other.get(tp).flatMap(fn(v, _))
      })

    def flatten[W](using ev: V <:< Option[W]): TopicPartitionMap[W] =
      TreeMap.from(m.flatMap { case (k, v) => ev(v).map(k -> _) })

    def topicPartitions: TopicPartitionList =
      TopicPartitionList(m.keys.toList)

  given [V: Encoder]: Encoder[TopicPartitionMap[V]] =
    (a: TopicPartitionMap[V]) =>
      Encoder.encodeList[Json].apply(
        a.value.map { case (tp, v) =>
          Json.obj(
            "topic" -> Json.fromString(tp.topic()),
            "partition" -> Json.fromInt(tp.partition()),
            "value" -> Encoder[V].apply(v)
          )
        }.toList
      )

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

opaque type TopicPartitionList = List[TopicPartition]
object TopicPartitionList:
  def apply(value: List[TopicPartition]): TopicPartitionList = value

  extension (tpl: TopicPartitionList)
    inline def value: List[TopicPartition] = tpl
    def javaTimed(ldt: NJTimestamp): java.util.Map[TopicPartition, java.lang.Long] =
      tpl.map(tp => tp -> ldt.javaLong).toMap.asJava

    def javaList: java.util.List[TopicPartition] = tpl.asJava

  given Codec[TopicPartitionList] = new Codec[TopicPartitionList] {
    override def apply(a: TopicPartitionList): Json =
      Encoder.encodeList[TopicPartition].apply(a.value.sortBy(_.partition()))
    override def apply(c: HCursor): Decoder.Result[TopicPartitionList] =
      Decoder.decodeList[TopicPartition].apply(c).map(TopicPartitionList(_))
  }
end TopicPartitionList
