package com.github.chenharryhua.nanjin.kafka

import cats.syntax.all.*
import cats.{Order, PartialOrder}
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import io.circe.*
import io.circe.Decoder.Result
import io.circe.generic.JsonCodec
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import java.{lang, util}
import scala.collection.immutable.TreeMap
import scala.jdk.CollectionConverters.*

final case class GroupId(value: String) extends AnyVal
object GroupId {
  implicit val codecKafkaGroupId: Codec[GroupId] = new Codec[GroupId] {
    override def apply(c: HCursor): Result[GroupId] = Decoder.decodeString(c).map(GroupId(_))
    override def apply(a: GroupId): Json            = Encoder.encodeString(a.value)
  }
}

final case class Offset(value: Long) extends AnyVal {
  def asLast: Offset         = Offset(value - 1) // represent last message
  def -(other: Offset): Long = value - other.value
}

object Offset {
  def apply(oam: OffsetAndMetadata): Offset  = Offset(oam.offset())
  def apply(oat: OffsetAndTimestamp): Offset = Offset(oat.offset())

  implicit val codecKafkaOffset: Codec[Offset] = new Codec[Offset] {
    override def apply(c: HCursor): Result[Offset] = Decoder.decodeLong(c).map(Offset(_))
    override def apply(a: Offset): Json            = Encoder.encodeLong(a.value)
  }

  implicit val orderKafkaOffset: Order[Offset] =
    (x: Offset, y: Offset) => x.value.compareTo(y.value)
}

final case class Partition(value: Int) extends AnyVal {
  def -(other: Partition): Int = value - other.value
}

object Partition {

  implicit val codecKafkaPartition: Codec[Partition] = new Codec[Partition] {
    override def apply(c: HCursor): Result[Partition] = Decoder.decodeInt(c).map(Partition(_))
    override def apply(a: Partition): Json            = Encoder.encodeInt(a.value)
  }

  implicit val orderKafkaPartition: Order[Partition] =
    (x: Partition, y: Partition) => x.value.compareTo(y.value)
}

@JsonCodec
final case class OffsetRange private (from: Offset, until: Offset, distance: Long)

object OffsetRange {
  def apply(from: Offset, until: Offset): Option[OffsetRange] =
    if (from < until)
      Some(OffsetRange(from, until, until - from))
    else
      None

  implicit val poKafkaOffsetRange: PartialOrder[OffsetRange] =
    (x: OffsetRange, y: OffsetRange) =>
      (x, y) match {
        case (OffsetRange(xf, xu, _), OffsetRange(yf, yu, _)) if xf >= yf && xu < yu =>
          -1.0
        case (OffsetRange(xf, xu, _), OffsetRange(yf, yu, _)) if xf === yf && xu === yu =>
          0.0
        case (OffsetRange(xf, xu, _), OffsetRange(yf, yu, _)) if xf <= yf && xu > yu =>
          1.0
        case _ => Double.NaN
      }
}

@JsonCodec
final case class LagBehind private (current: Offset, end: Offset, lag: Long)
object LagBehind {
  def apply(current: Offset, end: Offset): LagBehind =
    LagBehind(current, end, end - current)
}

final case class ListOfTopicPartitions(value: List[TopicPartition]) extends AnyVal {

  def javaTimed(ldt: NJTimestamp): util.Map[TopicPartition, lang.Long] =
    value.map(tp => tp -> ldt.javaLong).toMap.asJava

  def asJava: util.List[TopicPartition] = value.asJava
}

object ListOfTopicPartitions {
  implicit val codecListOfTopicPartitions: Codec[ListOfTopicPartitions] = new Codec[ListOfTopicPartitions] {
    override def apply(a: ListOfTopicPartitions): Json =
      Encoder.encodeList[TopicPartition].apply(a.value.sortBy(_.partition()))
    override def apply(c: HCursor): Result[ListOfTopicPartitions] =
      Decoder.decodeList[TopicPartition].apply(c).map(ListOfTopicPartitions(_))
  }
}

final case class TopicPartitionMap[V](value: TreeMap[TopicPartition, V]) extends AnyVal {
  def nonEmpty: Boolean = value.nonEmpty
  def isEmpty: Boolean  = value.isEmpty

  def get(tp: TopicPartition): Option[V] = value.get(tp)

  def get(topic: String, partition: Int): Option[V] =
    value.get(new TopicPartition(topic, partition))

  def mapValues[W](f: V => W): TopicPartitionMap[W] =
    copy(value = TreeMap.from(value.view.mapValues(f)))

  def map[W](f: (TopicPartition, V) => W): TopicPartitionMap[W] =
    copy(value = value.map { case (k, v) => k -> f(k, v) })

  def intersectCombine[U, W](other: TopicPartitionMap[U])(fn: (V, U) => W): TopicPartitionMap[W] = {
    val res: List[(TopicPartition, W)] = value.keySet.intersect(other.value.keySet).toList.flatMap { tp =>
      (value.get(tp), other.value.get(tp)).mapN((f, s) => tp -> fn(f, s))
    }
    TopicPartitionMap(res.toMap)
  }

  def leftCombine[U, W](other: TopicPartitionMap[U])(
    fn: (V, U) => Option[W]): TopicPartitionMap[Option[W]] = {
    val res: Map[TopicPartition, Option[W]] =
      value.map { case (tp, v) =>
        tp -> other.value.get(tp).flatMap(fn(v, _))
      }
    TopicPartitionMap(res)
  }

  def topicPartitions: ListOfTopicPartitions = ListOfTopicPartitions(value.keys.toList)
}

object TopicPartitionMap {
  def apply[V](map: Map[TopicPartition, V]): TopicPartitionMap[V] = TopicPartitionMap(TreeMap.from(map))

  implicit def codecKafkaTopicPartition[V: Encoder: Decoder]: Codec[TopicPartitionMap[V]] =
    new Codec[TopicPartitionMap[V]] {
      override def apply(a: TopicPartitionMap[V]): Json =
        Encoder
          .encodeList[Json]
          .apply(a.value.map { case (tp, v) =>
            Json.obj(
              "topic" -> Json.fromString(tp.topic()),
              "partition" -> Json.fromInt(tp.partition()),
              "value" -> Encoder[V].apply(v))
          }.toList)

      override def apply(c: HCursor): Result[TopicPartitionMap[V]] =
        Decoder
          .decodeList[Json]
          .flatMap { jsons =>
            Decoder.instance(_ =>
              jsons.traverse { json =>
                val hc = json.hcursor
                for {
                  t <- hc.downField("topic").as[String]
                  p <- hc.downField("partition").as[Int]
                  v <- hc.downField("value").as[V]
                } yield new TopicPartition(t, p) -> v
              }.map(lst => TopicPartitionMap(TreeMap.from(lst))))
          }
          .apply(c)
    }

  implicit final class KafkaTopicPartitionOps1[V](private val self: TopicPartitionMap[Option[V]]) {
    def flatten: TopicPartitionMap[V] =
      self.copy(value = TreeMap.from(self.value.flatMap { case (k, v) => v.map((k, _)) }))
  }

  implicit final class KafkaTopicPartitionOps2(
    private val self: TopicPartitionMap[Option[OffsetAndTimestamp]]) {
    def offsets: TopicPartitionMap[Option[Offset]] =
      self.copy(value = TreeMap.from(self.value.view.mapValues(_.map(Offset(_)))))
  }

  def empty[V]: TopicPartitionMap[V]         = TopicPartitionMap(Map.empty[TopicPartition, V])
  val emptyOffset: TopicPartitionMap[Offset] = empty[Offset]
}
