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
import scala.jdk.CollectionConverters.*

final case class KafkaGroupId(value: String) extends AnyVal
object KafkaGroupId {
  implicit val codecKafkaGroupId: Codec[KafkaGroupId] = new Codec[KafkaGroupId] {
    override def apply(c: HCursor): Result[KafkaGroupId] = Decoder.decodeString(c).map(KafkaGroupId(_))
    override def apply(a: KafkaGroupId): Json            = Encoder.encodeString(a.value)
  }
}

final case class KafkaOffset(value: Long) extends AnyVal {
  def asLast: KafkaOffset         = KafkaOffset(value - 1) // represent last message
  def -(other: KafkaOffset): Long = value - other.value
}

object KafkaOffset {
  def apply(oam: OffsetAndMetadata): KafkaOffset  = KafkaOffset(oam.offset())
  def apply(oat: OffsetAndTimestamp): KafkaOffset = KafkaOffset(oat.offset())

  implicit val codecKafkaOffset: Codec[KafkaOffset] = new Codec[KafkaOffset] {
    override def apply(c: HCursor): Result[KafkaOffset] = Decoder.decodeLong(c).map(KafkaOffset(_))
    override def apply(a: KafkaOffset): Json            = Encoder.encodeLong(a.value)
  }

  implicit val orderKafkaOffset: Order[KafkaOffset] =
    (x: KafkaOffset, y: KafkaOffset) => x.value.compareTo(y.value)
}

final case class KafkaPartition(value: Int) extends AnyVal {
  def -(other: KafkaPartition): Int = value - other.value
}

object KafkaPartition {

  implicit val codecKafkaPartition: Codec[KafkaPartition] = new Codec[KafkaPartition] {
    override def apply(c: HCursor): Result[KafkaPartition] = Decoder.decodeInt(c).map(KafkaPartition(_))
    override def apply(a: KafkaPartition): Json            = Encoder.encodeInt(a.value)
  }

  implicit val orderKafkaPartition: Order[KafkaPartition] =
    (x: KafkaPartition, y: KafkaPartition) => x.value.compareTo(y.value)
}

@JsonCodec
final case class KafkaOffsetRange private (from: KafkaOffset, until: KafkaOffset, distance: Long)

object KafkaOffsetRange {
  def apply(from: KafkaOffset, until: KafkaOffset): Option[KafkaOffsetRange] =
    if (from < until)
      Some(KafkaOffsetRange(from, until, until - from))
    else
      None

  implicit val poKafkaOffsetRange: PartialOrder[KafkaOffsetRange] =
    (x: KafkaOffsetRange, y: KafkaOffsetRange) =>
      (x, y) match {
        case (KafkaOffsetRange(xf, xu, _), KafkaOffsetRange(yf, yu, _)) if xf >= yf && xu < yu =>
          -1.0
        case (KafkaOffsetRange(xf, xu, _), KafkaOffsetRange(yf, yu, _)) if xf === yf && xu === yu =>
          0.0
        case (KafkaOffsetRange(xf, xu, _), KafkaOffsetRange(yf, yu, _)) if xf <= yf && xu > yu =>
          1.0
        case _ => Double.NaN
      }
}

@JsonCodec
final case class KafkaLagBehind private (current: KafkaOffset, end: KafkaOffset, lag: Long)
object KafkaLagBehind {
  def apply(current: KafkaOffset, end: KafkaOffset): KafkaLagBehind =
    KafkaLagBehind(current, end, end - current)
}

final case class ListOfTopicPartitions(value: List[TopicPartition]) extends AnyVal {

  def javaTimed(ldt: NJTimestamp): util.Map[TopicPartition, lang.Long] =
    value.map(tp => tp -> ldt.javaLong).toMap.asJava

  def asJava: util.List[TopicPartition] = value.asJava
}

object ListOfTopicPartitions {
  implicit val codecTopicPartition: Codec[TopicPartition] = new Codec[TopicPartition] {
    override def apply(a: TopicPartition): Json =
      Json.obj("topic" -> Json.fromString(a.topic()), "partition" -> Json.fromInt(a.partition()))

    override def apply(c: HCursor): Result[TopicPartition] =
      for {
        topic <- c.downField("topic").as[String]
        partition <- c.downField("partition").as[Int]
      } yield new TopicPartition(topic, partition)
  }

  implicit val codecListOfTopicPartitions: Codec[ListOfTopicPartitions] = new Codec[ListOfTopicPartitions] {
    override def apply(a: ListOfTopicPartitions): Json =
      Encoder.encodeList[TopicPartition].apply(a.value.sortBy(_.partition()))
    override def apply(c: HCursor): Result[ListOfTopicPartitions] =
      Decoder.decodeList[TopicPartition].apply(c).map(ListOfTopicPartitions(_))
  }
}

final case class KafkaTopicPartition[V](value: Map[TopicPartition, V]) extends AnyVal {
  def nonEmpty: Boolean = value.nonEmpty
  def isEmpty: Boolean  = value.isEmpty

  def get(tp: TopicPartition): Option[V] = value.get(tp)

  def get(topic: String, partition: Int): Option[V] =
    value.get(new TopicPartition(topic, partition))

  def mapValues[W](f: V => W): KafkaTopicPartition[W] =
    copy(value = value.view.mapValues(f).toMap)

  def map[W](f: (TopicPartition, V) => W): KafkaTopicPartition[W] =
    copy(value = value.map { case (k, v) => k -> f(k, v) })

  def intersectCombine[U, W](other: KafkaTopicPartition[U])(fn: (V, U) => W): KafkaTopicPartition[W] = {
    val res: List[(TopicPartition, W)] = value.keySet.intersect(other.value.keySet).toList.flatMap { tp =>
      (value.get(tp), other.value.get(tp)).mapN((f, s) => tp -> fn(f, s))
    }
    KafkaTopicPartition(res.toMap)
  }

  def leftCombine[U, W](other: KafkaTopicPartition[U])(
    fn: (V, U) => Option[W]): KafkaTopicPartition[Option[W]] = {
    val res: Map[TopicPartition, Option[W]] =
      value.map { case (tp, v) =>
        tp -> other.value.get(tp).flatMap(fn(v, _))
      }
    KafkaTopicPartition(res)
  }

  def topicPartitions: ListOfTopicPartitions = ListOfTopicPartitions(value.keys.toList)
}

object KafkaTopicPartition {
  import io.circe.generic.auto.*

  final private case class TPV[V](topic: String, partition: Int, value: V)

  implicit def codecKafkaTopicPartition[V: Encoder: Decoder]: Codec[KafkaTopicPartition[V]] =
    new Codec[KafkaTopicPartition[V]] {
      override def apply(a: KafkaTopicPartition[V]): Json =
        Encoder
          .encodeList[TPV[V]]
          .apply(
            a.value.map { case (tp, v) => TPV(tp.topic(), tp.partition(), v) }.toList.sortBy(_.partition))
      override def apply(c: HCursor): Result[KafkaTopicPartition[V]] =
        Decoder
          .decodeList[TPV[V]]
          .map(_.map(tpv => new TopicPartition(tpv.topic, tpv.partition) -> tpv.value).toMap)
          .map(KafkaTopicPartition[V])
          .apply(c)
    }

  implicit final class KafkaTopicPartitionOps1[V](private val self: KafkaTopicPartition[Option[V]]) {
    def flatten: KafkaTopicPartition[V] =
      self.copy(value = self.value.mapFilter(identity))
  }

  implicit final class KafkaTopicPartitionOps2(
    private val self: KafkaTopicPartition[Option[OffsetAndTimestamp]]) {
    def offsets: KafkaTopicPartition[Option[KafkaOffset]] =
      self.copy(value = self.value.view.mapValues(_.map(KafkaOffset(_))).toMap)
  }

  def empty[V]: KafkaTopicPartition[V]              = KafkaTopicPartition(Map.empty[TopicPartition, V])
  val emptyOffset: KafkaTopicPartition[KafkaOffset] = empty[KafkaOffset]
}
