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

sealed abstract case class KafkaOffsetRange private (from: KafkaOffset, until: KafkaOffset) {
  val distance: Long = until - from

  override def toString: String =
    s"KafkaOffsetRange(from=${from.value}, until=${until.value}, distance=$distance)"
}

object KafkaOffsetRange {
  implicit val codecKafkaOffsetRange: Codec[KafkaOffsetRange] = new Codec[KafkaOffsetRange] {
    override def apply(a: KafkaOffsetRange): Json = Json.obj(
      "from" -> Json.fromLong(a.from.value),
      "until" -> Json.fromLong(a.until.value),
      "distance" -> Json.fromLong(a.distance)
    )
    override def apply(c: HCursor): Result[KafkaOffsetRange] =
      for {
        from <- c.downField("from").as[Long]
        until <- c.downField("until").as[Long]
      } yield new KafkaOffsetRange(KafkaOffset(from), KafkaOffset(until)) {}
  }

  def apply(from: KafkaOffset, until: KafkaOffset): Option[KafkaOffsetRange] =
    if (from < until)
      Some(new KafkaOffsetRange(from, until) {})
    else
      None

  implicit val poKafkaOffsetRange: PartialOrder[KafkaOffsetRange] =
    (x: KafkaOffsetRange, y: KafkaOffsetRange) =>
      (x, y) match {
        case (KafkaOffsetRange(xf, xu), KafkaOffsetRange(yf, yu)) if xf >= yf && xu < yu =>
          -1.0
        case (KafkaOffsetRange(xf, xu), KafkaOffsetRange(yf, yu)) if xf === yf && xu === yu =>
          0.0
        case (KafkaOffsetRange(xf, xu), KafkaOffsetRange(yf, yu)) if xf <= yf && xu > yu =>
          1.0
        case _ => Double.NaN
      }
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

  def combineWith[W](other: KafkaTopicPartition[V])(fn: (V, V) => W): KafkaTopicPartition[W] = {
    val res = value.keySet.intersect(other.value.keySet).toList.flatMap { tp =>
      (value.get(tp), other.value.get(tp)).mapN((f, s) => tp -> fn(f, s))
    }
    KafkaTopicPartition(res.toMap)
  }

  def topicPartitions: ListOfTopicPartitions = ListOfTopicPartitions(value.keys.toList)
}

object KafkaTopicPartition {
  import io.circe.generic.auto.*

  final private case class TPV[V](topic: String, partition: Int, value: V)

  @annotation.nowarn
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
      self.copy(value = self.value.view.mapValues(_.map(x => KafkaOffset(x.offset))).toMap)
  }

  def empty[V]: KafkaTopicPartition[V]              = KafkaTopicPartition(Map.empty[TopicPartition, V])
  val emptyOffset: KafkaTopicPartition[KafkaOffset] = empty[KafkaOffset]
}

@JsonCodec
final case class KafkaConsumerGroupInfo(
  groupId: KafkaGroupId,
  lag: KafkaTopicPartition[Option[KafkaOffsetRange]])

object KafkaConsumerGroupInfo {

  def apply(
    groupId: KafkaGroupId,
    end: KafkaTopicPartition[Option[KafkaOffset]],
    offsetMeta: Map[TopicPartition, OffsetAndMetadata]): KafkaConsumerGroupInfo = {
    val gaps: Map[TopicPartition, Option[KafkaOffsetRange]] = offsetMeta.map { case (tp, om) =>
      end.get(tp).flatten.map(e => tp -> KafkaOffsetRange(KafkaOffset(om.offset()), e))
    }.toList.flatten.toMap
    new KafkaConsumerGroupInfo(groupId, KafkaTopicPartition(gaps))
  }
}
