package com.github.chenharryhua.nanjin.kafka

import cats.syntax.all.*
import cats.{Order, PartialOrder}
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.refineV
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import java.{lang, util}
import scala.jdk.CollectionConverters.*

final case class KafkaGroupId(value: String) extends AnyVal

final case class KafkaOffset(offset: Refined[Long, NonNegative]) {
  val value: Long                 = offset.value
  val javaLong: java.lang.Long    = value
  def asLast: KafkaOffset         = KafkaOffset(value - 1) // represent last message
  def -(other: KafkaOffset): Long = value - other.value
}

object KafkaOffset {

  @throws[Exception]
  def apply(v: Long): KafkaOffset =
    refineV[NonNegative](v).map(KafkaOffset(_)).fold(ex => throw new Exception(ex), identity)

  implicit val orderKafkaOffset: Order[KafkaOffset] =
    (x: KafkaOffset, y: KafkaOffset) => x.value.compareTo(y.value)
}

final case class KafkaPartition(partition: Refined[Int, NonNegative]) {
  val value: Int                    = partition.value
  def -(other: KafkaPartition): Int = value - other.value
}

object KafkaPartition {

  @throws[Exception]
  def apply(v: Int): KafkaPartition =
    refineV[NonNegative](v).map(KafkaPartition(_)).fold(ex => throw new Exception(ex), identity)

  implicit val orderKafkaPartition: Order[KafkaPartition] =
    (x: KafkaPartition, y: KafkaPartition) => x.value.compareTo(y.value)
}

sealed abstract case class KafkaOffsetRange private (from: KafkaOffset, until: KafkaOffset) {
  // require(from < until, s"from should be strictly less than until. from = $from, until=$until")

  val distance: Long = until - from

  override def toString: String =
    s"KafkaOffsetRange(from=${from.value}, until=${until.value}, distance=$distance)"
}

object KafkaOffsetRange {

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

final case class ListOfTopicPartitions(value: List[TopicPartition]) {

  def javaTimed(ldt: NJTimestamp): util.Map[TopicPartition, lang.Long] =
    value.map(tp => tp -> ldt.javaLong).toMap.asJava

  def asJava: util.List[TopicPartition] = value.asJava
}

final case class KafkaTopicPartition[V](value: Map[TopicPartition, V]) {
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

  implicit final class KafkaTopicPartitionOps1[V](private val self: KafkaTopicPartition[Option[V]])
      extends AnyVal {
    def flatten: KafkaTopicPartition[V] =
      self.copy(value = self.value.mapFilter(identity))
  }
  implicit final class KafkaTopicPartitionOps2(
    private val self: KafkaTopicPartition[Option[OffsetAndTimestamp]])
      extends AnyVal {
    def offsets: KafkaTopicPartition[Option[KafkaOffset]] =
      self.copy(value = self.value.view.mapValues(_.map(x => KafkaOffset(x.offset))).toMap)
  }

  def empty[V]: KafkaTopicPartition[V]              = KafkaTopicPartition(Map.empty[TopicPartition, V])
  val emptyOffset: KafkaTopicPartition[KafkaOffset] = empty[KafkaOffset]
}

final case class KafkaConsumerGroupInfo(
  groupId: KafkaGroupId,
  lag: KafkaTopicPartition[Option[KafkaOffsetRange]])

object KafkaConsumerGroupInfo {

  def apply(
    groupId: String,
    end: KafkaTopicPartition[Option[KafkaOffset]],
    offsetMeta: Map[TopicPartition, OffsetAndMetadata]): KafkaConsumerGroupInfo = {
    val gaps: Map[TopicPartition, Option[KafkaOffsetRange]] = offsetMeta.map { case (tp, om) =>
      end.get(tp).flatten.map(e => tp -> KafkaOffsetRange(KafkaOffset(om.offset()), e))
    }.toList.flatten.toMap
    new KafkaConsumerGroupInfo(KafkaGroupId(groupId), KafkaTopicPartition(gaps))
  }
}
