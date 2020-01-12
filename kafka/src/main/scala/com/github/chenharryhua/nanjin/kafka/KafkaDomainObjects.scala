package com.github.chenharryhua.nanjin.kafka

import java.{lang, util}

import cats.implicits._
import cats.{Order, PartialOrder, Show}
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import monocle.Iso
import monocle.macros.GenIso
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

final case class KafkaOffset(value: Long) extends AnyVal {
  def javaLong: java.lang.Long = value
  def asLast: KafkaOffset      = copy(value = value - 1) //represent last message
}

object KafkaOffset {
  implicit val orderKafkaOffset: Order[KafkaOffset] = cats.derived.semi.order[KafkaOffset]
}

final case class KafkaPartition(value: Int) extends AnyVal

object KafkaPartition {
  implicit val orderKafkaPartition: Order[KafkaPartition] = cats.derived.semi.order[KafkaPartition]
}

sealed abstract case class KafkaOffsetRange(from: KafkaOffset, until: KafkaOffset) {
  require(from < until, s"from should be strictly less than until. from = $from, until=$until")

  final val distance: Long = until.value - from.value

  final def show: String =
    s"KafkaOffsetRange(from = ${from.value}, until = ${until.value}, distance = $distance)"

  final override def toString: String = show
}

object KafkaOffsetRange {

  def apply(from: KafkaOffset, until: KafkaOffset): Option[KafkaOffsetRange] =
    if (from < until && from.value >= 0)
      Some(new KafkaOffsetRange(from, until) {})
    else
      None

  implicit val showKafkaOffsetRange: Show[KafkaOffsetRange] = _.show

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

final case class NJTopicPartition[V](value: Map[TopicPartition, V]) extends AnyVal {
  def nonEmpty: Boolean = value.nonEmpty
  def isEmpty: Boolean  = value.isEmpty

  def get(tp: TopicPartition): Option[V] = value.get(tp)

  def get(topic: String, partition: Int): Option[V] =
    value.get(new TopicPartition(topic, partition))

  def mapValues[W](f: V => W): NJTopicPartition[W] =
    copy(value = value.mapValues(f))

  def map[W](f: (TopicPartition, V) => W): NJTopicPartition[W] =
    copy(value = value.map { case (k, v) => k -> f(k, v) })

  def combineWith[W](other: NJTopicPartition[V])(fn: (V, V) => W): NJTopicPartition[W] = {
    val res = value.keySet.intersect(other.value.keySet).toList.flatMap { tp =>
      (value.get(tp), other.value.get(tp)).mapN((f, s) => tp -> fn(f, s))
    }
    NJTopicPartition(res.toMap)
  }

  def flatten[W](implicit ev: V =:= Option[W]): NJTopicPartition[W] =
    copy(value = value.mapValues(ev).mapFilter(identity))

  def topicPartitions: ListOfTopicPartitions = ListOfTopicPartitions(value.keys.toList)

  def offsets(
    implicit ev: V =:= Option[OffsetAndTimestamp]): NJTopicPartition[Option[KafkaOffset]] =
    copy(value = value.mapValues(_.map(x => KafkaOffset(x.offset))))
}

object NJTopicPartition {

  implicit def isoGenericTopicPartition[V]: Iso[NJTopicPartition[V], Map[TopicPartition, V]] =
    GenIso[NJTopicPartition[V], Map[TopicPartition, V]]
}

final case class KafkaConsumerGroupId(value: String) extends AnyVal

final case class KafkaConsumerGroupInfo(
  groupId: KafkaConsumerGroupId,
  lag: NJTopicPartition[Option[KafkaOffsetRange]])

object KafkaConsumerGroupInfo {

  def apply(
    groupId: String,
    end: NJTopicPartition[Option[KafkaOffset]],
    offsetMeta: Map[TopicPartition, OffsetAndMetadata]): KafkaConsumerGroupInfo = {
    val gaps: Map[TopicPartition, Option[KafkaOffsetRange]] = offsetMeta.map {
      case (tp, om) =>
        end.get(tp).flatten.map(e => tp -> KafkaOffsetRange(KafkaOffset(om.offset()), e))
    }.toList.flatten.toMap
    new KafkaConsumerGroupInfo(KafkaConsumerGroupId(groupId), NJTopicPartition(gaps))
  }
}
