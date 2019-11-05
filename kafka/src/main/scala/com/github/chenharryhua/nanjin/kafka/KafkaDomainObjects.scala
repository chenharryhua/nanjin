package com.github.chenharryhua.nanjin.kafka

import java.{lang, util}

import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

final case class KafkaOffset(value: Long) extends AnyVal {
  def javaLong: java.lang.Long = value
  def asLast: KafkaOffset      = copy(value = value - 1) //represent last message
}

final case class KafkaPartition(value: Int) extends AnyVal

final case class KafkaOffsetRange(from: KafkaOffset, until: KafkaOffset) {
  val distance: Long = until.value - from.value
}

final case class ListOfTopicPartitions(value: List[TopicPartition]) extends AnyVal {

  def javaTimed(ldt: NJTimestamp): util.Map[TopicPartition, lang.Long] =
    value.map(tp => tp -> ldt.javaLong).toMap.asJava

  def asJava: util.List[TopicPartition] = value.asJava
}

@Lenses final case class GenericTopicPartition[V](value: Map[TopicPartition, V]) extends AnyVal {
  def nonEmpty: Boolean = value.nonEmpty
  def isEmpty: Boolean  = value.isEmpty

  def get(tp: TopicPartition): Option[V] = value.get(tp)

  def get(topic: String, partition: Int): Option[V] =
    value.get(new TopicPartition(topic, partition))

  def mapValues[W](f: V => W): GenericTopicPartition[W] =
    copy(value = value.mapValues(f))

  def map[W](f: (TopicPartition, V) => W): GenericTopicPartition[W] =
    copy(value = value.map { case (k, v) => k -> f(k, v) })

  def combineWith[W](other: GenericTopicPartition[V])(fn: (V, V) => W): GenericTopicPartition[W] = {
    val res = value.keySet.intersect(other.value.keySet).toList.flatMap { tp =>
      (value.get(tp), other.value.get(tp)).mapN((f, s) => tp -> fn(f, s))
    }
    GenericTopicPartition(res.toMap)
  }

  def flatten[W](implicit ev: V =:= Option[W]): GenericTopicPartition[W] =
    copy(value = value.mapValues(ev).mapFilter(identity))

  def topicPartitions: ListOfTopicPartitions = ListOfTopicPartitions(value.keys.toList)

  def offsets(implicit ev: V =:= Option[OffsetAndMetadata]): GenericTopicPartition[KafkaOffset] =
    flatten[OffsetAndMetadata].mapValues(x => KafkaOffset(x.offset))
}

final case class KafkaConsumerGroupId(value: String) extends AnyVal

final case class KafkaConsumerGroupInfo(
  groupId: KafkaConsumerGroupId,
  lag: GenericTopicPartition[KafkaOffsetRange])

object KafkaConsumerGroupInfo {

  def apply(
    groupId: String,
    end: GenericTopicPartition[Option[KafkaOffset]],
    offsetMeta: Map[TopicPartition, OffsetAndMetadata]): KafkaConsumerGroupInfo = {
    val gaps = offsetMeta.map {
      case (tp, om) =>
        end.get(tp).flatten.map(e => tp -> KafkaOffsetRange(KafkaOffset(om.offset()), e))
    }.toList.flatten.toMap
    new KafkaConsumerGroupInfo(KafkaConsumerGroupId(groupId), GenericTopicPartition(gaps))
  }
}
