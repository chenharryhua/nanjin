package com.github.chenharryhua.nanjin.kafka

import java.time.LocalDateTime
import java.{lang, util}

import cats.Show
import com.github.chenharryhua.nanjin.codec.KafkaTimestamp
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
import cats.implicits._

final case class KafkaOffset(value: Long) extends AnyVal {
  def javaLong: java.lang.Long = value
}

final case class KafkaPartition(value: Int) extends AnyVal

final case class KafkaOffsetRange(fromOffset: Long, untilOffset: Long) {
  val size: Long = untilOffset - fromOffset
}

object KafkaOffsetRange {
  implicit val showKafkaOffsetRange: Show[KafkaOffsetRange] =
    cats.derived.semi.show[KafkaOffsetRange]
}

final case class ListOfTopicPartitions(value: List[TopicPartition]) extends AnyVal {

  def timed(ldt: LocalDateTime): util.Map[TopicPartition, lang.Long] =
    value.map(tp => tp -> KafkaTimestamp(ldt).javaLong).toMap.asJava
  def asJava: util.List[TopicPartition] = value.asJava

  def show: String = value.sortBy(_.partition()).mkString("\n")
}

object ListOfTopicPartitions {
  implicit val showListOfTopicPartitions: Show[ListOfTopicPartitions] = _.show
}

final case class GenericTopicPartition[V](value: Map[TopicPartition, V]) extends AnyVal {
  def get(tp: TopicPartition): Option[V] = value.get(tp)

  def get(topic: String, partition: Int): Option[V] =
    value.get(new TopicPartition(topic, partition))

  def mapValues[W](f: V => W): GenericTopicPartition[W] = copy(value = value.mapValues(f))

  def combine[W](other: GenericTopicPartition[V])(fn: (V, V) => W): GenericTopicPartition[W] = {
    val ret: List[Option[(TopicPartition, W)]] =
      (value.keySet ++ other.value.keySet).toList.map { tp =>
        (value.get(tp), other.value.get(tp)).mapN((f, s) => tp -> fn(f, s))
      }
    GenericTopicPartition(ret.flatten.toMap)
  }

  def flatten[W](implicit ev: V =:= Option[W]): GenericTopicPartition[W] =
    copy(value = value.mapValues(ev).mapFilter(identity))

  def topicPartitions: ListOfTopicPartitions = ListOfTopicPartitions(value.keys.toList)

  def show: String =
    s"""|
        |GenericTopicPartition:
        |total partitions: ${value.size}
        |${value.toList
         .sortBy(_._1.partition())
         .map { case (k, v) => k.toString + " -> " + v.toString }
         .mkString("\n")}
  """.stripMargin
}

object GenericTopicPartition {
  implicit def showGenericTopicPartition[A]: Show[GenericTopicPartition[A]] = _.show
}
