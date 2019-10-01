package com.github.chenharryhua.nanjin.kafka

import java.time.LocalDateTime
import java.{lang, util}

import cats.Show
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.KafkaTimestamp
import monocle.macros.Lenses
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

final case class KafkaOffset(value: Long) extends AnyVal {
  def javaLong: java.lang.Long = value
  def asLast: KafkaOffset      = copy(value = value - 1) //represent last message
}

final case class KafkaPartition(value: Int) extends AnyVal

final case class KafkaOffsetRange(from: KafkaOffset, until: KafkaOffset) {
  val distance: Long = until.value - from.value

  def show: String =
    s"KafkaOffsetRange(from = ${from.value}, until = ${until.value}, distance = $distance)"
}

object KafkaOffsetRange {
  implicit val showKafkaOffsetRange: Show[KafkaOffsetRange] = _.show
}

final case class ListOfTopicPartitions(value: List[TopicPartition]) extends AnyVal {

  def javaTimed(ldt: LocalDateTime): util.Map[TopicPartition, lang.Long] =
    value.map(tp => tp -> KafkaTimestamp(ldt).javaLong).toMap.asJava

  def asJava: util.List[TopicPartition] = value.asJava

  def show: String =
    value
      .sortBy(_.partition())
      .map(tp => s"TopicPartition(topic = ${tp.topic()}, partition = ${tp.partition()}")
      .mkString("\n")
}

object ListOfTopicPartitions {
  implicit val showListOfTopicPartitions: Show[ListOfTopicPartitions] = _.show
}

@Lenses final case class GenericTopicPartition[V](value: Map[TopicPartition, V]) extends AnyVal {
  def nonEmpty: Boolean = value.nonEmpty
  def isEmpty: Boolean  = value.isEmpty

  def get(tp: TopicPartition): Option[V] = value.get(tp)

  def get(topic: String, partition: Int): Option[V] =
    value.get(new TopicPartition(topic, partition))

  def mapValues[W](f: V => W): GenericTopicPartition[W] = copy(value = value.mapValues(f))

  def combineWith[W](other: GenericTopicPartition[V])(fn: (V, V) => W): GenericTopicPartition[W] = {
    val ret: List[(TopicPartition, W)] =
      (value.keySet ++ other.value.keySet).toList.flatMap { tp =>
        (value.get(tp), other.value.get(tp)).mapN((f, s) => tp -> fn(f, s))
      }
    GenericTopicPartition(ret.toMap)
  }

  def flatten[W](implicit ev: V =:= Option[W]): GenericTopicPartition[W] =
    copy(value = value.mapValues(ev).mapFilter(identity))

  def topicPartitions: ListOfTopicPartitions = ListOfTopicPartitions(value.keys.toList)

  def show(implicit ev: Show[V]): String =
    s"""|
        |total partitions: ${value.size}
        |${value.toList
         .sortBy(_._1.partition())
         .map { case (k, v) => k.toString + " -> " + v.show }
         .mkString("\n")}
  """.stripMargin
}

object GenericTopicPartition {
  implicit def showGenericTopicPartition[A: Show]: Show[GenericTopicPartition[A]] = _.show
}
