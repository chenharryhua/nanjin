package com.github.chenharryhua.nanjin.kafka

import java.sql.Timestamp
import java.time._
import java.util.concurrent.TimeUnit
import java.{lang, util}

import cats.implicits._
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}

final case class KafkaOffset(value: Long) extends AnyVal {
  def javaLong: java.lang.Long = value
  def asLast: KafkaOffset      = copy(value = value - 1) //represent last message
}

final case class KafkaPartition(value: Int) extends AnyVal

final case class KafkaOffsetRange(from: KafkaOffset, until: KafkaOffset) {
  val distance: Long = until.value - from.value
}

final case class ListOfTopicPartitions(value: List[TopicPartition]) extends AnyVal {

  def javaTimed(ldt: KafkaTimestamp): util.Map[TopicPartition, lang.Long] =
    value.map(tp => tp -> ldt.javaLong).toMap.asJava

  def asJava: util.List[TopicPartition] = value.asJava
}

@Lenses final case class GenericTopicPartition[V](value: Map[TopicPartition, V]) extends AnyVal {
  def nonEmpty: Boolean = value.nonEmpty
  def isEmpty: Boolean  = value.isEmpty

  def get(tp: TopicPartition): Option[V] = value.get(tp)

  def get(topic: String, partition: Int): Option[V] =
    value.get(new TopicPartition(topic, partition))

  def mapValues[W](f: V => W): GenericTopicPartition[W] = copy(value = value.mapValues(f))

  def combineWith[W](other: GenericTopicPartition[V])(fn: (V, V) => W): GenericTopicPartition[W] = {
    val res = value.keySet.intersect(other.value.keySet).toList.flatMap { tp =>
      (value.get(tp), other.value.get(tp)).mapN((f, s) => tp -> fn(f, s))
    }
    GenericTopicPartition(res.toMap)
  }

  def flatten[W](implicit ev: V =:= Option[W]): GenericTopicPartition[W] =
    copy(value = value.mapValues(ev).mapFilter(identity))

  def topicPartitions: ListOfTopicPartitions = ListOfTopicPartitions(value.keys.toList)
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

// in unit of milli-second
final case class KafkaTimestamp(milliseconds: Long, tz: ZoneId) {
  def instant: Instant         = Instant.ofEpochMilli(milliseconds)
  def utc: ZonedDateTime       = instant.atZone(ZoneId.of("Etc/UTC"))
  def local: LocalDateTime     = instant.atZone(tz).toLocalDateTime()
  def javaLong: java.lang.Long = milliseconds
}

object KafkaTimestamp {
  private val zoneId: ZoneId = ZoneId.systemDefault()

  def apply(ts: Long): KafkaTimestamp          = KafkaTimestamp(ts, zoneId)
  def apply(ts: Timestamp): KafkaTimestamp     = KafkaTimestamp(ts.getTime, zoneId)
  def apply(ts: Instant): KafkaTimestamp       = KafkaTimestamp(ts.toEpochMilli)
  def apply(ts: LocalDateTime): KafkaTimestamp = apply(ts.atZone(zoneId).toInstant)
  def apply(ts: ZonedDateTime): KafkaTimestamp = apply(ts.toInstant)
  def apply(ts: LocalDate): KafkaTimestamp     = apply(LocalDateTime.of(ts, LocalTime.MIDNIGHT))
}

@Lenses final case class KafkaDateTimeRange(
  start: Option[KafkaTimestamp],
  end: Option[KafkaTimestamp]) {

  def withStart(ts: Long): KafkaDateTimeRange =
    KafkaDateTimeRange.start.set(Some(KafkaTimestamp(ts)))(this)

  def withStart(ts: Timestamp): KafkaDateTimeRange =
    KafkaDateTimeRange.start.set(Some(KafkaTimestamp(ts)))(this)

  def withStart(ts: Instant): KafkaDateTimeRange =
    KafkaDateTimeRange.start.set(Some(KafkaTimestamp(ts)))(this)

  def withStart(ts: LocalDateTime): KafkaDateTimeRange =
    KafkaDateTimeRange.start.set(Some(KafkaTimestamp(ts)))(this)

  def withStart(ts: ZonedDateTime): KafkaDateTimeRange =
    KafkaDateTimeRange.start.set(Some(KafkaTimestamp(ts)))(this)

  def withStart(ts: LocalDate): KafkaDateTimeRange =
    KafkaDateTimeRange.start.set(Some(KafkaTimestamp(ts)))(this)

  def withEnd(ts: Long): KafkaDateTimeRange =
    KafkaDateTimeRange.end.set(Some(KafkaTimestamp(ts)))(this)

  def withEnd(ts: Timestamp): KafkaDateTimeRange =
    KafkaDateTimeRange.end.set(Some(KafkaTimestamp(ts)))(this)

  def withEnd(ts: Instant): KafkaDateTimeRange =
    KafkaDateTimeRange.end.set(Some(KafkaTimestamp(ts)))(this)

  def withEnd(ts: LocalDateTime): KafkaDateTimeRange =
    KafkaDateTimeRange.end.set(Some(KafkaTimestamp(ts)))(this)

  def withEnd(ts: ZonedDateTime): KafkaDateTimeRange =
    KafkaDateTimeRange.end.set(Some(KafkaTimestamp(ts)))(this)

  def withEnd(ts: LocalDate): KafkaDateTimeRange =
    KafkaDateTimeRange.end.set(Some(KafkaTimestamp(ts)))(this)

  def isInBetween(ts: Long): Boolean = (start, end) match {
    case (Some(s), Some(e)) => ts >= s.milliseconds && ts < e.milliseconds
    case (Some(s), None)    => ts >= s.milliseconds
    case (None, Some(e))    => ts < e.milliseconds
    case (None, None)       => true
  }

  val duration: Option[FiniteDuration] =
    (start, end).mapN((s, e) => Duration(e.milliseconds - s.milliseconds, TimeUnit.MILLISECONDS))

  require(
    duration.forall(_.length > 0),
    s"start time(${start.map(_.local)}) should be strictly before end time(${end.map(_.local)}).")
}
