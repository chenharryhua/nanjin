package com.github.chenharryhua.nanjin.spark.kafka

import io.circe.generic.JsonCodec

import java.time.{LocalDate, LocalDateTime, ZoneId}
import scala.collection.immutable.TreeMap

final case class MinutelyResult(minute: Int, count: Int)
final case class HourlyResult(hour: Int, count: Int)
final case class DailyResult(date: LocalDate, count: Int)
final case class DailyHourResult(dateTime: String, count: Int)
final case class DailyMinuteResult(dateTime: String, count: Int)

final private case class KafkaSummaryInternal(
  partition: Int,
  startOffset: Long,
  endOffset: Long,
  count: Long,
  startTs: Long,
  endTs: Long,
  topic: String)

@JsonCodec
final case class PartitionSummary(
  start_offset: Long,
  cease_offset: Long,
  offset_distance: Long,
  records_counted: Long,
  count_distance_gap: Long,
  start_ts: LocalDateTime,
  cease_ts: LocalDateTime,
  period: String)

@JsonCodec
final case class TopicSummary(
  topic: String,
  total_records: Long,
  zone_id: ZoneId,
  start_ts: LocalDateTime,
  cease_ts: LocalDateTime,
  period: String,
  partitions: TreeMap[Int, PartitionSummary]
)

final case class MissingOffset(partition: Int, offset: Long)

final case class Disorder(
  partition: Int,
  offset: Long,
  timestamp: Long,
  currTs: String,
  nextTS: String,
  msGap: Long,
  tsType: Int)

final case class DuplicateRecord(partition: Int, offset: Long, num: Long)

final case class DisorderedKey[K](
  key: K,
  partition: Int,
  offset: Long,
  ts: Long,
  msGap: Long,
  offsetDistance: Long,
  nextPartition: Int,
  nextOffset: Long,
  nextTS: Long)

final case class MisplacedKey[K](key: Option[K], count: Long)
