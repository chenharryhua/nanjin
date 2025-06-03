package com.github.chenharryhua.nanjin.spark.kafka

import cats.Show
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.DurationFormatter
import io.circe.generic.JsonCodec
import org.typelevel.cats.time.instances.{localdatetime, zoneid}

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

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
  topic: String) {
  private val distance: Long = endOffset - startOffset + 1L
  private val timeDistance: FiniteDuration = FiniteDuration(endTs - startTs, MILLISECONDS)

  def toKafkaSummary(zoneId: ZoneId): KafkaSummary = KafkaSummary(
    topic,
    partition,
    startOffset,
    endOffset,
    distance,
    count,
    count - distance,
    zoneId,
    Instant.ofEpochMilli(startTs).atZone(zoneId).toLocalDateTime,
    Instant.ofEpochMilli(endTs).atZone(zoneId).toLocalDateTime,
    DurationFormatter.defaultFormatter.format(timeDistance)
  )
}

@JsonCodec
final case class KafkaSummary(
  topic: String,
  partition: Int,
  start_offset: Long,
  end_offset: Long,
  offset_distance: Long,
  records_count: Long,
  count_distance_gap: Long,
  zone_id: ZoneId,
  start_ts: LocalDateTime,
  end_ts: LocalDateTime,
  period: String)

object KafkaSummary extends localdatetime with zoneid {
  implicit val showKafkaSummary: Show[KafkaSummary] = ks => {
    val gap: String =
      if (ks.count_distance_gap == 0) "perfect"
      else if (ks.count_distance_gap < 0) "probably lost data or its a compact topic"
      else "duplicates in the dataset"
    show"""
          |topic:         ${ks.topic}
          |partition:     ${ks.partition}
          |first_offset:  ${ks.start_offset}
          |last_offset:   ${ks.end_offset}
          |distance:      ${ks.offset_distance}
          |count:         ${ks.records_count}
          |gap:           ${ks.count_distance_gap} ($gap)
          |zone_id:       ${ks.zone_id}
          |start_ts:      ${ks.start_ts} (not necessary of the first offset)
          |end_ts:        ${ks.end_ts} (not necessary of the last offset)
          |period:        ${ks.period}
          |""".stripMargin
  }
}

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
