package com.github.chenharryhua.nanjin.spark.kafka

import cats.Show
import cats.implicits.catsSyntaxEq
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.datetime.{dayResolution, hourResolution, minuteResolution}
import io.circe.generic.JsonCodec
import org.apache.spark.sql.Dataset
import org.typelevel.cats.time.instances.{localdatetime, zoneid}

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

final case class MinutelyAggResult(minute: Int, count: Int)
final case class HourlyAggResult(hour: Int, count: Int)
final case class DailyAggResult(date: LocalDate, count: Int)
final case class DailyHourAggResult(dateTime: String, count: Int)
final case class DailyMinuteAggResult(dateTime: String, count: Int)

final private case class KafkaSummaryInternal(
  partition: Int,
  startOffset: Long,
  endOffset: Long,
  count: Long,
  startTs: Long,
  endTs: Long) {
  val distance: Long               = endOffset - startOffset + 1L
  val timeDistance: FiniteDuration = FiniteDuration(endTs - startTs, MILLISECONDS)

  def toKafkaSummary(zoneId: ZoneId): KafkaSummary = KafkaSummary(
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
  implicit val showKafkaSummary: Show[KafkaSummary] = ks =>
    s"""
       |partition:     ${ks.partition}
       |first_offset:  ${ks.start_offset}
       |last_offset:   ${ks.end_offset}
       |distance:      ${ks.offset_distance}
       |count:         ${ks.records_count}
       |gap:           ${ks.count_distance_gap} (${if (ks.count_distance_gap == 0) "perfect"
      else if (ks.count_distance_gap < 0) "probably lost data or its a compact topic"
      else "duplicates in the dataset"})
       |zone_id:       ${ks.zone_id.show}
       |start_ts:      ${ks.start_ts.show} (not necessary of the first offset)
       |end_ts:        ${ks.end_ts.show} (not necessary of the last offset)
       |period:        ${ks.period}
       |""".stripMargin
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

final class Statistics private[kafka] (ds: Dataset[CRMetaInfo], zoneId: ZoneId) extends Serializable {

  def minutely: Dataset[MinutelyAggResult] = {
    import ds.sparkSession.implicits.*
    ds.map(m => m.localDateTime(zoneId).getMinute)
      .groupByKey(identity)
      .mapGroups((m, iter) => MinutelyAggResult(m, iter.size))
      .orderBy("minute")
  }

  def hourly: Dataset[HourlyAggResult] = {
    import ds.sparkSession.implicits.*
    ds.map(m => m.localDateTime(zoneId).getHour)
      .groupByKey(identity)
      .mapGroups((m, iter) => HourlyAggResult(m, iter.size))
      .orderBy("hour")
  }

  def daily: Dataset[DailyAggResult] = {
    import ds.sparkSession.implicits.*
    ds.map(m => dayResolution(m.localDateTime(zoneId)))
      .groupByKey(identity)
      .mapGroups((m, iter) => DailyAggResult(m, iter.size))
      .orderBy("date")
  }

  def dailyHour: Dataset[DailyHourAggResult] = {
    import ds.sparkSession.implicits.*
    ds.map(m => hourResolution(m.localDateTime(zoneId)).toString)
      .groupByKey(identity)
      .mapGroups((m, iter) => DailyHourAggResult(m, iter.size))
      .orderBy("dateTime")
  }

  def dailyMinute: Dataset[DailyMinuteAggResult] = {
    import ds.sparkSession.implicits.*
    ds.map(m => minuteResolution(m.localDateTime(zoneId)).toString)
      .groupByKey(identity)
      .mapGroups((m, iter) => DailyMinuteAggResult(m, iter.size))
      .orderBy("dateTime")
  }

  private def internalSummary(ids: Dataset[CRMetaInfo]): List[KafkaSummaryInternal] = {
    import ids.sparkSession.implicits.*
    import org.apache.spark.sql.functions.{min, max, count, lit, asc}
    ids
      .groupBy("partition")
      .agg(
        min("offset").as("startOffset"),
        max("offset").as("endOffset"),
        count(lit(1)).as("count"),
        min("timestamp").as("startTs"),
        max("timestamp").as("endTs"))
      .as[KafkaSummaryInternal]
      .orderBy(asc("partition"))
      .collect()
      .toList
  }

  def summary: List[KafkaSummary] = internalSummary(ds).map(_.toKafkaSummary(zoneId))

  /** Notes: offset is supposed to be monotonically increasing in a partition, except compact topic
    */
  def missingOffsets: Dataset[MissingOffset] = {
    import ds.sparkSession.implicits.*
    import org.apache.spark.sql.functions.col
    val all: List[Dataset[MissingOffset]] = internalSummary(ds).map { kds =>
      val expect: Dataset[Long] = ds.sparkSession.range(kds.startOffset, kds.endOffset + 1L).map(_.toLong)
      val exist: Dataset[Long]  = ds.filter(_.partition === kds.partition).map(_.offset)
      expect.except(exist).map(os => MissingOffset(partition = kds.partition, offset = os))
    }
    all
      .foldLeft(ds.sparkSession.emptyDataset[MissingOffset])(_.union(_))
      .orderBy(col("partition").asc, col("offset").asc)
  }

  /** Notes:
    *
    * Timestamp is supposed to be ordered along with offset
    */
  def disorders: Dataset[Disorder] = {
    import ds.sparkSession.implicits.*
    import org.apache.spark.sql.functions.col
    val all: Array[Dataset[Disorder]] =
      ds.map(_.partition).distinct().collect().map { pt =>
        val curr: Dataset[(Long, CRMetaInfo)] = ds.filter(_.partition === pt).map(x => (x.offset, x))
        val pre: Dataset[(Long, CRMetaInfo)]  = curr.map { case (index, crm) => (index + 1, crm) }

        curr.joinWith(pre, curr("_1") === pre("_1"), "inner").flatMap { case ((_, c), (_, p)) =>
          if (c.timestamp >= p.timestamp) None
          else
            Some(Disorder(
              partition = pt,
              offset = p.offset,
              timestamp = p.timestamp,
              currTs = p.localDateTime(zoneId).toString,
              nextTS = c.localDateTime(zoneId).toString,
              msGap = p.timestamp - c.timestamp,
              tsType = p.timestampType
            ))
        }
      }
    all
      .foldLeft(ds.sparkSession.emptyDataset[Disorder])(_.union(_))
      .orderBy(col("partition").asc, col("offset").asc)
  }

  /** Notes: partition + offset supposed to be unique, of a topic
    */
  def dupRecords: Dataset[DuplicateRecord] = {
    import ds.sparkSession.implicits.*
    import org.apache.spark.sql.functions.{asc, col, count, lit}
    ds.groupBy(col("partition"), col("offset"))
      .agg(count(lit(1)))
      .as[(Int, Long, Long)]
      .flatMap { case (p, o, c) =>
        if (c > 1) Some(DuplicateRecord(p, o, c)) else None
      }
      .orderBy(asc("partition"), asc("offset"))
  }
}
