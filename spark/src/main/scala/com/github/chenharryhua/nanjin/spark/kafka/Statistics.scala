package com.github.chenharryhua.nanjin.spark.kafka

import cats.Show
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.datetime.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import io.circe.generic.JsonCodec
import org.apache.spark.sql.Dataset

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

final private[kafka] case class MinutelyAggResult(minute: Int, count: Int)
final private[kafka] case class HourlyAggResult(hour: Int, count: Int)
final private[kafka] case class DailyAggResult(date: LocalDate, count: Int)
final private[kafka] case class DailyHourAggResult(dateTime: String, count: Int)
final private[kafka] case class DailyMinuteAggResult(dateTime: String, count: Int)

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
    count,
    NJTimestamp(startTs).atZone(zoneId),
    NJTimestamp(endTs).atZone(zoneId),
    distance,
    count - distance,
    DurationFormatter.defaultFormatter.format(timeDistance)
  )
}

@JsonCodec
final case class KafkaSummary(
  partition: Int,
  start_offset: Long,
  end_offset: Long,
  count: Long,
  start_ts: ZonedDateTime,
  end_ts: ZonedDateTime,
  distance: Long,
  gap: Long,
  period: String) {}

object KafkaSummary {
  implicit val showKafkaSummary: Show[KafkaSummary] = ks =>
    s"""
       |partition:     ${ks.partition}
       |first offset:  ${ks.start_offset}
       |last offset:   ${ks.end_offset}
       |distance:      ${ks.distance}
       |count:         ${ks.count}
       |gap:           ${ks.gap} (${if (ks.gap == 0) "perfect"
    else if (ks.gap < 0) "probably lost data or its a compact topic"
    else "oops how is it possible"})
       |first TS:      ${ks.start_ts} not necessarily of the first offset)
       |last TS:       ${ks.end_ts} not necessarily of the last offset)
       |period:        ${ks.period}
       |""".stripMargin
}

final case class MissingOffset(partition: Int, offset: Long)

final case class Disorder(
  partition: Int,
  offset: Long,
  timestamp: Long,
  ts: String,
  nextTS: Long,
  msGap: Long,
  tsType: Int)

final case class DuplicateRecord(partition: Int, offset: Long, num: Long)

final class Statistics[F[_]] private[kafka] (
  ds: Dataset[CRMetaInfo],
  zoneId: ZoneId,
  rowNum: Int = 1500,
  isTruncate: Boolean = false)
    extends Serializable {

  def rows(rowNum: Int): Statistics[F] = new Statistics[F](ds, zoneId, rowNum, isTruncate)
  def truncate: Statistics[F]          = new Statistics[F](ds, zoneId, rowNum, true)
  def untruncate: Statistics[F]        = new Statistics[F](ds, zoneId, rowNum, false)

  def minutely(implicit F: Sync[F]): F[Unit] = {
    import ds.sparkSession.implicits.*
    F.delay(
      ds.map(m => NJTimestamp(m.timestamp).atZone(zoneId).getMinute)
        .groupByKey(identity)
        .mapGroups((m, iter) => MinutelyAggResult(m, iter.size))
        .orderBy("minute")
        .show(rowNum, isTruncate))
  }

  def hourly(implicit F: Sync[F]): F[Unit] = {
    import ds.sparkSession.implicits.*
    F.delay(
      ds.map(m => NJTimestamp(m.timestamp).atZone(zoneId).getHour)
        .groupByKey(identity)
        .mapGroups((m, iter) => HourlyAggResult(m, iter.size))
        .orderBy("hour")
        .show(rowNum, isTruncate))
  }

  def daily(implicit F: Sync[F]): F[Unit] = {
    import ds.sparkSession.implicits.*
    F.delay(
      ds.map(m => NJTimestamp(m.timestamp).dayResolution(zoneId))
        .groupByKey(identity)
        .mapGroups((m, iter) => DailyAggResult(m, iter.size))
        .orderBy("date")
        .show(rowNum, isTruncate))
  }

  def dailyHour(implicit F: Sync[F]): F[Unit] = {
    import ds.sparkSession.implicits.*
    F.delay(
      ds.map(m => NJTimestamp(m.timestamp).hourResolution(zoneId).toString)
        .groupByKey(identity)
        .mapGroups((m, iter) => DailyHourAggResult(m, iter.size))
        .orderBy("dateTime")
        .show(rowNum, isTruncate))
  }

  def dailyMinute(implicit F: Sync[F]): F[Unit] = {
    import ds.sparkSession.implicits.*
    F.delay(
      ds.map(m => NJTimestamp(m.timestamp).minuteResolution(zoneId).toString)
        .groupByKey(identity)
        .mapGroups((m, iter) => DailyMinuteAggResult(m, iter.size))
        .orderBy("dateTime")
        .show(rowNum, isTruncate))
  }

  private def internalSummary: List[KafkaSummaryInternal] = {
    import ds.sparkSession.implicits.*
    import org.apache.spark.sql.functions.*
    ds.groupBy("partition")
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

  def summary: List[KafkaSummary] = internalSummary.map(_.toKafkaSummary(zoneId))

  /** Notes: offset is supposed to be monotonically increasing in a partition, except compact topic
    */
  @SuppressWarnings(Array("UnnecessaryConversion")) // convert java long to scala long
  def missingOffsets: Dataset[MissingOffset] = {
    import ds.sparkSession.implicits.*
    import org.apache.spark.sql.functions.col
    val all: List[Dataset[MissingOffset]] = internalSummary.map { kds =>
      val expect: Dataset[Long] = ds.sparkSession.range(kds.startOffset, kds.endOffset + 1L).map(_.toLong)
      val exist: Dataset[Long]  = ds.filter(col("partition") === kds.partition).map(_.offset)
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
        val curr: Dataset[(Long, CRMetaInfo)] = ds.filter(ds("partition") === pt).map(x => (x.offset, x))
        val pre: Dataset[(Long, CRMetaInfo)]  = curr.map { case (index, crm) => (index + 1, crm) }

        curr.joinWith(pre, curr("_1") === pre("_1"), "inner").flatMap { case ((_, c), (_, p)) =>
          if (c.timestamp >= p.timestamp) None
          else
            Some(Disorder(
              partition = pt,
              offset = p.offset,
              timestamp = p.timestamp,
              ts = NJTimestamp(p.timestamp).atZone(zoneId).toString,
              nextTS = c.timestamp,
              msGap = p.timestamp - c.timestamp,
              tsType = p.timestampType
            ))
        }
      }
    all.foldLeft(ds.sparkSession.emptyDataset[Disorder])(_.union(_)).orderBy(col("partition").asc, col("offset").asc)
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
