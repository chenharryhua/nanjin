package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.datetime.*
import com.github.chenharryhua.nanjin.spark.injection.*
import frameless.functions.aggregate.count
import frameless.{Injection, TypedDataset}
import org.apache.spark.sql.Dataset

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

final private[kafka] case class MinutelyAggResult(minute: Int, count: Int)
final private[kafka] case class HourlyAggResult(hour: Int, count: Int)
final private[kafka] case class DailyAggResult(date: LocalDate, count: Int)
final private[kafka] case class DailyHourAggResult(dateTime: String, count: Int)
final private[kafka] case class DailyMinuteAggResult(dateTime: String, count: Int)

final case class KafkaSummary(
  partition: Int,
  startOffset: Long,
  endOffset: Long,
  count: Long,
  startTs: Long,
  endTs: Long) {
  val distance: Long               = endOffset - startOffset + 1L
  val gap: Long                    = count - distance
  val timeDistance: FiniteDuration = FiniteDuration(endTs - startTs, MILLISECONDS)

  def showData(zoneId: ZoneId): String =
    s"""
       |partition:     $partition
       |first offset:  $startOffset
       |last offset:   $endOffset
       |distance:      $distance
       |count:         $count
       |gap:           $gap (${if (gap == 0) "perfect"
    else if (gap < 0) "probably lost data or its a compact topic"
    else "oops how is it possible"})
       |first TS:      $startTs(${NJTimestamp(startTs).atZone(zoneId)} not necessarily of the first offset)
       |last TS:       $endTs(${NJTimestamp(endTs).atZone(zoneId)} not necessarily of the last offset)
       |time distance: ${timeDistance.toHours} Hours roughly
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

  implicit val zonedDateTimeInjection: Injection[ZonedDateTime, String] =
    new Injection[ZonedDateTime, String] {
      override def apply(a: ZonedDateTime): String  = a.toString
      override def invert(b: String): ZonedDateTime = ZonedDateTime.parse(b)
    }

  def typedDataset: TypedDataset[CRMetaInfo] = TypedDataset.create(ds)

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

  def summaryDS: TypedDataset[KafkaSummary] = {
    import frameless.functions.aggregate.{max, min}
    val tds = typedDataset.distinct
    val res = tds
      .groupBy(tds('partition))
      .agg(min(tds('offset)), max(tds('offset)), count(tds.asCol), min(tds('timestamp)), max(tds('timestamp)))
      .as[KafkaSummary]
    res.orderBy(res('partition).asc)
  }

  def summary(implicit F: Sync[F]): F[Unit] =
    F.delay(summaryDS.dataset.collect().foreach(x => println(x.showData(zoneId))))

  /** Notes: offset is supposed to be monotonically increasing in a partition, except compact topic
    */
  @SuppressWarnings(Array("UnnecessaryConversion")) // convert java long to scala long
  def missingOffsets: TypedDataset[MissingOffset] = {
    import ds.sparkSession.implicits.*
    import org.apache.spark.sql.functions.col
    val all: Array[Dataset[MissingOffset]] = summaryDS.dataset.collect().map { kds =>
      val expect: Dataset[Long] = ds.sparkSession.range(kds.startOffset, kds.endOffset + 1L).map(_.toLong)
      val exist: Dataset[Long]  = ds.filter(col("partition") === kds.partition).map(_.offset)
      expect.except(exist).map(os => MissingOffset(partition = kds.partition, offset = os))
    }
    val sum: Dataset[MissingOffset] = all
      .foldLeft(ds.sparkSession.emptyDataset[MissingOffset])(_.union(_))
      .orderBy(col("partition").asc, col("offset").asc)
    TypedDataset.create(sum)
  }

  /** Notes:
    *
    * Timestamp is supposed to be ordered along with offset
    */
  def disorders: TypedDataset[Disorder] = {
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
    val sum: Dataset[Disorder] =
      all.foldLeft(ds.sparkSession.emptyDataset[Disorder])(_.union(_)).orderBy(col("partition").asc, col("offset").asc)
    TypedDataset.create(sum)
  }

  /** Notes: partition + offset supposed to be unique, of a topic
    */
  def dupRecords: TypedDataset[DuplicateRecord] = {
    val tds = typedDataset
    val res =
      tds.groupBy(tds('partition), tds('offset)).agg(count(tds.asCol)).deserialized.flatMap { case (p, o, c) =>
        if (c > 1) Some(DuplicateRecord(p, o, c)) else None
      }
    res.orderBy(res('partition).asc, res('offset).asc)
  }
}
