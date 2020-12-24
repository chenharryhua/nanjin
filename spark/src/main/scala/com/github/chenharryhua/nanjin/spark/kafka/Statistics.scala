package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.syntax.functor._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.functions.aggregate.count
import frameless.{Injection, TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder}

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

final private[kafka] case class MinutelyAggResult(minute: Int, count: Long)
final private[kafka] case class HourlyAggResult(hour: Int, count: Long)
final private[kafka] case class DailyAggResult(date: LocalDate, count: Long)
final private[kafka] case class DailyHourAggResult(dateTime: ZonedDateTime, count: Long)
final private[kafka] case class DailyMinuteAggResult(dateTime: ZonedDateTime, count: Long)

final private[kafka] case class KafkaDataSummary(
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
    else if (gap < 0) "probably lost data"
    else "oops how is it possible"})
       |first TS:      $startTs(${NJTimestamp(startTs)
      .atZone(zoneId)} not necessarily of the first offset)
       |last TS:       $endTs(${NJTimestamp(endTs)
      .atZone(zoneId)} not necessarily of the last offset)
       |time distance: ${timeDistance.toHours} Hours
       |""".stripMargin
}

final case class MissingOffset(partition: Int, offset: Long)

object MissingOffset {
  implicit val teMissingOffset: TypedEncoder[MissingOffset] = shapeless.cachedImplicit
}

final case class Disorder(
  partition: Int,
  offset: Long,
  timestamp: Long,
  ts: ZonedDateTime,
  nextTS: Long,
  msGap: Long,
  tsType: Int)

final case class DuplicateRecord(partition: Int, offset: Long, num: Long)

final class Statistics[F[_]] private[kafka] (ds: Dataset[CRMetaInfo], cfg: SKConfig)
    extends Serializable {

  val params: SKParams = cfg.evalConfig

  private def update(f: SKConfig => SKConfig): Statistics[F] =
    new Statistics[F](ds, f(cfg))

  def rows(num: Int): Statistics[F] = update(_.withShowRows(num))
  def truncate: Statistics[F]       = update(_.withTruncate)
  def untruncate: Statistics[F]     = update(_.withoutTruncate)

  implicit def zonedDateTimeInjection: Injection[ZonedDateTime, String] =
    new Injection[ZonedDateTime, String] {
      override def apply(a: ZonedDateTime): String  = a.toString
      override def invert(b: String): ZonedDateTime = ZonedDateTime.parse(b)
    }

  def typedDataset: TypedDataset[CRMetaInfo] = TypedDataset.create(ds)

  def minutely(implicit ev: Sync[F]): F[Unit] = {
    val tds = typedDataset
    val minute: TypedDataset[Int] = tds.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(params.timeRange.zoneId).getMinute
    }
    val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[MinutelyAggResult]
    res.orderBy(res('minute).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def hourly(implicit ev: Sync[F]): F[Unit] = {
    val tds = typedDataset
    val hour =
      tds.deserialized.map(m => NJTimestamp(m.timestamp).atZone(params.timeRange.zoneId).getHour)
    val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[HourlyAggResult]
    res.orderBy(res('hour).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def daily(implicit ev: Sync[F]): F[Unit] = {
    val tds = typedDataset
    val day: TypedDataset[LocalDate] = tds.deserialized.map { m =>
      NJTimestamp(m.timestamp).dayResolution(params.timeRange.zoneId)
    }
    val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
    res.orderBy(res('date).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def dailyHour(implicit ev: Sync[F]): F[Unit] = {
    val tds = typedDataset
    val dayHour: TypedDataset[ZonedDateTime] = tds.deserialized.map { m =>
      NJTimestamp(m.timestamp).hourResolution(params.timeRange.zoneId)
    }
    val res = dayHour.groupBy(dayHour.asCol).agg(count(dayHour.asCol)).as[DailyHourAggResult]
    res.orderBy(res('dateTime).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def dailyMinute(implicit ev: Sync[F]): F[Unit] = {
    val tds = typedDataset
    val dayMinute: TypedDataset[ZonedDateTime] = tds.deserialized.map { m =>
      NJTimestamp(m.timestamp).minuteResolution(params.timeRange.zoneId)
    }
    val res =
      dayMinute.groupBy(dayMinute.asCol).agg(count(dayMinute.asCol)).as[DailyMinuteAggResult]
    res.orderBy(res('dateTime).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  private def kafkaSummary: TypedDataset[KafkaDataSummary] = {
    import frameless.functions.aggregate.{max, min}
    val tds = typedDataset.distinct
    val res = tds
      .groupBy(tds('partition))
      .agg(
        min(tds('offset)),
        max(tds('offset)),
        count(tds.asCol),
        min(tds('timestamp)),
        max(tds('timestamp)))
      .as[KafkaDataSummary]
    res.orderBy(res('partition).asc)
  }

  def summary(implicit ev: Sync[F]): F[Unit] =
    kafkaSummary.collect[F]().map(_.foreach(x => println(x.showData(params.timeRange.zoneId))))

  def missingOffsets(implicit ev: Sync[F]): TypedDataset[MissingOffset] = {
    import org.apache.spark.sql.functions.col
    val enc: Encoder[Long] = TypedExpressionEncoder[Long]
    val all: Array[Dataset[MissingOffset]] = kafkaSummary.dataset.collect().map { kds =>
      val expected = ds.sparkSession.range(kds.startOffset, kds.endOffset + 1L).map(_.toLong)(enc)
      val exist =
        ds.filter(col("partition") === kds.partition).map(_.offset)(enc)
      expected
        .except(exist)
        .map(os => MissingOffset(partition = kds.partition, offset = os))(
          TypedExpressionEncoder[MissingOffset])
    }
    TypedDataset.create(all.reduce(_.union(_)).orderBy(col("partition").asc, col("offset").asc))
  }

  def disorders: TypedDataset[Disorder] = {

    val all: Array[RDD[Disorder]] =
      ds.map(_.partition)(TypedExpressionEncoder[Int]).distinct().collect().map { partition =>
        val curr: RDD[(Long, CRMetaInfo)] =
          ds.rdd.filter(_.partition == partition).map(x => (x.offset, x))
        val pre: RDD[(Long, CRMetaInfo)] = curr.map { case (index, crm) => (index + 1, crm) }

        curr.join(pre).flatMap { case (_, (c, p)) =>
          if (c.timestamp >= p.timestamp) None
          else
            Some(Disorder(
              partition = partition,
              offset = p.offset,
              timestamp = p.timestamp,
              ts = NJTimestamp(p.timestamp).atZone(params.timeRange.zoneId),
              nextTS = c.timestamp,
              msGap = p.timestamp - c.timestamp,
              tsType = p.timestampType
            ))
        }
      }
    val tds = TypedDataset.create(all.reduce(_.union(_)))(TypedEncoder[Disorder], ds.sparkSession)
    tds.orderBy(tds('partition).asc, tds('offset).asc)
  }

  def dupRecords: TypedDataset[DuplicateRecord] = {
    val tds = typedDataset
    tds
      .groupBy(tds('partition), tds('offset))
      .agg(count())
      .deserialized
      .flatMap(x =>
        if (x._3 > 1) Some(DuplicateRecord(partition = x._1, offset = x._2, num = x._3)) else None)
  }
}
