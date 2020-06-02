package com.github.chenharryhua.nanjin.spark.kafka

import java.time.{LocalDate, LocalDateTime, LocalTime}

import cats.effect.Sync
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.datetime.iso._
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.functions.aggregate.count
import org.apache.spark.sql.Dataset

final private[kafka] case class CRMetaInfo(
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long)

private[kafka] object CRMetaInfo {

  def apply[K, V](cr: NJConsumerRecord[K, V]): CRMetaInfo =
    CRMetaInfo(
      cr.topic,
      cr.partition,
      cr.offset,
      cr.timestamp
    )
}

final private[kafka] case class MinutelyAggResult(minute: Int, count: Long)
final private[kafka] case class HourlyAggResult(hour: Int, count: Long)
final private[kafka] case class DailyAggResult(date: LocalDate, count: Long)
final private[kafka] case class DailyHourAggResult(date: LocalDateTime, count: Long)
final private[kafka] case class DailyMinuteAggResult(date: LocalDateTime, count: Long)

final class Statistics[F[_]](ds: Dataset[CRMetaInfo], cfg: SKConfig) extends Serializable {

  val params: SKParams = SKConfigF.evalConfig(cfg)

  @transient private lazy val typedDataset: TypedDataset[CRMetaInfo] =
    TypedDataset.create(ds)

  def dupIdentities(implicit ev: Sync[F]): F[Unit] =
    typedDataset
      .groupBy(typedDataset.asCol)
      .agg(count())
      .deserialized
      .filter(_._2 > 1)
      .show[F](params.showDs.rowNum, params.showDs.isTruncate)

  def minutely(implicit ev: Sync[F]): F[Unit] = {
    val minute: TypedDataset[Int] = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(params.timeRange.zoneId).getMinute
    }
    val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[MinutelyAggResult]
    res.orderBy(res('minute).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def hourly(implicit ev: Sync[F]): F[Unit] = {
    val hour = typedDataset.deserialized.map(m =>
      NJTimestamp(m.timestamp).atZone(params.timeRange.zoneId).getHour)
    val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[HourlyAggResult]
    res.orderBy(res('hour).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def daily(implicit ev: Sync[F]): F[Unit] = {
    val day: TypedDataset[LocalDate] = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(params.timeRange.zoneId).toLocalDate
    }
    val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
    res.orderBy(res('date).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def dailyHour(implicit ev: Sync[F]): F[Unit] = {
    val dayHour: TypedDataset[LocalDateTime] = typedDataset.deserialized.map { m =>
      val dt = NJTimestamp(m.timestamp).atZone(params.timeRange.zoneId).toLocalDateTime
      LocalDateTime.of(dt.toLocalDate, LocalTime.of(dt.getHour, 0))
    }
    val res = dayHour.groupBy(dayHour.asCol).agg(count(dayHour.asCol)).as[DailyHourAggResult]
    res.orderBy(res('date).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def dailyMinute(implicit ev: Sync[F]): F[Unit] = {
    val dayMinute: TypedDataset[LocalDateTime] = typedDataset.deserialized.map { m =>
      val dt = NJTimestamp(m.timestamp).atZone(params.timeRange.zoneId).toLocalDateTime
      LocalDateTime.of(dt.toLocalDate, LocalTime.of(dt.getHour, dt.getMinute))
    }
    val res =
      dayMinute.groupBy(dayMinute.asCol).agg(count(dayMinute.asCol)).as[DailyMinuteAggResult]
    res.orderBy(res('date).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }
}
