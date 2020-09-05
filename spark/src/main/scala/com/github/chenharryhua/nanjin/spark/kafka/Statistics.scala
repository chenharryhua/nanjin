package com.github.chenharryhua.nanjin.spark.kafka

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.functions.aggregate.count
import frameless.{Injection, SparkDelay, TypedDataset}
import org.apache.spark.sql.Dataset

final private[kafka] case class MinutelyAggResult(minute: Int, count: Long)
final private[kafka] case class HourlyAggResult(hour: Int, count: Long)
final private[kafka] case class DailyAggResult(date: LocalDate, count: Long)
final private[kafka] case class DailyHourAggResult(date: LocalDateTime, count: Long)
final private[kafka] case class DailyMinuteAggResult(date: LocalDateTime, count: Long)

final class Statistics[F[_]](ds: Dataset[CRMetaInfo], cfg: SKConfig) extends Serializable {

  val params: SKParams = cfg.evalConfig

  implicit def localDateTimeInjection: Injection[LocalDateTime, Instant] =
    new Injection[LocalDateTime, Instant] {
      private val zoneId: ZoneId                     = params.timeRange.zoneId
      override def apply(a: LocalDateTime): Instant  = a.atZone(zoneId).toInstant
      override def invert(b: Instant): LocalDateTime = b.atZone(zoneId).toLocalDateTime
    }

  @transient private lazy val typedDataset: TypedDataset[CRMetaInfo] =
    TypedDataset.create(ds)

  def minutely(implicit ev: SparkDelay[F]): F[Unit] = {
    val minute: TypedDataset[Int] = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(params.timeRange.zoneId).getMinute
    }
    val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[MinutelyAggResult]
    res.orderBy(res('minute).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def hourly(implicit ev: SparkDelay[F]): F[Unit] = {
    val hour = typedDataset.deserialized.map(m =>
      NJTimestamp(m.timestamp).atZone(params.timeRange.zoneId).getHour)
    val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[HourlyAggResult]
    res.orderBy(res('hour).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def daily(implicit ev: SparkDelay[F]): F[Unit] = {
    val day: TypedDataset[LocalDate] = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).dayResolution(params.timeRange.zoneId)
    }
    val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
    res.orderBy(res('date).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def dailyHour(implicit ev: SparkDelay[F]): F[Unit] = {
    val dayHour: TypedDataset[LocalDateTime] = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).hourResolution(params.timeRange.zoneId)
    }
    val res = dayHour.groupBy(dayHour.asCol).agg(count(dayHour.asCol)).as[DailyHourAggResult]
    res.orderBy(res('date).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def dailyMinute(implicit ev: SparkDelay[F]): F[Unit] = {
    val dayMinute: TypedDataset[LocalDateTime] = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).minuteResolution(params.timeRange.zoneId)
    }
    val res =
      dayMinute.groupBy(dayMinute.asCol).agg(count(dayMinute.asCol)).as[DailyMinuteAggResult]
    res.orderBy(res('date).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }
}
