package com.github.chenharryhua.nanjin.spark.kafka

import java.time.{LocalDate, ZonedDateTime}

import cats.effect.Sync
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.functions.aggregate.count
import frameless.{Injection, TypedDataset}
import org.apache.spark.sql.Dataset

final private[kafka] case class MinutelyAggResult(minute: Int, count: Long)
final private[kafka] case class HourlyAggResult(hour: Int, count: Long)
final private[kafka] case class DailyAggResult(date: LocalDate, count: Long)
final private[kafka] case class DailyHourAggResult(dateTime: ZonedDateTime, count: Long)
final private[kafka] case class DailyMinuteAggResult(dateTime: ZonedDateTime, count: Long)

final class Statistics[F[_]](ds: Dataset[CRMetaInfo], cfg: SKConfig) extends Serializable {

  val params: SKParams = cfg.evalConfig

  implicit def localDateTimeInjection: Injection[ZonedDateTime, String] =
    new Injection[ZonedDateTime, String] {
      override def apply(a: ZonedDateTime): String  = a.toString
      override def invert(b: String): ZonedDateTime = ZonedDateTime.parse(b)
    }

  @transient private lazy val typedDataset: TypedDataset[CRMetaInfo] =
    TypedDataset.create(ds)

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
      NJTimestamp(m.timestamp).dayResolution(params.timeRange.zoneId)
    }
    val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
    res.orderBy(res('date).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def dailyHour(implicit ev: Sync[F]): F[Unit] = {
    val dayHour: TypedDataset[ZonedDateTime] = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).hourResolution(params.timeRange.zoneId)
    }
    val res = dayHour.groupBy(dayHour.asCol).agg(count(dayHour.asCol)).as[DailyHourAggResult]
    res.orderBy(res('dateTime).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  def dailyMinute(implicit ev: Sync[F]): F[Unit] = {
    val dayMinute: TypedDataset[ZonedDateTime] = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).minuteResolution(params.timeRange.zoneId)
    }
    val res =
      dayMinute.groupBy(dayMinute.asCol).agg(count(dayMinute.asCol)).as[DailyMinuteAggResult]
    res.orderBy(res('dateTime).asc).show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }
}
