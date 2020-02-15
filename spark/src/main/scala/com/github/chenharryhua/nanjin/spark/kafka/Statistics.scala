package com.github.chenharryhua.nanjin.spark.kafka

import java.time.{LocalDate, LocalDateTime, LocalTime}

import cats.effect.Sync
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.datetime.iso._
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.cats.implicits._
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final case class MinutelyAggResult(minute: Int, count: Long)
final case class HourlyAggResult(hour: Int, count: Long)
final case class DailyAggResult(date: LocalDate, count: Long)
final case class DailyHourAggResult(date: LocalDateTime, count: Long)
final case class DailyMinuteAggResult(date: LocalDateTime, count: Long)

final class Statistics[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJConsumerRecord[K, V]],
  cfg: SKConfig)
    extends Serializable {

  private val p: SKParams = SKConfigF.evalParams(cfg)

  @transient private lazy val typedDataset: TypedDataset[NJConsumerRecord[K, V]] =
    TypedDataset.create(ds)

  def minutely(implicit ev: Sync[F]): F[Unit] = {
    val minute: TypedDataset[Int] = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(p.timeRange.zoneId).getMinute
    }
    val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[MinutelyAggResult]
    res.orderBy(res('minute).asc).show[F](p.showDs.rowNum, p.showDs.isTruncate)
  }

  def hourly(implicit ev: Sync[F]): F[Unit] = {
    val hour = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(p.timeRange.zoneId).getHour
    }
    val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[HourlyAggResult]
    res.orderBy(res('hour).asc).show[F](p.showDs.rowNum, p.showDs.isTruncate)
  }

  def daily(implicit ev: Sync[F]): F[Unit] = {
    val day: TypedDataset[LocalDate] = typedDataset.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(p.timeRange.zoneId).toLocalDate
    }
    val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
    res.orderBy(res('date).asc).show[F](p.showDs.rowNum, p.showDs.isTruncate)
  }

  def dailyHour(implicit ev: Sync[F]): F[Unit] = {
    val dayHour: TypedDataset[LocalDateTime] = typedDataset.deserialized.map { m =>
      val dt = NJTimestamp(m.timestamp).atZone(p.timeRange.zoneId).toLocalDateTime
      LocalDateTime.of(dt.toLocalDate, LocalTime.of(dt.getHour, 0))
    }
    val res = dayHour.groupBy(dayHour.asCol).agg(count(dayHour.asCol)).as[DailyHourAggResult]
    res.orderBy(res('date).asc).show[F](p.showDs.rowNum, p.showDs.isTruncate)
  }

  def dailyMinute(implicit ev: Sync[F]): F[Unit] = {
    val dayMinute: TypedDataset[LocalDateTime] = typedDataset.deserialized.map { m =>
      val dt = NJTimestamp(m.timestamp).atZone(p.timeRange.zoneId).toLocalDateTime
      LocalDateTime.of(dt.toLocalDate, LocalTime.of(dt.getHour, dt.getMinute))
    }
    val res =
      dayMinute.groupBy(dayMinute.asCol).agg(count(dayMinute.asCol)).as[DailyMinuteAggResult]
    res.orderBy(res('date).asc).show[F](p.showDs.rowNum, p.showDs.isTruncate)
  }
}
