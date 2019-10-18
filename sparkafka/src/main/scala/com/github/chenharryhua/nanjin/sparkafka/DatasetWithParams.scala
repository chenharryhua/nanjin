package com.github.chenharryhua.nanjin.sparkafka

import java.time.{LocalDate, LocalDateTime, LocalTime}

import cats.effect.{ConcurrentEffect, Timer}
import com.github.chenharryhua.nanjin.kafka.{KafkaTimestamp, KafkaTopic}
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final case class MinutelyAggResult(minute: Int, count: Long)
final case class HourlyAggResult(hour: Int, count: Long)
final case class DailyAggResult(date: LocalDate, count: Long)
final case class DailyHourAggResult(date: LocalDateTime, count: Long)

final case class ConsumerRecordDatasetWithParams[K: TypedEncoder, V: TypedEncoder](
  params: SparKafkaParams,
  private val crs: Dataset[SparKafkaConsumerRecord[K, V]]) {

  def consumerRecords: TypedDataset[SparKafkaConsumerRecord[K, V]] = TypedDataset.create(crs)

  def minutely: TypedDataset[MinutelyAggResult] = {
    val minute: TypedDataset[Int] = consumerRecords.deserialized.map { m =>
      KafkaTimestamp(m.timestamp).atZone(params.zoneId).getMinute
    }
    val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[MinutelyAggResult]
    res.orderBy(res('minute).asc)
  }

  def hourly: TypedDataset[HourlyAggResult] = {
    val hour = consumerRecords.deserialized.map { m =>
      KafkaTimestamp(m.timestamp).atZone(params.zoneId).getHour
    }
    val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[HourlyAggResult]
    res.orderBy(res('hour).asc)
  }

  def daily: TypedDataset[DailyAggResult] = {
    val day: TypedDataset[LocalDate] = consumerRecords.deserialized.map { m =>
      KafkaTimestamp(m.timestamp).atZone(params.zoneId).toLocalDate
    }
    val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
    res.orderBy(res('date).asc)
  }

  def dailyHour: TypedDataset[DailyHourAggResult] = {
    val dayHour: TypedDataset[LocalDateTime] = consumerRecords.deserialized.map { m =>
      val dt   = KafkaTimestamp(m.timestamp).atZone(params.zoneId).toLocalDateTime
      val hour = dt.getHour
      LocalDateTime.of(dt.toLocalDate, LocalTime.of(hour, 0))
    }
    val res = dayHour.groupBy(dayHour.asCol).agg(count(dayHour.asCol)).as[DailyHourAggResult]
    res.orderBy(res('date).asc)
  }

  def nullValues: TypedDataset[SparKafkaConsumerRecord[K, V]] =
    consumerRecords.filter(consumerRecords('value).isNone)

  def nullKeys: TypedDataset[SparKafkaConsumerRecord[K, V]] =
    consumerRecords.filter(consumerRecords('key).isNone)

  def values: TypedDataset[V] =
    consumerRecords.select(consumerRecords('value)).as[Option[V]].deserialized.flatMap(x => x)

  def keys: TypedDataset[K] =
    consumerRecords.select(consumerRecords('key)).as[Option[K]].deserialized.flatMap(x => x)

  def toProducerRecords: ProducerRecordDatasetWithParams[K, V] =
    ProducerRecordDatasetWithParams(
      params,
      SparKafka.toProducerRecords(consumerRecords, params.conversionStrategy).dataset)
}

final case class ProducerRecordDatasetWithParams[K: TypedEncoder, V: TypedEncoder](
  params: SparKafkaParams,
  private val prs: Dataset[SparKafkaProducerRecord[K, V]]) {

  def producerRecords: TypedDataset[SparKafkaProducerRecord[K, V]] = TypedDataset.create(prs)

  def kafkaUpload[F[_]: ConcurrentEffect: Timer](topic: => KafkaTopic[F, K, V]): F[Unit] =
    SparKafka.uploadToKafka[F, K, V](topic, producerRecords, params.uploadRate).compile.drain
}
