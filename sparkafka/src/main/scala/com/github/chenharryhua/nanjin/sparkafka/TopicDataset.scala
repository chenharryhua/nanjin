package com.github.chenharryhua.nanjin.sparkafka

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util

import cats.effect.{ConcurrentEffect, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.{SparkafkaConsumerRecord, SparkafkaProducerRecord}
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, Keyboard}
import frameless.{TypedDataset, TypedEncoder}
import fs2.{Chunk, Stream}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.apache.kafka.clients.consumer.ConsumerRecord

final case class TopicDataset[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder] private (
  topic: () => KafkaTopic[F, K, V],
  startDate: Option[LocalDateTime],
  endDate: LocalDateTime,
  parquetPath: String) {

  private def props(maps: Map[String, String]): util.Map[String, Object] =
    (Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName) ++
      remove(ConsumerConfig.CLIENT_ID_CONFIG)(maps)).mapValues[Object](identity).asJava

  def withParquetPath(path: String): TopicDataset[F, K, V] = copy(parquetPath = path)

  def withStartDate(date: LocalDateTime): TopicDataset[F, K, V] = copy(startDate = Some(date))
  def withEndDate(date: LocalDateTime): TopicDataset[F, K, V]   = copy(endDate   = date)

  def withStartDate(date: LocalDate): TopicDataset[F, K, V] =
    withStartDate(LocalDateTime.of(date, LocalTime.MIDNIGHT))

  def withEndDate(date: LocalDate): TopicDataset[F, K, V] =
    withEndDate(LocalDateTime.of(date, LocalTime.MIDNIGHT))

  def fromKafka(implicit spark: SparkSession) = {
    val tpk = topic()
    startDate
      .fold(tpk.consumer.offsetRangeFor(endDate))(tpk.consumer.offsetRangeFor(_, endDate))
      .map { gtp =>
        KafkaUtils
          .createRDD[Array[Byte], Array[Byte]](
            spark.sparkContext,
            props(tpk.kafkaConsumerSettings.props),
            KafkaOffsets.offsetRange(gtp),
            LocationStrategies.PreferConsistent)
          .mapPartitions { crs =>
            val d = topic().sparkDecoder
            crs.map(m => d(SparkafkaConsumerRecord.fromConsumerRecord(m)))
          }
      }
      .map(TypedDataset.create(_))
  }
}

object TopicDataset {

  def apply[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): TopicDataset[F, K, V] =
    new TopicDataset(() => topic, None, LocalDateTime.now, "./data/kafka/parquet/")
}
