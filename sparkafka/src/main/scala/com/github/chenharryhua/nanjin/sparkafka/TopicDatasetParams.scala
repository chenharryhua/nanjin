package com.github.chenharryhua.nanjin.sparkafka

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util

import cats.effect.{ConcurrentEffect, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.codec._
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

final case class TopicDatasetParams(
  startDate: Option[LocalDateTime],
  endDate: LocalDateTime,
  parquetPath: String) {

  def props(maps: Map[String, String]): util.Map[String, Object] =
    (Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName) ++
      remove(ConsumerConfig.CLIENT_ID_CONFIG)(maps)).mapValues[Object](identity).asJava

  def withParquetPath(path: String): TopicDatasetParams = copy(parquetPath = path)

  def withStartDate(date: LocalDateTime): TopicDatasetParams = copy(startDate = Some(date))
  def withEndDate(date: LocalDateTime): TopicDatasetParams   = copy(endDate   = date)

  def withStartDate(date: LocalDate): TopicDatasetParams =
    withStartDate(LocalDateTime.of(date, LocalTime.MIDNIGHT))

  def withEndDate(date: LocalDate): TopicDatasetParams =
    withEndDate(LocalDateTime.of(date, LocalTime.MIDNIGHT))
}
