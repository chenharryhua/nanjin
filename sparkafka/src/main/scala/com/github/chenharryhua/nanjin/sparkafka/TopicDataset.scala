package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDateTime
import java.util

import cats.Monad
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import frameless.TypedDataset
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}

import scala.collection.JavaConverters._

final case class TopicDataset[F[_]: Monad, K, V] private (
  topic: KafkaTopic[F, K, V],
  startDate: Option[LocalDateTime],
  endDate: LocalDateTime) {

  private def props(maps: Map[String, String]): util.Map[String, Object] =
    (Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName) ++
      remove(ConsumerConfig.CLIENT_ID_CONFIG)(maps)).mapValues[Object](identity).asJava

  def withStartDate(date: LocalDateTime): TopicDataset[F, K, V] = copy(startDate = Some(date))
  def withEndDate(date: LocalDateTime): TopicDataset[F, K, V]   = copy(endDate   = date)

  def dateset(implicit spark: SparkSession)
    : F[TypedDataset[SparkafkaConsumerRecord[Array[Byte], Array[Byte]]]] =
    startDate
      .fold(topic.consumer.offsetRangeFor(endDate))(topic.consumer.offsetRangeFor(_, endDate))
      .map { gtp =>
        KafkaUtils
          .createRDD[Array[Byte], Array[Byte]](
            spark.sparkContext,
            props(topic.kafkaConsumerSettings.props),
            KafkaOffsets.offsetRange(gtp),
            LocationStrategies.PreferConsistent)
          .map(SparkafkaConsumerRecord.from[Array[Byte], Array[Byte]])
      }
      .map(TypedDataset.create(_))
}

object TopicDataset {

  def apply[F[_]: Monad, K, V](topic: KafkaTopic[F, K, V]): TopicDataset[F, K, V] =
    new TopicDataset(topic, None, LocalDateTime.now)
}
