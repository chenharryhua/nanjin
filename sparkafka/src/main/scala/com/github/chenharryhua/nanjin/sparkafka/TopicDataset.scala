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
  topic: KafkaTopic[F, K, V],
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

  def fromKafka(f: ConsumerRecord[Array[Byte], Array[Byte]] => SparkafkaConsumerRecord[K, V])(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    startDate
      .fold(topic.consumer.offsetRangeFor(endDate))(topic.consumer.offsetRangeFor(_, endDate))
      .map { gtp =>
        KafkaUtils
          .createRDD[Array[Byte], Array[Byte]](
            spark.sparkContext,
            props(topic.kafkaConsumerSettings.props),
            KafkaOffsets.offsetRange(gtp),
            LocationStrategies.PreferConsistent)
          .map(f)
      }
      .map(TypedDataset.create(_))

  private val path: String = parquetPath + topic.topicName

  def fromDisk(implicit spark: SparkSession): TypedDataset[SparkafkaConsumerRecord[K, V]] =
    TypedDataset.createUnsafe[SparkafkaConsumerRecord[K, V]](spark.read.parquet(path))

  def save(f: ConsumerRecord[Array[Byte], Array[Byte]] => SparkafkaConsumerRecord[K, V])(
    implicit spark: SparkSession): F[Unit] =
    fromKafka(f).map(_.write.parquet(path))

  def upload(data: TypedDataset[SparkafkaProducerRecord[K, V]], batchSize: Int)(
    implicit spark: SparkSession): Stream[F, Chunk[RecordMetadata]] =
    for {
      kb <- Keyboard.signal[F]
      ck <- Stream
        .fromIterator[F](data.dataset.toLocalIterator().asScala)
        .chunkN(batchSize)
        .zipLeft(Stream.fixedRate(1.second))
        .evalMap(r => topic.producer.send(r.mapFilter(Option(_).map(_.toProducerRecord))))
        .pauseWhen(kb.map(_.contains(Keyboard.pauSe)))
        .interruptWhen(kb.map(_.contains(Keyboard.Quit)))
    } yield ck

  def uploadFromDisk(batchSize: Int)(implicit spark: SparkSession): Stream[F, Chunk[RecordMetadata]] = {
    val ds = fromDisk(spark)
    upload(
      ds.orderBy(ds('timestamp).asc, ds('offset).asc).deserialized.map(_.toSparkafkaProducerRecord),
      batchSize)
  }
}

object TopicDataset {

  def apply[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): TopicDataset[F, K, V] =
    new TopicDataset(topic, None, LocalDateTime.now, "./data/kafka/parquet/")
}
