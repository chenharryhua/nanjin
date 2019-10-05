package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDateTime
import java.util

import cats.Monad
import cats.effect.{ConcurrentEffect, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.codec._
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, Keyboard}
import frameless.{TypedDataset, TypedEncoder}
import fs2.{Chunk, Stream}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object Sparkafka {

  private def props(maps: Map[String, String]): util.Map[String, Object] =
    (Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName) ++
      remove(ConsumerConfig.CLIENT_ID_CONFIG)(maps)).mapValues[Object](identity).asJava

  private def datasetFromBrokers[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: Option[LocalDateTime],
    end: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    start
      .fold(topic.consumer.offsetRangeFor(end))(topic.consumer.offsetRangeFor(_, end))
      .map { gtp =>
        KafkaUtils
          .createRDD[Array[Byte], Array[Byte]](
            spark.sparkContext,
            props(topic.kafkaConsumerSettings.props),
            KafkaOffsets.offsetRange(gtp),
            LocationStrategies.PreferConsistent)
          .mapPartitions { crs =>
            val t = topic
            val decoder = (cr: ConsumerRecord[Array[Byte], Array[Byte]]) =>
              SparkafkaConsumerRecord
                .fromConsumerRecord(cr)
                .bimap(t.keyCodec.prism.getOption, t.valueCodec.prism.getOption)
                .flattenKeyValue
            crs.map(decoder)
          }
      }
      .map(TypedDataset.create(_))

  def datasetFromBrokers[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    datasetFromBrokers(topic, Some(start), end)

  def datasetFromBrokers[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    end: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    datasetFromBrokers(topic, None, end)

  def datasetFromBrokers[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    datasetFromBrokers(topic, None, LocalDateTime.now)

  def datasetFromDisk[F[_], K: TypedEncoder, V: TypedEncoder](topic: KafkaTopic[F, K, V])(
    implicit spark: SparkSession): TypedDataset[SparkafkaConsumerRecord[K, V]] =
    TypedDataset.createUnsafe[SparkafkaConsumerRecord[K, V]](
      spark.read.parquet(parquetPath(topic.topicName)))

  private def parquetPath(topicName: String): String = s"./data/kafka/parquet/$topicName"

  def saveToDisk[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[Unit] =
    datasetFromBrokers(topic).map(_.write.parquet(parquetPath(topic.topicName)))

  // upload to kafka
  def uploadToBrokers[F[_]: ConcurrentEffect: Timer, K, V](
    data: TypedDataset[SparkafkaProducerRecord[K, V]],
    topic: KafkaTopic[F, K, V],
    batchSize: Int): Stream[F, Chunk[RecordMetadata]] =
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

  def uploadSavedToBrokersIntactly[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: KafkaTopic[F, K, V],
    batchSize: Int = 1000)(implicit spark: SparkSession): Stream[F, Chunk[RecordMetadata]] = {
    val ds = datasetFromDisk[F, K, V](topic)
    uploadToBrokers(
      ds.orderBy(ds('timestamp).asc, ds('offset).asc).deserialized.map(_.toSparkafkaProducerRecord),
      topic,
      batchSize)
  }

  def uploadSavedKeyValueToBrokers[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: KafkaTopic[F, K, V],
    batchSize: Int = 1000)(implicit spark: SparkSession): Stream[F, Chunk[RecordMetadata]] = {
    val ds = datasetFromDisk[F, K, V](topic)
    uploadToBrokers(
      ds.orderBy(ds('timestamp).asc, ds('offset).asc)
        .deserialized
        .map(_.toSparkafkaProducerRecord.withoutPartition.withoutTimestamp),
      topic,
      batchSize)
  }

}
