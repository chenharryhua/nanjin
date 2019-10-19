package com.github.chenharryhua.nanjin.sparkafka

import java.util

import cats.effect.{ConcurrentEffect, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{KafkaDateTimeRange, KafkaTopic, Keyboard}
import com.github.chenharryhua.nanjin.spark.{StorageRootPath, UploadRate}
import frameless.{TypedDataset, TypedEncoder}
import fs2.{Chunk, Stream}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategy}

import scala.collection.JavaConverters._

private[sparkafka] object SparKafka {

  private def props(maps: Map[String, String]): util.Map[String, Object] =
    (Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName) ++
      remove(ConsumerConfig.CLIENT_ID_CONFIG)(maps)).mapValues[Object](identity).asJava

  private def path[F[_]](root: StorageRootPath, topic: KafkaTopic[F, _, _]): String =
    root.value + topic.topicDef.topicName

  def datasetFromKafka[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    timeRange: KafkaDateTimeRange,
    locationStrategy: LocationStrategy)(
    implicit sparkSession: SparkSession): F[TypedDataset[SparKafkaConsumerRecord[K, V]]] =
    Sync[F].suspend {
      topic.consumer
        .offsetRangeFor(timeRange)
        .map { gtp =>
          KafkaUtils
            .createRDD[Array[Byte], Array[Byte]](
              sparkSession.sparkContext,
              props(topic.kafkaConsumerSettings.props),
              KafkaOffsets.offsetRange(gtp),
              locationStrategy)
            .mapPartitions { crs =>
              val t = topic
              val decoder = (cr: ConsumerRecord[Array[Byte], Array[Byte]]) =>
                SparKafkaConsumerRecord
                  .fromConsumerRecord(cr)
                  .bimap(t.codec.keyCodec.prism.getOption, t.codec.valueCodec.prism.getOption)
                  .flattenKeyValue
              crs.map(decoder)
            }
        }
        .map(TypedDataset.create(_))
    }

  def datasetFromDisk[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    timeRange: KafkaDateTimeRange,
    rootPath: StorageRootPath)(
    implicit sparkSession: SparkSession): F[TypedDataset[SparKafkaConsumerRecord[K, V]]] =
    Sync[F].delay {
      val tds =
        TypedDataset.createUnsafe[SparKafkaConsumerRecord[K, V]](
          sparkSession.read.parquet(path(rootPath, topic)))
      val inBetween = tds.makeUDF[Long, Boolean](timeRange.isInBetween)
      tds.filter(inBetween(tds('timestamp)))
    }

  def saveToDisk[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    timeRange: KafkaDateTimeRange,
    rootPath: StorageRootPath,
    saveMode: SaveMode,
    locationStrategy: LocationStrategy)(implicit sparkSession: SparkSession): F[Unit] =
    datasetFromKafka(topic, timeRange, locationStrategy).map(
      _.write.mode(saveMode).parquet(path(rootPath, topic)))

  def toProducerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
    tds: TypedDataset[SparKafkaConsumerRecord[K, V]],
    cs: ConversionStrategy): TypedDataset[SparKafkaProducerRecord[K, V]] = {
    val sorted = tds.orderBy(tds('timestamp).asc, tds('offset).asc)
    cs match {
      case ConversionStrategy.Intact =>
        sorted.deserialized.map(_.toSparkafkaProducerRecord)
      case ConversionStrategy.RemovePartition =>
        sorted.deserialized.map(_.toSparkafkaProducerRecord.withoutPartition)
      case ConversionStrategy.RemoveTimestamp =>
        sorted.deserialized.map(_.toSparkafkaProducerRecord.withoutTimestamp)
      case ConversionStrategy.RemovePartitionAndTimestamp =>
        sorted.deserialized.map(_.toSparkafkaProducerRecord.withoutTimestamp.withoutPartition)
    }
  }

  // upload to kafka
  def uploadToKafka[F[_]: ConcurrentEffect: Timer, K, V](
    topic: => KafkaTopic[F, K, V],
    tds: TypedDataset[SparKafkaProducerRecord[K, V]],
    uploadRate: UploadRate
  ): Stream[F, Chunk[RecordMetadata]] =
    for {
      kb <- Keyboard.signal[F]
      ck <- Stream
        .fromIterator[F](tds.dataset.toLocalIterator().asScala)
        .chunkN(uploadRate.batchSize)
        .zipLeft(Stream.fixedRate(uploadRate.duration))
        .evalMap(r => topic.producer.send(r.mapFilter(Option(_).map(_.toProducerRecord))))
        .pauseWhen(kb.map(_.contains(Keyboard.pauSe)))
        .interruptWhen(kb.map(_.contains(Keyboard.Quit)))
    } yield ck

  // load data from disk and then upload into kafka
  def replay[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    timeRange: KafkaDateTimeRange,
    rootPath: StorageRootPath,
    conversionStrategy: ConversionStrategy,
    uploadRate: UploadRate)(implicit sparkSession: SparkSession): Stream[F, Chunk[RecordMetadata]] =
    for {
      ds <- Stream.eval(datasetFromDisk[F, K, V](topic, timeRange, rootPath))
      res <- uploadToKafka(topic, toProducerRecords(ds, conversionStrategy), uploadRate)
    } yield res
}
