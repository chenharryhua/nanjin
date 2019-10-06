package com.github.chenharryhua.nanjin.sparkafka

import java.util

import cats.effect.{ConcurrentEffect, Sync, Timer}
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

  def datasetFromKafka[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    Sync[F].suspend {
      topic.consumer
        .offsetRangeFor(topic.dateRange)
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
    }

  def datasetFromDisk[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](topic: KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    Sync[F].delay(
      TypedDataset.createUnsafe[SparkafkaConsumerRecord[K, V]](
        spark.read.parquet(parquetPath(topic.topicName))))

  private def parquetPath(topicName: String): String = s"./data/kafka/parquet/$topicName"

  def saveToDisk[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[Unit] =
    datasetFromKafka(topic).map(_.write.parquet(parquetPath(topic.topicName)))

  // upload to kafka
  def uploadToKafka[F[_]: ConcurrentEffect: Timer, K, V](
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

  // load data from disk and then upload into kafka
  def replay[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: KafkaTopic[F, K, V],
    batchSize: Int    = 1000,
    isIntact: Boolean = true)(implicit spark: SparkSession): Stream[F, Chunk[RecordMetadata]] =
    for {
      ds <- Stream.eval(datasetFromDisk[F, K, V](topic))
      sorted = ds.orderBy(ds('timestamp).asc, ds('offset).asc).deserialized.map { scr =>
        if (isIntact) scr.toSparkafkaProducerRecord
        else scr.toSparkafkaProducerRecord.withoutPartition.withoutTimestamp
      }
      res <- uploadToKafka(sorted, topic, batchSize)
    } yield res
}
