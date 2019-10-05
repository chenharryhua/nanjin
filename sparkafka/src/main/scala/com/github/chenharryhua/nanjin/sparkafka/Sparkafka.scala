package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDateTime
import java.util

import cats.Monad
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

object Sparkafka {

  private def props(maps: Map[String, String]): util.Map[String, Object] =
    (Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName) ++
      remove(ConsumerConfig.CLIENT_ID_CONFIG)(maps)).mapValues[Object](identity).asJava

  private def rawDS[F[_]: Monad, K, V](
    topic: KafkaTopic[F, K, V],
    start: Option[LocalDateTime],
    end: LocalDateTime)(implicit spark: SparkSession)
    : F[TypedDataset[SparkafkaConsumerRecord[Array[Byte], Array[Byte]]]] =
    start
      .fold(topic.consumer.offsetRangeFor(end))(topic.consumer.offsetRangeFor(_, end))
      .map { gtp =>
        KafkaUtils
          .createRDD[Array[Byte], Array[Byte]](
            spark.sparkContext,
            props(topic.kafkaConsumerSettings.props),
            KafkaOffsets.offsetRange(gtp),
            LocationStrategies.PreferConsistent)
          .map(SparkafkaConsumerRecord.fromConsumerRecord[Array[Byte], Array[Byte]])
      }
      .map(TypedDataset.create(_))

  private def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: Option[LocalDateTime],
    end: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    rawDS(topic, start, end).map {
      _.deserialized.mapPartitions(msgs => {
        val t = topic
        msgs.map(_.bimap(t.keyCodec.prism.getOption, t.valueCodec.prism.getOption).flatten)
      })
    }

  def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    dataset(topic, Some(start), end)

  def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    end: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    dataset(topic, None, end)

  def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    dataset(topic, None, LocalDateTime.now)

  // save topic
  private def parquetPath(topicName: String): String = s"./data/kafka/parquet/$topicName"

  def saveTopicToDisk[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[Unit] =
    dataset(topic).map(_.write.parquet(parquetPath(topic.topicName)))

  // load data
  def loadTopicFromDisk[F[_], K: TypedEncoder, V: TypedEncoder](topic: KafkaTopic[F, K, V])(
    implicit spark: SparkSession): TypedDataset[SparkafkaConsumerRecord[K, V]] =
    TypedDataset.createUnsafe[SparkafkaConsumerRecord[K, V]](
      spark.read.parquet(parquetPath(topic.topicName)))

  // into kafka
  def uploadIntoTopic[F[_]: ConcurrentEffect: Timer, K, V](
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

  def uploadIntoTopicFromDisk[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: KafkaTopic[F, K, V],
    batchSize: Int = 1000)(implicit spark: SparkSession): Stream[F, Chunk[RecordMetadata]] = {
    val ds = loadTopicFromDisk[F, K, V](topic)
    uploadIntoTopic(
      ds.orderBy(ds('timestamp).asc, ds('offset).asc).deserialized.map(_.toSparkafkaProducerRecord),
      topic,
      batchSize)
  }
}
