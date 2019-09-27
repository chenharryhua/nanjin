package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDateTime
import java.util

import cats.Monad
import cats.effect.ConcurrentEffect
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{KafkaProducerApi, KafkaTopic, Keyboard}
import frameless.{TypedDataset, TypedEncoder}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}

import scala.collection.JavaConverters._

object SparkafkaDataset {

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
          .map(SparkafkaConsumerRecord.from[Array[Byte], Array[Byte]])
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
        msgs.map(m => m.bimap(t.keyCodec.decode, t.valueCodec.decode))
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

  private def safeDataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: Option[LocalDateTime],
    end: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    rawDS(topic, start, end).map(_.deserialized.mapPartitions(msgs => {
      val t = topic
      msgs.flatMap(
        _.bitraverse[Option, K, V](
          k => t.keyCodec.prism.getOption(k),
          v => t.valueCodec.prism.getOption(v)))
    }))

  def safeDataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    safeDataset(topic, Some(start), end)

  def safeDataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    end: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    safeDataset(topic, None, end)

  def safeDataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    safeDataset(topic, None, LocalDateTime.now)

  private def valueDataset[F[_]: Monad, K, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: Option[LocalDateTime],
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    rawDS(topic, start, end).map { ds =>
      val udf = ds.makeUDF((x: Array[Byte]) => topic.valueCodec.decode(x))
      ds.select(udf(ds('value)))
    }

  def valueDataset[F[_]: Monad, K, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    valueDataset(topic, Some(start), end)

  def valueDataset[F[_]: Monad, K, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    valueDataset(topic, None, end)

  def valueDataset[F[_]: Monad, K, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[V]] =
    valueDataset(topic, None, LocalDateTime.now)

  private def safeValueDataset[F[_]: Monad, K, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: Option[LocalDateTime],
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    rawDS(topic, start, end).map { ds =>
      val udf =
        ds.makeUDF((x: Array[Byte]) => topic.valueCodec.tryDecode(x).toOption)
      ds.select(udf(ds('value))).deserialized.flatMap(x => x)
    }

  def safeValueDataset[F[_]: Monad, K, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    safeValueDataset(topic, Some(start), end)

  def safeValueDataset[F[_]: Monad, K, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    safeValueDataset(topic, None, end)

  def safeValueDataset[F[_]: Monad, K, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[V]] =
    safeValueDataset(topic, None, LocalDateTime.now)

// save topic
  private def parquetPath(topicName: String): String = s"./data/kafka/parquet/$topicName"

  def saveTopicToDisk[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[Unit] =
    safeDataset(topic).map(_.write.parquet(parquetPath(topic.topicName)))

// load data
  def loadTopicFromDisk[F[_], K: TypedEncoder, V: TypedEncoder](topic: KafkaTopic[F, K, V])(
    implicit spark: SparkSession): TypedDataset[SparkafkaConsumerRecord[K, V]] =
    TypedDataset.createUnsafe[SparkafkaConsumerRecord[K, V]](
      spark.read.parquet(parquetPath(topic.topicName)))

// into kafka
  def uploadIntoTopic[F[_]: ConcurrentEffect, K, V](
    data: TypedDataset[SparkafkaProducerRecord[K, V]],
    topic: KafkaTopic[F, K, V]): F[Unit] =
    Keyboard.signal.flatMap { signal =>
      fs2.Stream
        .fromIterator[F](data.dataset.toLocalIterator().asScala)
        .chunkN(500)
        .evalMap(r => topic.producer.send(r.mapFilter(Option(_).map(_.toProducerRecord))))
        .pauseWhen(signal.map(_.contains(Keyboard.pauSe)))
        .interruptWhen(signal.map(_.contains(Keyboard.Quit)))
    }.compile.drain

  def uploadIntoTopicFromDisk[F[_]: ConcurrentEffect, K: TypedEncoder, V: TypedEncoder](
    topic: KafkaTopic[F, K, V])(implicit spark: SparkSession): F[Unit] =
    uploadIntoTopic(
      loadTopicFromDisk[F, K, V](topic).deserialized
        .flatMap(Option(_).map(_.toSparkafkaProducerRecord)),
      topic)
}
