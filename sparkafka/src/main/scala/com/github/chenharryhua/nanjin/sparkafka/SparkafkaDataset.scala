package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDateTime
import java.util

import cats.Monad
import cats.effect.{ConcurrentEffect, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{KafkaProducerApi, KafkaTopic}
import frameless.{TypedDataset, TypedEncoder}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}

import scala.collection.JavaConverters._

object SparkafkaDataset {
  private def epoch: LocalDateTime = LocalDateTime.MIN

  private def props(maps: Map[String, String]): util.Map[String, Object] =
    (Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName) ++
      remove(ConsumerConfig.CLIENT_ID_CONFIG)(maps)).mapValues[Object](identity).asJava

  private def rawDS[F[_]: Monad, K, V](
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(implicit spark: SparkSession)
    : F[TypedDataset[SparkafkaConsumerRecord[Array[Byte], Array[Byte]]]] =
    topic.consumer
      .offsetRangeFor(start, end)
      .map { gtp =>
        KafkaUtils
          .createRDD[Array[Byte], Array[Byte]](
            spark.sparkContext,
            props(topic.kafkaConsumerSettings.props),
            SparkOffsets.offsetRange(gtp),
            LocationStrategies.PreferConsistent)
          .map(SparkafkaConsumerRecord.from[Array[Byte], Array[Byte]])
      }
      .map(TypedDataset.create(_))

  def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
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
    start: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    dataset(topic, start, LocalDateTime.now)

  def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    dataset(topic, epoch, LocalDateTime.now)

  def safeDataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    rawDS(topic, start, end).map(_.deserialized.mapPartitions(msgs => {
      val t = topic
      msgs.flatMap(
        _.bitraverse[Option, K, V](
          k => t.keyCodec.tryDecode(k).toOption,
          v => t.valueCodec.tryDecode(v).toOption))
    }))

  def safeDataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime)(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    safeDataset(topic, start, LocalDateTime.now)

  def safeDataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
    safeDataset(topic, epoch, LocalDateTime.now)

  def valueDataset[F[_]: Monad, K, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    rawDS(topic, start, end).map { ds =>
      val udf = ds.makeUDF((x: Array[Byte]) => topic.valueCodec.decode(x))
      ds.select(udf(ds('value)))
    }

  def valueDataset[F[_]: Monad, K, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    valueDataset(topic, start, LocalDateTime.now)

  def valueDataset[F[_]: Monad, K, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[V]] =
    valueDataset(topic, epoch, LocalDateTime.now)

  def safeValueDataset[F[_]: Monad, K, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    rawDS(topic, start, end).map { ds =>
      val udf =
        ds.makeUDF((x: Array[Byte]) => topic.valueCodec.tryDecode(x).toOption)
      ds.select(udf(ds('value))).deserialized.flatMap(x => x)
    }

  def safeValueDataset[F[_]: Monad, K, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    safeValueDataset(topic, start, LocalDateTime.now)

  def safeValueDataset[F[_]: Monad, K, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): F[TypedDataset[V]] =
    safeValueDataset(topic, epoch, LocalDateTime.now)

  def upload[F[_]: ConcurrentEffect, K, V](
    data: TypedDataset[SparkafkaProducerRecord[K, V]],
    producer: KafkaProducerApi[F, K, V]): F[Unit] =
    fs2.Stream
      .fromIterator[F](data.dataset.toLocalIterator().asScala)
      .chunkN(500)
      .evalMap(r => producer.send(r.mapFilter(Option(_).map(_.toProducerRecord))))
      .compile
      .drain

  private def updateDB[A](
    data: TypedDataset[A],
    db: DatabaseSettings,
    dbTable: String,
    saveMode: SaveMode): Unit =
    data.write
      .mode(saveMode)
      .format("jdbc")
      .option("url", db.connStr.value)
      .option("driver", db.driver.value)
      .option("dbtable", dbTable)
      .save()

  def appendDB[A](data: TypedDataset[A], db: DatabaseSettings, dbTable: String): Unit =
    updateDB(data, db, dbTable, SaveMode.Append)

  def overwriteDB[A](data: TypedDataset[A], db: DatabaseSettings, dbTable: String): Unit =
    updateDB(data, db, dbTable, SaveMode.Overwrite)

}
