package com.github.chenharryhua.nanjin.sparkafka
import java.time.LocalDateTime
import java.util

import cats.Monad
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{BitraverseKafkaRecord, KafkaTopic}
import frameless.{SparkDelay, TypedDataset, TypedEncoder}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Success, Try}

object SparkafkaDataset extends BitraverseKafkaRecord {
  private def props(maps: Map[String, String]): util.Map[String, Object] =
    (Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName) ++
      remove(ConsumerConfig.CLIENT_ID_CONFIG)(maps)).mapValues[Object](identity).asJava

  private def rdd[F[_]: Monad, K, V](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime): F[RDD[ConsumerRecord[Array[Byte], Array[Byte]]]] =
    topic.consumer.offsetRangeFor(start, end).map { gtp =>
      val range = gtp.value.toArray.map {
        case (tp, r) => OffsetRange.create(tp, r.fromOffset, r.untilOffset)
      }
      KafkaUtils.createRDD[Array[Byte], Array[Byte]](
        spark.sparkContext,
        props(topic.kafkaConsumerSettings.props),
        range,
        LocationStrategies.PreferConsistent)
    }

  def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime,
    key: Array[Byte]   => K,
    value: Array[Byte] => V): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] = {
    implicit val s: SparkSession = spark
    rdd(spark, topic, start, end).map {
      _.map(msg => SparkafkaConsumerRecord.from(msg.bimap(key, value)))
    }.map(TypedDataset.create(_))
  }

  def safeDataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime,
    key: Array[Byte]   => K,
    value: Array[Byte] => V): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] = {
    implicit val s: SparkSession = spark
    rdd(spark, topic, start, end)
      .map(
        _.flatMap(_.bitraverse(k => Try(key(k)), v => Try(value(v))).toOption
          .map(SparkafkaConsumerRecord.from)))
      .map(TypedDataset.create(_))
  }

  def valueDataset[F[_]: Monad, K, V: TypedEncoder: ClassTag](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime,
    value: Array[Byte] => V): F[TypedDataset[V]] = {
    implicit val s: SparkSession = spark
    rdd(spark, topic, start, end)
      .map(_.map(_.bimap(identity, value).value()))
      .map(TypedDataset.create(_))
  }

  def safeValueDataset[F[_]: Monad, K, V: TypedEncoder: ClassTag](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime,
    value: Array[Byte] => V): F[TypedDataset[V]] = {
    implicit val s: SparkSession = spark
    rdd(spark, topic, start, end)
      .map(_.flatMap(_.bitraverse(Success(_), v => Try(value(v))).toOption.map(_.value())))
      .map(TypedDataset.create(_))
  }

  final private case class KeyPartition[K](key: K, partition: Int)
  final private case class AggregatedKeyPartitions[K](key: K, partitions: Vector[Int])
  import frameless.functions.aggregate.collectSet
  import frameless.functions.size

  def checkSameKeyInSamePartition[F[_]: Monad: SparkDelay, K: TypedEncoder, V: TypedEncoder](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime,
    key: Array[Byte]   => K,
    value: Array[Byte] => V): F[Long] =
    for {
      ds <- dataset[F, K, V](spark, topic, start, end, key, value)
      keyPartition = ds.project[KeyPartition[K]]
      agged = keyPartition
        .groupBy(keyPartition('key))
        .agg(collectSet(keyPartition('partition)))
        .as[AggregatedKeyPartitions[K]]
      filtered = agged.filter(size(agged('partitions)) > 1)
      _ <- filtered.show[F]()
      _ <- ds.count[F]().map(x => println(s"number of source records: $x"))
      count <- filtered.count[F]()
    } yield count
}
