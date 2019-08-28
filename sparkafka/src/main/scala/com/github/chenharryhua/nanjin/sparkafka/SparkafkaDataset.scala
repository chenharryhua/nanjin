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
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

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
      KafkaUtils.createRDD[Array[Byte], Array[Byte]](
        spark.sparkContext,
        props(topic.kafkaConsumerSettings.props),
        SparkOffsets.offsetRange(gtp),
        LocationStrategies.PreferConsistent)
    }

  def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[SparkConsumerRecord[K, V]]] =
    rdd(spark, topic, start, end).map {
      _.mapPartitions(msgs => {
        val t = topic
        msgs.map(m => SparkConsumerRecord.from(m.bimap(t.keyIso.get, t.valueIso.get)))
      })
    }.map(TypedDataset.create(_))

  def safeDataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[SparkConsumerRecord[K, V]]] =
    rdd(spark, topic, start, end)
      .map(_.mapPartitions(msgs => {
        val t = topic
        msgs
          .flatMap(_.bitraverse[Option, K, V](t.keyPrism.getOption, t.valuePrism.getOption))
          .map(SparkConsumerRecord.from)
      }))
      .map(TypedDataset.create(_))

  def valueDataset[F[_]: Monad, K, V: TypedEncoder: ClassTag](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    rdd(spark, topic, start, end).map {
      _.mapPartitions(msgs => {
        val t = topic
        msgs.map(m => SparkConsumerRecord.from(m.bimap(identity, t.valueIso.get)).value)
      })
    }.map(TypedDataset.create(_))

  def safeValueDataset[F[_]: Monad, K, V: TypedEncoder: ClassTag](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(implicit spark: SparkSession): F[TypedDataset[V]] =
    rdd(spark, topic, start, end)
      .map(_.mapPartitions(msgs => {
        val t = topic
        msgs
          .flatMap(_.bitraverse[Option, Array[Byte], V](Some(_), t.valuePrism.getOption))
          .map(m => SparkConsumerRecord.from(m).value)
      }))
      .map(TypedDataset.create(_))
}
