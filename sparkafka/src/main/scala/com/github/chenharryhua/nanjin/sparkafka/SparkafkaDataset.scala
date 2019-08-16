package com.github.chenharryhua.nanjin.sparkafka
import java.time.LocalDateTime
import java.util

import cats.implicits._
import cats.{Monad, Show}
import com.github.chenharryhua.nanjin.kafka.{utils, BitraverseKafkaRecord, KafkaTopic}
import frameless.{TypedDataset, TypedEncoder}
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Success, Try}

@Lenses final case class SparkafkaConsumerRecord[K, V](
  topic: String,
  partition: Int,
  offset: Long,
  key: K,
  value: V,
  timestamp: Long,
  timestampType: String,
  serializedKeySize: Int,
  serializedValueSize: Int,
  headers: String,
  leaderEpoch: String)

private[sparkafka] trait LowestPriorityShow {

  def build[K, V](t: SparkafkaConsumerRecord[K, V], key: String, value: String): String = {
    val (utc, local) = utils.kafkaTimestamp(t.timestamp)
    s"""
       |topic:               ${t.topic}
       |partition:           ${t.partition}
       |offset:              ${t.offset}
       |key:                 $key
       |value:               $value
       |timestamp:           ${t.timestamp}
       |utc:                 $utc
       |local:               $local
       |time-stamp-type:     ${t.timestampType}
       |serializedKeySize:   ${t.serializedKeySize}
       |serializedValueSize: ${t.serializedValueSize}
       |headers:             ${t.headers}
       |leaderEpoch:         ${t.leaderEpoch}
       |""".stripMargin
  }

  implicit def showSparkafkaConsumerRecord2[K, V]: Show[SparkafkaConsumerRecord[K, V]] =
    (t: SparkafkaConsumerRecord[K, V]) => build(t, t.key.toString, t.value.toString)
}

private[sparkafka] trait LowPriorityShow extends LowestPriorityShow {
  implicit def showSparkafkaConsumerRecord1[K, V: Show]: Show[SparkafkaConsumerRecord[K, V]] =
    (t: SparkafkaConsumerRecord[K, V]) => build(t, t.key.toString, t.value.show)
}

object SparkafkaConsumerRecord extends LowPriorityShow {

  def from[K, V](d: ConsumerRecord[K, V]): SparkafkaConsumerRecord[K, V] =
    SparkafkaConsumerRecord(
      d.topic(),
      d.partition(),
      d.offset(),
      d.key(),
      d.value(),
      d.timestamp(),
      d.timestampType().name,
      d.serializedKeySize(),
      d.serializedValueSize(),
      d.headers().toString,
      d.leaderEpoch().toString
    )
  implicit def showSparkafkaConsumerRecord0[K: Show, V: Show]: Show[SparkafkaConsumerRecord[K, V]] =
    (t: SparkafkaConsumerRecord[K, V]) => build(t, t.key.show, t.value.show)
}

object SparkafkaDataset extends BitraverseKafkaRecord {
  private def props(maps: Map[String, String]): util.Map[String, Object] =
    (Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.CLIENT_ID_CONFIG -> s"spark-${utils.random4d.value}"
    ) ++ maps).mapValues[Object](identity).asJava

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
}
