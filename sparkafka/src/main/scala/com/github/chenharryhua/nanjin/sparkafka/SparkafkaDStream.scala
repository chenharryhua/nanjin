package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDateTime
import java.util

import cats.Monad
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{BitraverseKafkaRecord, KafkaTopic}
import frameless.{Injection, SparkDelay, TypedDataset, TypedEncoder}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object SparkafkaDStream extends BitraverseKafkaRecord {
  implicit private val timestampTypeInjection: Injection[TimestampType, String] =
    new Injection[TimestampType, String] {
      override def apply(a: TimestampType): String  = a.name
      override def invert(b: String): TimestampType = TimestampType.forName(b)
    }

  implicit private val headersInjection: Injection[Headers, String] =
    new Injection[Headers, String] {
      override def apply(a: Headers): String  = a.toString
      override def invert(b: String): Headers = new RecordHeaders()
    }

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

  final private case class KeyPartition[K](key: K, partition: Int)
  final private case class AggregatedKeyPartitions[K](key: K, partitions: Vector[Int])
  import frameless.functions.aggregate.collectSet
  import frameless.functions.size

  def checkSameKeyInSamePartition[F[_]: Monad: SparkDelay, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime)(implicit spark: SparkSession): F[Long] =
    for {
      ds <- dataset[F, K, V](topic, start, end)
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
