package com.github.chenharryhua.nanjin.sparkafka
import java.time.LocalDateTime

import cats.implicits._
import cats.{Monad, Show}
import com.github.chenharryhua.nanjin.kafka.{utils, KafkaRecordBitraverse, KafkaTopic}
import frameless.{Injection, TypedDataset, TypedEncoder}
import monocle.macros.Lenses
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._
import scala.util.Try

@Lenses final case class SparkafkaConsumerRecord[K, V](
  topic: String,
  partition: Int,
  offset: Long,
  key: K,
  value: V,
  timestamp: Long,
  timestampType: TimestampType)

private[sparkafka] trait LowestPriorityShow {

  def build[K, V](t: SparkafkaConsumerRecord[K, V], key: String, value: String): String = {
    val (utc, local) = utils.kafkaTimestamp(t.timestamp)
    s"""
       |topic:     ${t.topic}
       |partition: ${t.partition}
       |offset:    ${t.offset}
       |key:       $key
       |value:     $value
       |timestamp: ${t.timestamp}
       |utc:       $utc
       |local:     $local
       |ts-type:   ${t.timestampType}
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
  implicit def showSparkafkaConsumerRecord0[K: Show, V: Show]: Show[SparkafkaConsumerRecord[K, V]] =
    (t: SparkafkaConsumerRecord[K, V]) => build(t, t.key.show, t.value.show)
}

object SparkafkaDataset extends KafkaRecordBitraverse {
  implicit private val timestampTypeInjection: Injection[TimestampType, String] =
    new Injection[TimestampType, String] {
      override def apply(a: TimestampType): String = a.name

      override def invert(b: String): TimestampType =
        Try(TimestampType.forName(b)).getOrElse(TimestampType.NO_TIMESTAMP_TYPE)
    }

  def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime,
    key: Array[Byte]   => K,
    value: Array[Byte] => V): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] = {
    val props = Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName
    ) ++ topic.fs2Settings.consumerProps

    topic.consumer.offsetRangeFor(start, end).map { gtp =>
      val range = gtp.value.toArray.map {
        case (tp, r) => OffsetRange.create(tp, r.fromOffset, r.untilOffset)
      }
      implicit val s: SparkSession = spark
      val rdd = KafkaUtils
        .createRDD[Array[Byte], Array[Byte]](
          spark.sparkContext,
          props.mapValues[Object](identity).asJava,
          range,
          LocationStrategies.PreferConsistent)
        .map { msg =>
          val d = msg.bimap(key, value)
          SparkafkaConsumerRecord(
            d.topic(),
            d.partition(),
            d.offset(),
            d.key(),
            d.value(),
            d.timestamp(),
            d.timestampType())
        }
      TypedDataset.create(rdd)
    }
  }
}
