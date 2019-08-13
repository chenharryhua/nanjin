package com.github.chenharryhua.nanjin.sparkafka
import java.time.LocalDateTime

import cats.implicits._
import cats.{Monad, Show}
import com.github.chenharryhua.nanjin.kafka.{KafkaRecordBitraverse, KafkaTopic}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._

final case class SparkafkaConsumerRecord[K, V](
  topic: String,
  partition: Int,
  offset: Long,
  key: K,
  value: V)

private[sparkafka] trait LowestPriorityShow {
  implicit def showSparkafkaConsumerRecord2[K, V]: Show[SparkafkaConsumerRecord[K, V]] =
    (t: SparkafkaConsumerRecord[K, V]) => s"""
                                             |topic:     ${t.topic}
                                             |partition: ${t.partition}
                                             |offset:    ${t.offset}
                                             |key:       ${t.key.toString}
                                             |value:     ${t.value.toString}
                                             |""".stripMargin
}

private[sparkafka] trait LowPriorityShow extends LowestPriorityShow{
  implicit def showSparkafkaConsumerRecord1[K, V:Show]: Show[SparkafkaConsumerRecord[K, V]] =
    (t: SparkafkaConsumerRecord[K, V]) => s"""
                                             |topic:     ${t.topic}
                                             |partition: ${t.partition}
                                             |offset:    ${t.offset}
                                             |key:       ${t.key.toString}
                                             |value:     ${t.value.show}
                                             |""".stripMargin

}

object SparkafkaConsumerRecord extends LowPriorityShow {
  implicit def showSparkafkaConsumerRecord0[K:Show, V: Show]: Show[SparkafkaConsumerRecord[K, V]] =
    (t: SparkafkaConsumerRecord[K, V]) => s"""
                                             |topic:     ${t.topic}
                                             |partition: ${t.partition}
                                             |offset:    ${t.offset}
                                             |key:       ${t.key.show}
                                             |value:     ${t.value.show}
                                             |""".stripMargin
}

object SparkafkaDataset extends KafkaRecordBitraverse {

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
          SparkafkaConsumerRecord(d.topic(), d.partition(), d.offset(), d.key(), d.value())
        }
      TypedDataset.create(rdd)
    }
  }
}
