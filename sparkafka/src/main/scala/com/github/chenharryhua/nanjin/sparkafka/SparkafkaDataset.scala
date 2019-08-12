package com.github.chenharryhua.nanjin.sparkafka
import java.time.LocalDateTime

import cats.Monad
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import frameless.{TypedDataset, TypedEncoder}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._

object SparkafkaDataset {

  def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](
    spark: SparkSession,
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime,
    key: Array[Byte]   => K,
    value: Array[Byte] => V): F[TypedDataset[(K, V)]] = {
    val props = Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName
    ) ++ topic.fs2Settings.consumerProps ++ topic.schemaRegistrySettings.props

    topic.consumer.offsetRangeFor(start, end).map { gtp =>
      println(gtp.show)
      val range = gtp.value.toArray.map {
        case (tp, r) => OffsetRange.create(tp, r.fromOffset, r.untilOffset)
      }
      implicit val s = spark
      val rdd = KafkaUtils
        .createRDD[Array[Byte], Array[Byte]](
          spark.sparkContext,
          props.mapValues[Object](identity).asJava,
          range,
          LocationStrategies.PreferConsistent)
        .map(msg => (key(msg.key), value(msg.value())))
      TypedDataset.create(rdd)
    }
  }
}
