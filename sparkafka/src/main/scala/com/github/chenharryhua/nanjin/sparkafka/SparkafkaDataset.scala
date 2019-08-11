package com.github.chenharryhua.nanjin.sparkafka
import java.time.LocalDateTime

import cats.Monad
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._

final class SparkafkaDataset(spark: SparkSession) {

  def dateset[F[_]: Monad, K: Encoder, V: Encoder](
    topic: KafkaTopic[F, K, V],
    start: LocalDateTime,
    end: LocalDateTime,
    key: Array[Byte]   => K,
    value: Array[Byte] => V): F[Dataset[(K, V)]] = {
    val props = Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName
    ) ++ topic.fs2Settings.consumerProps ++ topic.schemaRegistrySettings.props

    topic.consumer.offsetRangeFor(start, end).map { gtp =>
      val range = gtp.value.toArray.map {
        case (tp, r) => OffsetRange.create(tp, r.fromOffset, r.untilOffset)
      }
      implicit val encoder: Encoder[(K, V)] =
        Encoders.tuple(implicitly[Encoder[K]], implicitly[Encoder[V]])
      val rdd = KafkaUtils
        .createRDD[Array[Byte], Array[Byte]](
          spark.sparkContext,
          props.mapValues[Object](identity).asJava,
          range,
          LocationStrategies.PreferConsistent)
        .map(x => (key(x.key), value(x.value())))
      spark.createDataset(rdd)
    }
  }
}
