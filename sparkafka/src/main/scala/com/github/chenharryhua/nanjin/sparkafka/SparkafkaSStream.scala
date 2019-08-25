package com.github.chenharryhua.nanjin.sparkafka
import java.time.LocalDateTime

import cats.Monad
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

object SparkafkaSStream {
  private def sparkKeys(m: Map[String, String]): Map[String, String] = m.map {
    case (k, v) => (s"spark.$k" -> v)
  }

  def dataset[F[_]: Monad, K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): TypedDataset[SparkConsumerRecord[K, V]] = {
    import spark.implicits._
    TypedDataset
      .create(
        spark.readStream
          .format("kafka")
          .options(sparkKeys(topic.kafkaConsumerSettings.props))
          .option("subscribe", topic.topicName)
          .load()
          .as[SparkConsumerRecord[Array[Byte], Array[Byte]]])
      .deserialized
      .map(msg => {
        val t = topic
        msg.bimap(t.keyIso.get, t.valueIso.get)
      })
  }
}
