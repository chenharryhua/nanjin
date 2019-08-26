package com.github.chenharryhua.nanjin.sparkafka
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import frameless.{TypedDataset, TypedEncoder}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import fs2.Stream

object SparkafkaStream {

  private def toSparkOptions(m: Map[String, String]): Map[String, String] = {
    val rm1 = remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)(_: Map[String, String])
    val rm2 = remove(ConsumerConfig.GROUP_ID_CONFIG)(_: Map[String, String])
    rm1.andThen(rm2)(m).map { case (k, v) => s"kafka.$k" -> v }
  }

  final def sstream[F[_], K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): TypedDataset[SparkConsumerRecord[K, V]] = {
    import spark.implicits._
    TypedDataset
      .create(
        spark.readStream
          .format("kafka")
          .options(toSparkOptions(topic.kafkaConsumerSettings.props))
          .option("subscribe", topic.topicName)
          .load()
          .as[SparkConsumerRecord[Array[Byte], Array[Byte]]])
      .deserialized
      .map { msg =>
        val t = topic
        msg.bimap(t.keyIso.get, t.valueIso.get)
      }
  }
}
