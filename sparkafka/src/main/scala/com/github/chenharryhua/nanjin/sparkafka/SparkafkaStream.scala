package com.github.chenharryhua.nanjin.sparkafka
import cats.effect.Bracket
import cats.implicits._
import com.github.chenharryhua.nanjin.codec._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import frameless.{TypedDataset, TypedEncoder}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamWriter

object SparkafkaStream {

  private def toSparkOptions(m: Map[String, String]): Map[String, String] = {
    val rm1 = remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)(_: Map[String, String])
    val rm2 = remove(ConsumerConfig.GROUP_ID_CONFIG)(_: Map[String, String])
    rm1.andThen(rm2)(m).map { case (k, v) => s"kafka.$k" -> v }
  }

  def sstream[F[_], K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
    implicit spark: SparkSession): TypedDataset[SparkafkaConsumerRecord[K, V]] = {
    import spark.implicits._
    TypedDataset
      .create(
        spark.readStream
          .format("kafka")
          .options(toSparkOptions(topic.kafkaConsumerSettings.props))
          .option("subscribe", topic.topicDef.topicName)
          .load()
          .as[SparkafkaConsumerRecord[Array[Byte], Array[Byte]]])
      .deserialized
      .mapPartitions { msgs =>
        val t = topic
        val decoder = (msg: SparkafkaConsumerRecord[Array[Byte], Array[Byte]]) =>
          msg.bimap(t.keyCodec.prism.getOption, t.valueCodec.prism.getOption).flattenKeyValue
        msgs.map(decoder)
      }
  }

  def start[F[_], A](dsw: DataStreamWriter[A])(implicit bkt: Bracket[F, Throwable]): F[Unit] =
    bkt.bracket(bkt.pure(dsw.start))(s => bkt.pure(s.awaitTermination()))(s => bkt.pure(s.stop()))

}
