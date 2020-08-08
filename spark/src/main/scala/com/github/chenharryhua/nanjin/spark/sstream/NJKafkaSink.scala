package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaProducerSettings
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.kafka.TopicName
import fs2.Stream
import monocle.function.At.remove
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQueryProgress}

final class NJKafkaSink[F[_]](
  dsw: DataStreamWriter[NJProducerRecord[Array[Byte], Array[Byte]]],
  cfg: NJSStreamConfig,
  producer: KafkaProducerSettings,
  topicName: TopicName
) extends NJStreamSink[F] {

  override val params: NJSStreamParams = cfg.evalConfig

  //  https://spark.apache.org/docs/2.4.5/structured-streaming-kafka-integration.html
  private def producerOptions(m: Map[String, String]): Map[String, String] = {
    val rm1 = remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)(_: Map[String, String])
    val rm2 = remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)(_: Map[String, String])
    rm1.andThen(rm2)(m).map { case (k, v) => s"kafka.$k" -> v }
  }

  override def queryStream(implicit
    F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .trigger(params.trigger)
        .format("kafka")
        .outputMode(params.outputMode)
        .options(producerOptions(producer.config))
        .option("topic", topicName.value)
        .option("checkpointLocation", params.checkpoint.value)
        .option("failOnDataLoss", params.dataLoss.value))

}
