package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaProducerSettings
import com.github.chenharryhua.nanjin.kafka.common.{NJProducerRecord, TopicName}
import fs2.Stream
import monocle.function.At.remove
import monocle.macros.Lenses
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.streaming.{
  DataStreamWriter,
  OutputMode,
  StreamingQueryProgress,
  Trigger
}

@Lenses final case class NJKafkaSink[F[_]](
  dataStreamWriter: DataStreamWriter[NJProducerRecord[Array[Byte], Array[Byte]]],
  outputMode: OutputMode,
  producer: KafkaProducerSettings,
  topicName: TopicName,
  checkpoint: NJCheckpoint,
  dataLoss: NJFailOnDataLoss,
  trigger: Trigger
) extends NJStreamSink[F] {

  def withTrigger(tg: Trigger): NJKafkaSink[F] =
    NJKafkaSink.trigger.set(tg)(this)

  def withCheckpoint(cp: String): NJKafkaSink[F] =
    NJKafkaSink.checkpoint.set(NJCheckpoint(cp))(this)

  def withoutFailONDataLoss: NJKafkaSink[F] =
    NJKafkaSink.dataLoss.set(NJFailOnDataLoss(false))(this)

  def withOutputMode(om: OutputMode): NJKafkaSink[F] =
    NJKafkaSink.outputMode.set(om)(this)

  def withOptions(
    f: DataStreamWriter[NJProducerRecord[Array[Byte], Array[Byte]]] => DataStreamWriter[
      NJProducerRecord[Array[Byte], Array[Byte]]]): NJKafkaSink[F] =
    NJKafkaSink.dataStreamWriter.modify(f)(this)

  //  https://spark.apache.org/docs/2.4.5/structured-streaming-kafka-integration.html
  private def producerOptions(m: Map[String, String]): Map[String, String] = {
    val rm1 = remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)(_: Map[String, String])
    val rm2 = remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)(_: Map[String, String])
    rm1.andThen(rm2)(m).map { case (k, v) => s"kafka.$k" -> v }
  }

  override def queryStream(
    implicit F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dataStreamWriter
        .trigger(trigger)
        .format("kafka")
        .outputMode(outputMode)
        .options(producerOptions(producer.config))
        .option("topic", topicName.value)
        .option("checkpointLocation", checkpoint.value)
        .option("failOnDataLoss", dataLoss.value))

}
