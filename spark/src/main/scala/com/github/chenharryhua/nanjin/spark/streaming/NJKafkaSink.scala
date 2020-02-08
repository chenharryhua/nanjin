package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaProducerSettings
import com.github.chenharryhua.nanjin.kafka.common.{NJProducerRecord, TopicName}
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}

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

  override def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    ss.queryStream(
        dataStreamWriter
          .trigger(trigger)
          .format("kafka")
          .outputMode(outputMode)
          .options(producer.config.map { case (k, v) => s"kafka.$k" -> v })
          .option("topic", topicName.value)
          .option("checkpointLocation", checkpoint.value)
          .option("failOnDataLoss", dataLoss.value))
      .compile
      .drain
}
