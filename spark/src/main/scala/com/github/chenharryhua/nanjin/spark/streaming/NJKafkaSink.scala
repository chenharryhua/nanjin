package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaBrokers
import com.github.chenharryhua.nanjin.kafka.common.{NJProducerRecord, TopicName}
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJFailOnDataLoss}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

final class NJKafkaSink[F[_]](
  dsw: DataStreamWriter[NJProducerRecord[Array[Byte], Array[Byte]]],
  outputMode: OutputMode,
  brokers: KafkaBrokers,
  topicName: TopicName,
  checkpoint: NJCheckpoint,
  dataLoss: NJFailOnDataLoss
) extends NJStreamSink[F] {

  def withCheckpoint(cp: String): NJKafkaSink[F] =
    new NJKafkaSink[F](dsw, outputMode, brokers, topicName, NJCheckpoint(cp), dataLoss)

  def withoutFailONDataLoss: NJKafkaSink[F] =
    new NJKafkaSink[F](dsw, outputMode, brokers, topicName, checkpoint, NJFailOnDataLoss(false))

  def withOutputMode(om: OutputMode): NJKafkaSink[F] =
    new NJKafkaSink[F](dsw, om, brokers, topicName, checkpoint, dataLoss)

  def withOptions(
    f: DataStreamWriter[NJProducerRecord[Array[Byte], Array[Byte]]] => DataStreamWriter[
      NJProducerRecord[Array[Byte], Array[Byte]]]): NJKafkaSink[F] =
    new NJKafkaSink[F](f(dsw), outputMode, brokers, topicName, checkpoint, dataLoss)

  override def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    ss.queryStream(
        dsw
          .format("kafka")
          .outputMode(outputMode)
          .option("kafka.bootstrap.servers", brokers.value)
          .option("topic", topicName.value)
          .option("checkpointLocation", checkpoint.value)
          .option("failOnDataLoss", dataLoss.value))
      .compile
      .drain
}
