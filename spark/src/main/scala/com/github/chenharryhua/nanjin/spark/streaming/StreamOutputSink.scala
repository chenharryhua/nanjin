package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.KafkaBrokers
import com.github.chenharryhua.nanjin.kafka.common.TopicName
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJFailOnDataLoss, NJPath}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

//http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks

sealed trait StreamOutputSink extends Serializable {
  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A]
  protected def failOnDataLoss: NJFailOnDataLoss
}

final case class FileSink(
  fileFormat: NJFileFormat,
  path: NJPath,
  checkpoint: NJCheckpoint,
  failOnDataLoss: NJFailOnDataLoss)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw
      .format(fileFormat.format)
      .outputMode(OutputMode.Append)
      .option("path", path.value)
      .option("checkpointLocation", checkpoint.value)
      .option("failOnDataLoss", failOnDataLoss.value)

}

final case class KafkaSink(
  mode: OutputMode,
  brokers: KafkaBrokers,
  topicName: TopicName,
  checkpoint: NJCheckpoint,
  failOnDataLoss: NJFailOnDataLoss)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw
      .format("kafka")
      .outputMode(mode)
      .option("kafka.bootstrap.servers", brokers.value)
      .option("topic", topicName.value)
      .option("checkpointLocation", checkpoint.value)
      .option("failOnDataLoss", failOnDataLoss.value)

}

final case class ConsoleSink(numRows: Int, trucate: Boolean, failOnDataLoss: NJFailOnDataLoss)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw
      .format("console")
      .outputMode(OutputMode.Append)
      .option("truncate", trucate)
      .option("numRows", numRows.toString)
      .option("failOnDataLoss", failOnDataLoss.value)

}

final case class MemorySink(mode: OutputMode, queryName: String, failOnDataLoss: NJFailOnDataLoss)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw
      .format("memory")
      .queryName(queryName)
      .outputMode(mode)
      .option("failOnDataLoss", failOnDataLoss.value)

}
