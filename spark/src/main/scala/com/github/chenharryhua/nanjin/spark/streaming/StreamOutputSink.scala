package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.KafkaBrokers
import com.github.chenharryhua.nanjin.kafka.common.TopicName
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJPath}
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.DataStreamWriter
import shapeless.{Coproduct, Poly1}

//http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks

sealed trait StreamOutputSink extends Serializable {
  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A]
}

private object getMode extends Poly1 {

  implicit val append: Case.Aux[StreamOutputMode.Append, String] =
    at[StreamOutputMode.Append](_.value)

  implicit val update: Case.Aux[StreamOutputMode.Update, String] =
    at[StreamOutputMode.Update](_.value)

  implicit val complete: Case.Aux[StreamOutputMode.Complete, String] =
    at[StreamOutputMode.Complete](_.value)
}

@Lenses final case class FileSink(
  mode: StreamOutputMode.Append,
  fileFormat: NJFileFormat,
  path: NJPath,
  checkpoint: NJCheckpoint)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw
      .format(fileFormat.format)
      .outputMode(mode.value)
      .option("path", path.value)
      .option("checkpointLocation", checkpoint.value)
}

object FileSink {

  def append(fileFormat: NJFileFormat, path: NJPath, checkpoint: NJCheckpoint): FileSink =
    FileSink(StreamOutputMode.Append, fileFormat, path, checkpoint)
}

final case class KafkaSink(
  mode: StreamOutputMode,
  brokers: KafkaBrokers,
  topicName: TopicName,
  checkpoint: NJCheckpoint)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw
      .format("kafka")
      .outputMode(mode.value)
      .option("kafka.bootstrap.servers", brokers.value)
      .option("topic", topicName.value)
      .option("checkpointLocation", checkpoint.value)
}

@Lenses final case class ConsoleSink(mode: StreamOutputMode) extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw.format("console").outputMode(mode.value)
}

@Lenses final case class MemorySink(mode: StreamOutputMode, queryName: String)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw.format("memory").queryName(queryName).outputMode(mode.value)
}
