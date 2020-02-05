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

@Lenses final case class KafkaSink(
  mode: StreamOutputMode.FullMode,
  brokers: KafkaBrokers,
  topicName: TopicName,
  checkpoint: NJCheckpoint)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw
      .format("kafka")
      .outputMode(mode.fold(getMode))
      .option("kafka.bootstrap.servers", brokers.value)
      .option("topic", topicName.value)
      .option("checkpointLocation", checkpoint.value)
}

object KafkaSink {

  def append(brokers: KafkaBrokers, topicName: TopicName, checkpoint: NJCheckpoint): KafkaSink =
    KafkaSink(
      Coproduct[StreamOutputMode.FullMode](StreamOutputMode.Append),
      brokers,
      topicName,
      checkpoint)

  def update(brokers: KafkaBrokers, topicName: TopicName, checkpoint: NJCheckpoint): KafkaSink =
    KafkaSink(
      Coproduct[StreamOutputMode.FullMode](StreamOutputMode.Update),
      brokers,
      topicName,
      checkpoint)
}

@Lenses final case class ConsoleSink(mode: StreamOutputMode.FullMode) extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw.format("console").outputMode(mode.fold(getMode))
}

object ConsoleSink {

  def append: ConsoleSink =
    ConsoleSink(Coproduct[StreamOutputMode.FullMode](StreamOutputMode.Append))

  def update: ConsoleSink =
    ConsoleSink(Coproduct[StreamOutputMode.FullMode](StreamOutputMode.Update))

  def complete: ConsoleSink =
    ConsoleSink(Coproduct[StreamOutputMode.FullMode](StreamOutputMode.Complete))

}

@Lenses final case class MemorySink(mode: StreamOutputMode.MemoryMode, queryName: String)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw.format("memory").queryName(queryName).outputMode(mode.fold(getMode))
}

object MemorySink {

  def append(queryName: String): MemorySink =
    MemorySink(Coproduct[StreamOutputMode.MemoryMode](StreamOutputMode.Append), queryName)

  def complete(queryName: String): MemorySink =
    MemorySink(Coproduct[StreamOutputMode.MemoryMode](StreamOutputMode.Complete), queryName)

}
