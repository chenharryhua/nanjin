package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.KafkaBrokers
import com.github.chenharryhua.nanjin.kafka.common.TopicName
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.DataStreamWriter
import shapeless._

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
  path: String,
  checkpoint: String)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw
      .format(fileFormat.format)
      .outputMode(mode.value)
      .option("path", path)
      .option("checkpointLocation", checkpoint)
}

object FileSink {

  def withAppendMode(fileFormat: NJFileFormat, path: String, checkpoint: String): FileSink =
    FileSink(StreamOutputMode.Append, fileFormat, path, checkpoint)
}

@Lenses final case class KafkaSink(
  mode: StreamOutputMode.FullMode,
  brokers: KafkaBrokers,
  topicName: TopicName,
  checkpoint: String)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw
      .format("kafka")
      .outputMode(mode.fold(getMode))
      .option("kafka.bootstrap.servers", brokers.value)
      .option("topic", topicName.value)
      .option("checkpointLocation", checkpoint)
}

object KafkaSink {

  def withAppendMode(brokers: KafkaBrokers, topicName: TopicName, checkpoint: String): KafkaSink =
    KafkaSink(
      Coproduct[StreamOutputMode.FullMode](StreamOutputMode.Append),
      brokers,
      topicName,
      checkpoint)

  def withUpdateMode(brokers: KafkaBrokers, topicName: TopicName, checkpoint: String): KafkaSink =
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

  def withAppendMode: ConsoleSink =
    ConsoleSink(Coproduct[StreamOutputMode.FullMode](StreamOutputMode.Append))

  def withUpdateMode: ConsoleSink =
    ConsoleSink(Coproduct[StreamOutputMode.FullMode](StreamOutputMode.Update))

  def withCompleteMode: ConsoleSink =
    ConsoleSink(Coproduct[StreamOutputMode.FullMode](StreamOutputMode.Complete))

}

@Lenses final case class MemorySink(mode: StreamOutputMode.MemoryMode, queryName: String)
    extends StreamOutputSink {

  def sinkOptions[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
    dsw.format("memory").queryName(queryName).outputMode(mode.fold(getMode))
}

object MemorySink {

  def withAppendMode(queryName: String): MemorySink =
    MemorySink(Coproduct[StreamOutputMode.MemoryMode](StreamOutputMode.Append), queryName)

  def withCompleteMode(queryName: String): MemorySink =
    MemorySink(Coproduct[StreamOutputMode.MemoryMode](StreamOutputMode.Complete), queryName)

}
