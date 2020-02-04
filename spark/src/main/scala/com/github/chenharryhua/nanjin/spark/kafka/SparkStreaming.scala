package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Concurrent
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.common.TopicName
import enumeratum.CatsEnum
import enumeratum.values.IntEnumEntry
import frameless.TypedEncoder
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import enumeratum.{CatsEnum, Enum, EnumEntry}
import shapeless._

import scala.collection.immutable

//http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes
sealed abstract class StreamOutputMode(val value: String) extends EnumEntry

object StreamOutputMode extends Enum[StreamOutputMode] with CatsEnum[StreamOutputMode] {
  override val values: immutable.IndexedSeq[StreamOutputMode] = findValues
  case object Append extends StreamOutputMode("append")
  case object Update extends StreamOutputMode("update")
  case object Complete extends StreamOutputMode("complete")

  type Append   = Append.type
  type Update   = Update.type
  type Complete = Complete.type

  type FileSinkMode         = Append
  type KafkaSinkMode        = Append :+: Update :+: Complete :+: CNil
  type ForeachSinkMode      = Append :+: Update :+: Complete :+: CNil
  type ForeachBatchSinkMode = Append :+: Update :+: Complete :+: CNil
  type ConsoleSinkMode      = Append :+: Update :+: Complete :+: CNil
  type MemorySinkMode       = Append :+: Complete :+: CNil
}

sealed abstract class StreamOutputSink {
  def withOption[A](dsw: DataStreamWriter[A]): DataStreamWriter[A]
}

object StreamOutputSink {

  final case class FileSink(mode: StreamOutputMode, fileFormat: NJFileFormat, path: String)
      extends StreamOutputSink {

    def withOption[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
      dsw.format(fileFormat.format).outputMode(mode.value).option("path", path)
  }

  final case class KafkaSink(mode: StreamOutputMode, broker: String, topicName: TopicName)
      extends StreamOutputSink {

    def withOption[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
      dsw
        .format("kafka")
        .outputMode(mode.value)
        .option("kafka.bootstrap.servers", broker)
        .option("topic", topicName.value)
  }

  final case class ForeachSink(mode: StreamOutputMode) extends StreamOutputSink {
    def withOption[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] = dsw.outputMode(mode.value)

  }

  final case class ForeachBatchSink(mode: StreamOutputMode) extends StreamOutputSink {
    def withOption[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] = dsw.outputMode(mode.value)

  }

  final case class ConsoleSink(mode: StreamOutputMode, showDs: ShowSparkDataset)
      extends StreamOutputSink {

    def withOption[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
      dsw.format("console").outputMode(mode.value)
  }

  final case class MemorySink(mode: StreamOutputMode, queryName: String) extends StreamOutputSink {

    def withOption[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] =
      dsw.format("memory").queryName(queryName).outputMode(mode.value)
  }
}

final class SparkStreaming[F[_], A: TypedEncoder](ds: Dataset[A]) extends Serializable {

  def transform[B: TypedEncoder](f: Dataset[A] => Dataset[B]) =
    new SparkStreaming[F, B](f(ds))

  def run(implicit F: Concurrent[F]): F[Unit] = {
    val ss = ds.writeStream.outputMode(OutputMode.Update).format("console")
    F.bracket(F.delay(ss.start))(s => F.delay(s.awaitTermination()))(_ => F.pure(()))
  }
}
