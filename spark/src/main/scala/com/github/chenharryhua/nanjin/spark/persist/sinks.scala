package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.kernel.{Async, Sync}
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.pipes.serde.*
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJParquet, NJPath}
import com.sksamuel.avro4s.{Encoder as AvroEncoder, ToRecord}
import fs2.{Pipe, Stream}
import io.circe.Encoder as JsonEncoder
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter
import scalapb.GeneratedMessage
import squants.information.Information

object sinks {

  def avro[F[_]: Sync, A](
    path: NJPath,
    cfg: Configuration,
    encoder: AvroEncoder[A],
    cf: CodecFactory): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val toRec: ToRecord[A]                 = ToRecord(encoder)
    val sink: Pipe[F, GenericRecord, Unit] = NJHadoop[F](cfg).avroSink(path, encoder.schema, cf)
    ss.map(toRec.to).through(sink)
  }

  def binAvro[F[_]: Sync, A](path: NJPath, cfg: Configuration, encoder: AvroEncoder[A]): Pipe[F, A, Unit] = {
    (ss: Stream[F, A]) =>
      val toRec: ToRecord[A]                 = ToRecord(encoder)
      val pipe: Pipe[F, GenericRecord, Byte] = new BinaryAvroSerialization[F](encoder.schema).serialize
      val sink: Pipe[F, Byte, Unit]          = NJHadoop[F](cfg).byteSink(path)
      ss.map(toRec.to).through(pipe.andThen(sink))
  }

  def jackson[F[_]: Sync, A](
    path: NJPath,
    cfg: Configuration,
    encoder: AvroEncoder[A],
    compression: Pipe[F, Byte, Byte]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val toRec: ToRecord[A]                 = ToRecord(encoder)
    val pipe: Pipe[F, GenericRecord, Byte] = new JacksonSerialization[F](encoder.schema).serialize
    val sink: Pipe[F, Byte, Unit]          = NJHadoop[F](cfg).byteSink(path)
    ss.map(toRec.to).through(pipe.andThen(compression).andThen(sink))
  }

  def parquet[F[_]: Sync, A](
    builder: AvroParquetWriter.Builder[GenericRecord],
    encoder: AvroEncoder[A]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val toRec: ToRecord[A]                 = ToRecord(encoder)
    val sink: Pipe[F, GenericRecord, Unit] = NJParquet.fs2Sink[F](builder)
    ss.map(toRec.to).through(sink)
  }

  def circe[F[_]: Sync, A: JsonEncoder](
    path: NJPath,
    cfg: Configuration,
    isKeepNull: Boolean,
    compression: Pipe[F, Byte, Byte]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, A, Byte]    = new CirceSerialization[F, A].serialize(isKeepNull)
    val sink: Pipe[F, Byte, Unit] = NJHadoop[F](cfg).byteSink(path)
    ss.through(pipe.andThen(compression).andThen(sink))
  }

  def text[F[_]: Sync, A: Show](
    path: NJPath,
    cfg: Configuration,
    compression: Pipe[F, Byte, Byte]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, String, Byte] = new TextSerialization[F].serialize
    val sink: Pipe[F, Byte, Unit]   = NJHadoop[F](cfg).byteSink(path)
    ss.map(_.show).through(pipe.andThen(compression).andThen(sink))
  }

  def protobuf[F[_]: Async, A](path: NJPath, cfg: Configuration, byteBuffer: Information)(implicit
    enc: A <:< GeneratedMessage): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, A, Byte]    = new DelimitedProtoBufSerialization[F].serialize(byteBuffer)
    val sink: Pipe[F, Byte, Unit] = NJHadoop[F](cfg).byteSink(path)
    ss.through(pipe.andThen(sink))
  }

  def csv[F[_]: Async, A: RowEncoder](
    path: NJPath,
    cfg: Configuration,
    csvConf: CsvConfiguration,
    compression: Pipe[F, Byte, Byte],
    byteBuffer: Information): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, A, Byte]    = new CsvSerialization[F, A](csvConf).serialize(byteBuffer)
    val sink: Pipe[F, Byte, Unit] = NJHadoop[F](cfg).byteSink(path)
    ss.through(pipe.andThen(compression).andThen(sink))
  }
}
