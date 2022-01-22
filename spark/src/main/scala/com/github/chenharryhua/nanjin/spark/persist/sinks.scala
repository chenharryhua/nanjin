package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.kernel.{Async, Sync}
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.pipes.serde.*
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJParquet, NJPath}
import com.sksamuel.avro4s.Encoder as AvroEncoder
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
    val grc: Pipe[F, A, GenericRecord]     = new GenericRecordCodec[F, A].encode(encoder)
    val sink: Pipe[F, GenericRecord, Unit] = NJHadoop[F](cfg).avroSink(path, encoder.schema, cf)
    ss.through(grc).through(sink)
  }

  def binAvro[F[_]: Sync, A](
    path: NJPath,
    cfg: Configuration,
    encoder: AvroEncoder[A],
    chunkSize: ChunkSize): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val grc: Pipe[F, A, GenericRecord]     = new GenericRecordCodec[F, A].encode(encoder)
    val pipe: Pipe[F, GenericRecord, Byte] = new BinaryAvroSerialization[F](encoder.schema).serialize(chunkSize)
    val sink: Pipe[F, Byte, Unit]          = NJHadoop[F](cfg).byteSink(path)
    ss.through(grc).through(pipe).through(sink)
  }

  def jackson[F[_]: Sync, A](
    path: NJPath,
    cfg: Configuration,
    encoder: AvroEncoder[A],
    compression: Pipe[F, Byte, Byte],
    chunkSize: ChunkSize): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val grc: Pipe[F, A, GenericRecord]     = new GenericRecordCodec[F, A].encode(encoder)
    val pipe: Pipe[F, GenericRecord, Byte] = new JacksonSerialization[F](encoder.schema).serialize(chunkSize)
    val sink: Pipe[F, Byte, Unit]          = NJHadoop[F](cfg).byteSink(path)
    ss.through(grc).through(pipe).through(compression).through(sink)
  }

  def parquet[F[_]: Sync, A](
    builder: AvroParquetWriter.Builder[GenericRecord],
    encoder: AvroEncoder[A]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val grc: Pipe[F, A, GenericRecord]     = new GenericRecordCodec[F, A].encode(encoder)
    val sink: Pipe[F, GenericRecord, Unit] = new NJParquet[F].parquetSink(builder)
    ss.through(grc).through(sink)
  }

  def circe[F[_]: Sync, A: JsonEncoder](
    path: NJPath,
    cfg: Configuration,
    isKeepNull: Boolean,
    compression: Pipe[F, Byte, Byte]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, A, Byte]    = new CirceSerialization[F, A].serialize(isKeepNull)
    val sink: Pipe[F, Byte, Unit] = NJHadoop[F](cfg).byteSink(path)
    ss.through(pipe).through(compression).through(sink)
  }

  def text[F[_]: Sync, A: Show](
    path: NJPath,
    cfg: Configuration,
    compression: Pipe[F, Byte, Byte]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, String, Byte] = new TextSerialization[F].serialize
    val sink: Pipe[F, Byte, Unit]   = NJHadoop[F](cfg).byteSink(path)
    ss.map(_.show).through(pipe).through(compression).through(sink)
  }

  def protobuf[F[_]: Async, A](path: NJPath, cfg: Configuration, byteBuffer: Information)(implicit
    enc: A <:< GeneratedMessage): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, A, Byte]    = new DelimitedProtoBufSerialization[F].serialize(byteBuffer)
    val sink: Pipe[F, Byte, Unit] = NJHadoop[F](cfg).byteSink(path)
    ss.through(pipe).through(sink)
  }

  def csv[F[_]: Async, A: RowEncoder](
    path: NJPath,
    cfg: Configuration,
    csvConf: CsvConfiguration,
    compression: Pipe[F, Byte, Byte],
    byteBuffer: Information): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, A, Byte]    = new CsvSerialization[F, A](csvConf).serialize(byteBuffer)
    val sink: Pipe[F, Byte, Unit] = NJHadoop[F](cfg).byteSink(path)
    ss.through(pipe).through(compression).through(sink)
  }
}
