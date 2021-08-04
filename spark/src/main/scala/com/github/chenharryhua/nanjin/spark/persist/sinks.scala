package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.kernel.{Async, Sync}
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.pipes.*
import com.github.chenharryhua.nanjin.pipes.serde.{
  BinaryAvroSerialization,
  CirceSerialization,
  CsvSerialization,
  DelimitedProtoBufSerialization,
  GenericRecordCodec,
  JacksonSerialization,
  TextSerialization
}
import com.github.chenharryhua.nanjin.terminals.NJHadoop
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.{Pipe, Stream}
import io.circe.Encoder as JsonEncoder
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import scalapb.GeneratedMessage

object sinks {

  def avro[F[_]: Sync, A](
    path: String,
    cfg: Configuration,
    encoder: AvroEncoder[A],
    cf: CodecFactory): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val grc: Pipe[F, A, GenericRecord]     = new GenericRecordCodec[F, A].encode(encoder)
    val sink: Pipe[F, GenericRecord, Unit] = NJHadoop[F](cfg).avroSink(path, encoder.schema, cf)
    ss.through(grc).through(sink)
  }

  def binAvro[F[_]: Sync, A](path: String, cfg: Configuration, encoder: AvroEncoder[A]): Pipe[F, A, Unit] = {
    (ss: Stream[F, A]) =>
      val grc: Pipe[F, A, GenericRecord]     = new GenericRecordCodec[F, A].encode(encoder)
      val pipe: Pipe[F, GenericRecord, Byte] = new BinaryAvroSerialization[F](encoder.schema).serialize
      val sink: Pipe[F, Byte, Unit]          = NJHadoop[F](cfg).byteSink(path)
      ss.through(grc).through(pipe).through(sink)
  }

  def jackson[F[_]: Sync, A](
    path: String,
    cfg: Configuration,
    encoder: AvroEncoder[A],
    compression: Pipe[F, Byte, Byte]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val grc: Pipe[F, A, GenericRecord]     = new GenericRecordCodec[F, A].encode(encoder)
    val pipe: Pipe[F, GenericRecord, Byte] = new JacksonSerialization[F](encoder.schema).serialize
    val sink: Pipe[F, Byte, Unit]          = NJHadoop[F](cfg).byteSink(path)
    ss.through(grc).through(pipe).through(compression).through(sink)
  }

  def parquet[F[_]: Sync, A](
    path: String,
    cfg: Configuration,
    encoder: AvroEncoder[A],
    ccn: CompressionCodecName): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val grc: Pipe[F, A, GenericRecord]     = new GenericRecordCodec[F, A].encode(encoder)
    val sink: Pipe[F, GenericRecord, Unit] = NJHadoop[F](cfg).parquetSink(path, encoder.schema, ccn)
    ss.through(grc).through(sink)
  }

  def circe[F[_]: Sync, A: JsonEncoder](
    path: String,
    cfg: Configuration,
    isKeepNull: Boolean,
    compression: Pipe[F, Byte, Byte]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, A, Byte]    = new CirceSerialization[F, A].serialize(isKeepNull)
    val sink: Pipe[F, Byte, Unit] = NJHadoop[F](cfg).byteSink(path)
    ss.through(pipe).through(compression).through(sink)
  }

  def text[F[_]: Sync, A: Show](
    path: String,
    cfg: Configuration,
    compression: Pipe[F, Byte, Byte]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, String, Byte] = new TextSerialization[F].serialize
    val sink: Pipe[F, Byte, Unit]   = NJHadoop[F](cfg).byteSink(path)
    ss.map(_.show).through(pipe).through(compression).through(sink)
  }

  def protobuf[F[_]: Async, A](path: String, cfg: Configuration)(implicit
    enc: A <:< GeneratedMessage): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, A, Byte]    = new DelimitedProtoBufSerialization[F].serialize
    val sink: Pipe[F, Byte, Unit] = NJHadoop[F](cfg).byteSink(path)
    ss.through(pipe).through(sink)
  }

  def csv[F[_]: Async, A: RowEncoder](
    path: String,
    cfg: Configuration,
    csvConf: CsvConfiguration,
    compression: Pipe[F, Byte, Byte]): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val pipe: Pipe[F, A, Byte]    = new CsvSerialization[F, A](csvConf).serialize
    val sink: Pipe[F, Byte, Unit] = NJHadoop[F](cfg).byteSink(path)
    ss.through(pipe).through(compression).through(sink)
  }
}
