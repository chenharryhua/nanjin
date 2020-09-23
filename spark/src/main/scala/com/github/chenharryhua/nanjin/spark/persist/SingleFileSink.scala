package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes._
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import fs2.Pipe
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.avro.file.CodecFactory
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import scalapb.GeneratedMessage

final class SingleFileSink[F[_]](blocker: Blocker, conf: Configuration) {

  def delete(pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): F[Boolean] =
    new NJHadoop[F](conf).delete(pathStr, blocker)

  def isExist(pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): F[Boolean] =
    new NJHadoop[F](conf).isExist(pathStr, blocker)

// 1
  def jackson[A](pathStr: String)(implicit
    enc: AvroEncoder[A],
    ce: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf).byteSink(pathStr, blocker)
    val gr     = new GenericRecordCodec[F, A]
    val pipe   = new JacksonSerialization[F](enc.schema)
    _.through(gr.encode).through(pipe.serialize).through(hadoop)
  }

// 2
  def circe[A](pathStr: String)(implicit
    enc: JsonEncoder[A],
    F: Sync[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf).byteSink(pathStr, blocker)
    val pipe   = new CirceSerialization[F, A]
    _.through(pipe.serialize).through(hadoop)
  }

// 3
  def text[A](
    pathStr: String)(implicit enc: Show[A], F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf).byteSink(pathStr, blocker)
    val pipe   = new TextSerialization[F]
    _.map(enc.show).through(pipe.serialize).through(hadoop)
  }

// 4
  def csv[A](pathStr: String, csvConfig: CsvConfiguration)(implicit
    enc: RowEncoder[A],
    ce: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf).byteSink(pathStr, blocker)
    val pipe   = new CsvSerialization[F, A](csvConfig)
    _.through(pipe.serialize(blocker)).through(hadoop)
  }

  def csv[A](pathStr: String)(implicit
    enc: RowEncoder[A],
    ce: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] =
    csv[A](pathStr, CsvConfiguration.rfc)

// 11
  def parquet[A](pathStr: String, ccn: CompressionCodecName)(implicit
    enc: AvroEncoder[A],
    F: Sync[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf).parquetSink(pathStr, enc.schema, ccn, blocker)
    val pipe   = new GenericRecordCodec[F, A]
    _.through(pipe.encode).through(hadoop)
  }

// 12
  def avro[A](pathStr: String, cf: CodecFactory)(implicit
    enc: AvroEncoder[A],
    F: Sync[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf).avroSink(pathStr, enc.schema, cf, blocker)
    val pipe   = new GenericRecordCodec[F, A]
    _.through(pipe.encode).through(hadoop)
  }

// 13
  def binAvro[A](pathStr: String)(implicit
    enc: AvroEncoder[A],
    ce: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf).byteSink(pathStr, blocker)
    val gr     = new GenericRecordCodec[F, A]
    val pipe   = new BinaryAvroSerialization[F](enc.schema)
    _.through(gr.encode).through(pipe.serialize).through(hadoop)
  }

// 14
  def javaObject[A](
    pathStr: String)(implicit F: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf).byteSink(pathStr, blocker)
    val pipe   = new JavaObjectSerialization[F, A]
    _.through(pipe.serialize).through(hadoop)
  }

// 15
  def protobuf[A](pathStr: String)(implicit
    ce: Concurrent[F],
    cs: ContextShift[F],
    ev: A <:< GeneratedMessage): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf).byteSink(pathStr, blocker)
    val pipe   = new DelimitedProtoBufSerialization[F]
    _.through(pipe.serialize(blocker)).through(hadoop)
  }
}
