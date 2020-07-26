package com.github.chenharryhua.nanjin.spark

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes._
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.TypedEncoder
import fs2.Pipe
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.hadoop.conf.Configuration
import scalapb.GeneratedMessage

final class SingleFileSink[F[_]](blocker: Blocker, conf: Configuration) {

  def delete(pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): F[Boolean] =
    new NJHadoop[F](conf, blocker).delete(pathStr)

  // text
  def csv[A: RowEncoder](pathStr: String, csvConfig: CsvConfiguration)(implicit
    ce: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new CsvSerialization[F, A](csvConfig, blocker)
    _.through(pipe.serialize).through(hadoop)
  }

  def csv[A: RowEncoder](
    pathStr: String)(implicit ce: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, Unit] =
    csv[A](pathStr, CsvConfiguration.rfc)

  def circe[A: JsonEncoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new CirceSerialization[F, A]
    _.through(pipe.serialize).through(hadoop)
  }

  def text[A: Show](pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new TextSerialization[F]
    _.map(_.show).through(pipe.serialize).through(hadoop)
  }

  // java native serialization
  def javaObject[A](
    pathStr: String)(implicit F: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new JavaObjectSerialization[F, A]
    _.through(pipe.serialize).through(hadoop)
  }

  // avro
  def binAvro[A: AvroEncoder](
    pathStr: String)(implicit ce: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val gr     = new GenericRecordEncoder[F, A]
    val pipe   = new BinaryAvroSerialization[F](AvroEncoder[A].schema)
    _.through(gr.encode).through(pipe.serialize).through(hadoop)
  }

  def jackson[A: AvroEncoder](
    pathStr: String)(implicit ce: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val gr     = new GenericRecordEncoder[F, A]
    val pipe   = new JacksonSerialization[F](AvroEncoder[A].schema)
    _.through(gr.encode).through(pipe.serialize).through(hadoop)
  }

  def avro[A: AvroEncoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).avroSink(pathStr, AvroEncoder[A].schema)
    val pipe   = new GenericRecordEncoder[F, A]
    _.through(pipe.encode).through(hadoop)
  }

  def parquet[A: AvroEncoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).parquetSink(pathStr, AvroEncoder[A].schema)
    val pipe   = new GenericRecordEncoder[F, A]
    _.through(pipe.encode).through(hadoop)
  }

  // protobuf
  def protobuf[A](pathStr: String)(implicit
    ce: Concurrent[F],
    cs: ContextShift[F],
    ev: A <:< GeneratedMessage): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new DelimitedProtoBufSerialization[F, A](blocker)
    _.through(pipe.serialize).through(hadoop)
  }
}
