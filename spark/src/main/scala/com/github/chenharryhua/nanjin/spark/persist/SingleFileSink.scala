package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes._
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import fs2.Pipe
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.hadoop.conf.Configuration
import scalapb.GeneratedMessage

final class SingleFileSink[F[_]](blocker: Blocker, conf: Configuration) {

  def delete(pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): F[Boolean] =
    new NJHadoop[F](conf, blocker).delete(pathStr)

  def isExist(pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): F[Boolean] =
    new NJHadoop[F](conf, blocker).isExist(pathStr)

// 1
  def jackson[A](pathStr: String)(implicit
    enc: AvroEncoder[A],
    ce: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val gr     = new GenericRecordEncoder[F, A](enc)
    val pipe   = new JacksonSerialization[F](enc.schema)
    _.through(gr.encode).through(pipe.serialize).through(hadoop)
  }

// 2
  def circe[A](pathStr: String)(implicit
    enc: JsonEncoder[A],
    F: Sync[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new CirceSerialization[F, A](enc)
    _.through(pipe.serialize).through(hadoop)
  }

// 3
  def text[A](
    pathStr: String)(implicit enc: Show[A], F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new TextSerialization[F]
    _.map(enc.show).through(pipe.serialize).through(hadoop)
  }

// 4
  def csv[A](pathStr: String, csvConfig: CsvConfiguration)(implicit
    enc: RowEncoder[A],
    ce: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new CsvSerialization[F, A](enc, csvConfig, blocker)
    _.through(pipe.serialize).through(hadoop)
  }

  def csv[A](pathStr: String)(implicit
    enc: RowEncoder[A],
    ce: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] =
    csv[A](pathStr, CsvConfiguration.rfc)

// 11
  def parquet[A](pathStr: String)(implicit
    enc: AvroEncoder[A],
    F: Sync[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).parquetSink(pathStr, enc.schema)
    val pipe   = new GenericRecordEncoder[F, A](enc)
    _.through(pipe.encode).through(hadoop)
  }

// 12
  def avro[A](pathStr: String)(implicit
    enc: AvroEncoder[A],
    F: Sync[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).avroSink(pathStr, enc.schema)
    val pipe   = new GenericRecordEncoder[F, A](enc)
    _.through(pipe.encode).through(hadoop)
  }

// 13
  def binAvro[A](pathStr: String)(implicit
    enc: AvroEncoder[A],
    ce: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val gr     = new GenericRecordEncoder[F, A](enc)
    val pipe   = new BinaryAvroSerialization[F](enc.schema)
    _.through(gr.encode).through(pipe.serialize).through(hadoop)
  }

// 14
  def javaObject[A](
    pathStr: String)(implicit F: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new JavaObjectSerialization[F, A]
    _.through(pipe.serialize).through(hadoop)
  }

// 15
  def protobuf[A](pathStr: String)(implicit
    ce: Concurrent[F],
    cs: ContextShift[F],
    ev: A <:< GeneratedMessage): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new DelimitedProtoBufSerialization[F, A](blocker)
    _.through(pipe.serialize).through(hadoop)
  }

}
