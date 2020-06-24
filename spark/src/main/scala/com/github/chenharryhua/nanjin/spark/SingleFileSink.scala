package com.github.chenharryhua.nanjin.spark

import cats.effect.{Blocker, Concurrent, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes._
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import fs2.{Pipe, Stream}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.hadoop.conf.Configuration

final class SingleFileSink[F[_]](blocker: Blocker, conf: Configuration) {

  // text
  def csv[A: RowEncoder](pathStr: String, csvConfig: CsvConfiguration)(implicit
    ce: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new CsvSerialization[F, A](csvConfig)
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(hadoop)
  }

  def csv[A: RowEncoder](
    pathStr: String)(implicit ce: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, Unit] =
    csv[A](pathStr, CsvConfiguration.rfc)

  def json[A: JsonEncoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new CirceSerialization[F, A]
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(hadoop)
  }

  def text(pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, String, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new TextSerialization[F]
    (ss: Stream[F, String]) => ss.through(pipe.serialize).through(hadoop)
  }

  // avro
  def binary[A: AvroEncoder](
    pathStr: String)(implicit ce: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new AvroSerialization[F, A]
    (ss: Stream[F, A]) => ss.through(pipe.toBinary).through(hadoop)
  }

  def jackson[A: AvroEncoder](
    pathStr: String)(implicit ce: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).byteSink(pathStr)
    val pipe   = new AvroSerialization[F, A]
    (ss: Stream[F, A]) => ss.through(pipe.toByteJson).through(hadoop)
  }

  def avro[A: AvroEncoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).avroSink(pathStr, AvroEncoder[A].schema)
    val pipe   = new GenericRecordSerialization
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(hadoop)
  }

  def parquet[A: AvroEncoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker).parquetSink(pathStr, AvroEncoder[A].schema)
    val pipe   = new GenericRecordSerialization
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(hadoop)
  }

  def delete(pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): F[Boolean] =
    new NJHadoop[F](conf, blocker).delete(pathStr)
}
