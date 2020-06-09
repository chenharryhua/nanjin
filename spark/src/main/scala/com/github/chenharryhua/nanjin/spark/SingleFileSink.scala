package com.github.chenharryhua.nanjin.spark

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.{
  AvroSerialization,
  CirceSerialization,
  CsvSerialization
}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import fs2.{Pipe, Stream}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.hadoop.conf.Configuration

final class SingleFileSink[F[_]](blocker: Blocker, conf: Configuration) {

  def csv[A: RowEncoder](pathStr: String, csvConfig: CsvConfiguration)(implicit
    F: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new CsvSerialization[F, A](csvConfig)
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(hadoop.sink(pathStr))
  }

  def csv[A: RowEncoder](
    pathStr: String)(implicit F: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, Unit] =
    csv[A](pathStr, CsvConfiguration.rfc)

  def json[A: JsonEncoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new CirceSerialization[F, A]
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(hadoop.sink(pathStr))
  }

  def avro[A: AvroEncoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    (ss: Stream[F, A]) => ss.through(hadoop.avroSink(pathStr))
  }

  def binary[A: AvroEncoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new AvroSerialization[F, A]
    (ss: Stream[F, A]) => ss.through(pipe.toBinary).through(hadoop.sink(pathStr))
  }

  def jackson[A: AvroEncoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new AvroSerialization[F, A]
    (ss: Stream[F, A]) => ss.through(pipe.toByteJson).through(hadoop.sink(pathStr))
  }

  def parquet[A: AvroEncoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val schema = AvroEncoder[A].schema
    (ss: Stream[F, A]) => ss.through(hadoop.parquetSink(pathStr))
  }

  def delete(pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): F[Boolean] =
    new NJHadoop[F](conf, blocker).delete(pathStr)
}
