package com.github.chenharryhua.nanjin.spark

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.{
  AvroSerialization,
  CirceSerialization,
  CsvSerialization,
  GenericRecordSerialization
}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import fs2.{Pipe, Stream}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import org.apache.hadoop.conf.Configuration

final class SingleFileSink[F[_]](blocker: Blocker, conf: Configuration) {

  def csv[A: HeaderEncoder](pathStr: String, csvConfig: CsvConfiguration)(implicit
    F: Sync[F],
    cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new CsvSerialization[F, A](csvConfig)
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(hadoop.sink(pathStr))
  }

  def csv[A: HeaderEncoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] =
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
    val pipe   = new AvroSerialization[F, A](blocker)
    (ss: Stream[F, A]) => ss.through(pipe.toData).through(hadoop.sink(pathStr))
  }

  def binary[A: AvroEncoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new AvroSerialization[F, A](blocker)
    (ss: Stream[F, A]) => ss.through(pipe.toBinary).through(hadoop.sink(pathStr))
  }

  def jackson[A: AvroEncoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new AvroSerialization[F, A](blocker)
    (ss: Stream[F, A]) => ss.through(pipe.toByteJson).through(hadoop.sink(pathStr))
  }

  def parquet[A: AvroEncoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Pipe[F, A, Unit] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new GenericRecordSerialization[F, A]
    val schema = AvroEncoder[A].schema
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(hadoop.parquetSink(pathStr, schema))
  }

  def delete(pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): F[Boolean] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    hadoop.delete(pathStr)
  }
}
