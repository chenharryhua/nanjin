package com.github.chenharryhua.nanjin.spark

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.{
  AvroDeserialization,
  CirceDeserialization,
  CsvDeserialization
}
import com.sksamuel.avro4s.{Decoder => AvroDecoder}
import fs2.Stream
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.hadoop.conf.Configuration

final class SingleFileSource[F[_]](blocker: Blocker, conf: Configuration) {

  def csv[A: RowDecoder](pathStr: String, csvConfig: CsvConfiguration)(implicit
    F: ConcurrentEffect[F],
    cs: ContextShift[F]): Stream[F, A] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new CsvDeserialization[F, A](csvConfig)
    hadoop.byteStream(pathStr).through(pipe.deserialize)
  }

  def csv[A: RowDecoder](
    pathStr: String)(implicit F: ConcurrentEffect[F], cs: ContextShift[F]): Stream[F, A] =
    csv[A](pathStr, CsvConfiguration.rfc)

  def json[A: JsonDecoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Stream[F, A] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new CirceDeserialization[F, A]
    hadoop.byteStream(pathStr).through(pipe.deserialize)
  }

  def avro[A: AvroDecoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, A] =
    new NJHadoop[F](conf, blocker).avroSource(pathStr)

  def binary[A: AvroDecoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, A] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new AvroDeserialization[F, A]
    hadoop.inputStream(pathStr).flatMap(pipe.fromBinary)
  }

  def jackson[A: AvroDecoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, A] = {
    val hadoop = new NJHadoop[F](conf, blocker)
    val pipe   = new AvroDeserialization[F, A]
    hadoop.inputStream(pathStr).flatMap(pipe.fromJackson)
  }

  def parquet[A: AvroDecoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Stream[F, A] =
    new NJHadoop[F](conf, blocker).parquetSource(pathStr)
}
