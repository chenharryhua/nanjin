package com.github.chenharryhua.nanjin.spark

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes._
import com.sksamuel.avro4s.{Decoder => AvroDecoder}
import fs2.Stream
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.hadoop.conf.Configuration
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

final class SingleFileSource[F[_]](blocker: Blocker, conf: Configuration) {

  // text
  def csv[A: RowDecoder](pathStr: String, csvConfig: CsvConfiguration)(implicit
    F: ConcurrentEffect[F],
    cs: ContextShift[F]): Stream[F, A] = {
    val pipe = new CsvDeserialization[F, A](csvConfig)
    new NJHadoop[F](conf, blocker).byteStream(pathStr).through(pipe.deserialize)
  }

  def csv[A: RowDecoder](
    pathStr: String)(implicit F: ConcurrentEffect[F], cs: ContextShift[F]): Stream[F, A] =
    csv[A](pathStr, CsvConfiguration.rfc)

  def json[A: JsonDecoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Stream[F, A] = {
    val pipe = new CirceDeserialization[F, A]
    new NJHadoop[F](conf, blocker).byteStream(pathStr).through(pipe.deserialize)
  }

  def text(
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, String] = {
    val pipe = new TextDeserialization[F]
    new NJHadoop[F](conf, blocker).byteStream(pathStr).through(pipe.deserialize)
  }

  // avro
  def avro[A: AvroDecoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, A] = {
    val pipe = new GenericRecordDecoder[F, A]
    new NJHadoop[F](conf, blocker).avroSource(pathStr).through(pipe.decode)
  }

  def binary[A: AvroDecoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, A] = {
    val pipe = new BinaryAvroDeserialization[F](AvroDecoder[A].schema)
    val gr   = new GenericRecordDecoder[F, A]
    new NJHadoop[F](conf, blocker).byteStream(pathStr).through(pipe.deserialize).through(gr.decode)
  }

  def jackson[A: AvroDecoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, A] = {
    val pipe = new JacksonDeserialization[F](AvroDecoder[A].schema)
    val gr   = new GenericRecordDecoder[F, A]
    new NJHadoop[F](conf, blocker).byteStream(pathStr).through(pipe.deserialize).through(gr.decode)
  }

  def parquet[A: AvroDecoder](
    pathStr: String)(implicit F: Sync[F], cs: ContextShift[F]): Stream[F, A] = {
    val pipe = new GenericRecordDecoder[F, A]
    new NJHadoop[F](conf, blocker).parquetSource(pathStr).through(pipe.decode)
  }

  def javaObject[A](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, A] = {
    val pipe = new JavaObjectDeserialization[F, A]
    new NJHadoop[F](conf, blocker).byteStream(pathStr).through(pipe.deserialize)
  }

  //
  def protobuf[A <: GeneratedMessage](pathStr: String)(implicit
    cs: ContextShift[F],
    ce: ConcurrentEffect[F],
    ev: GeneratedMessageCompanion[A]): Stream[F, A] = {
    val pipe = new ProtoBufDeserialization[F, A]()
    new NJHadoop[F](conf, blocker).byteStream(pathStr).through(pipe.deserialize)
  }
}
