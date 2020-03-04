package com.github.chenharryhua.nanjin.pipes

import java.io.InputStream

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroInputStreamBuilder,
  DefaultFieldMapper,
  SchemaFor,
  Decoder => AvroDecoder
}
import fs2.Stream
import fs2.io.readInputStream
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder => JsonDecoder}
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration

final class SingleFileSource[F[_]: ContextShift: Sync](hadoopConfiguration: Configuration) {

  private def source[A: AvroDecoder](
    pathStr: String,
    schema: Schema,
    builder: AvroInputStreamBuilder[A]): Stream[F, A] =
    for {
      blocker <- Stream.resource(Blocker[F])
      ais <- Stream.resource(
        hadoop.avroInputResource(pathStr, schema, hadoopConfiguration, builder, blocker))
      data <- Stream.fromIterator(ais.iterator)
    } yield data

  def avro[A: AvroDecoder](pathStr: String, schema: Schema): Stream[F, A] =
    source[A](pathStr, schema, AvroInputStream.data[A])

  def avro[A: SchemaFor: AvroDecoder](pathStr: String): Stream[F, A] =
    avro(pathStr, SchemaFor[A].schema(DefaultFieldMapper))

  def jackson[A: AvroDecoder](pathStr: String, schema: Schema): Stream[F, A] =
    source[A](pathStr, schema, AvroInputStream.json[A])

  def jackson[A: SchemaFor: AvroDecoder](pathStr: String): Stream[F, A] =
    jackson(pathStr, SchemaFor[A].schema(DefaultFieldMapper))

  def avroBinary[A: AvroDecoder](pathStr: String, schema: Schema): Stream[F, A] =
    source[A](pathStr, schema, AvroInputStream.binary[A])

  def avroBinary[A: SchemaFor: AvroDecoder](pathStr: String): Stream[F, A] =
    avroBinary(pathStr, SchemaFor[A].schema(DefaultFieldMapper))

  def json[A: JsonDecoder](pathStr: String): Stream[F, A] =
    for {
      blocker <- Stream.resource(Blocker[F])
      is <- Stream.resource(hadoop.inputPathResource(pathStr, hadoopConfiguration, blocker))
      data <- readInputStream(Sync[F].pure[InputStream](is), 4096, blocker)
        .through(fs2.text.utf8Decode)
        .through(fs2.text.lines)
        .map(decode[A])
        .rethrow
    } yield data
}
