package com.github.chenharryhua.nanjin.pipes

import java.io.InputStream

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroInputStreamBuilder,
  SchemaFor,
  Decoder => AvroDecoder
}
import fs2.Stream
import fs2.io.readInputStream
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder => JsonDecoder}
import org.apache.hadoop.conf.Configuration

object sources {

  private def source[F[_]: Sync: ContextShift, A: SchemaFor: AvroDecoder](
    pathStr: String,
    builder: AvroInputStreamBuilder[A],
    hadoopConfiguration: Configuration): Stream[F, A] =
    for {
      blocker <- Stream.resource(Blocker[F])
      ais <- Stream.resource(
        hadoop.avroInputResource(pathStr, hadoopConfiguration, builder, blocker))
      data <- Stream.fromIterator(ais.iterator)
    } yield data

  def avroFileSource[F[_]: Sync: ContextShift, A: SchemaFor: AvroDecoder](
    pathStr: String,
    hadoopConfiguration: Configuration): Stream[F, A] =
    source[F, A](pathStr, AvroInputStream.data[A], hadoopConfiguration)

  def jacksonFileSource[F[_]: Sync: ContextShift, A: SchemaFor: AvroDecoder](
    pathStr: String,
    hadoopConfiguration: Configuration): Stream[F, A] =
    source[F, A](pathStr, AvroInputStream.json[A], hadoopConfiguration)

  def binaryAvroFileSource[F[_]: Sync: ContextShift, A: SchemaFor: AvroDecoder](
    pathStr: String,
    hadoopConfiguration: Configuration): Stream[F, A] =
    source[F, A](pathStr, AvroInputStream.binary[A], hadoopConfiguration)

  def jsonFileSource[F[_]: Sync: ContextShift, A: JsonDecoder](
    pathStr: String,
    hadoopConfiguration: Configuration): Stream[F, A] =
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
