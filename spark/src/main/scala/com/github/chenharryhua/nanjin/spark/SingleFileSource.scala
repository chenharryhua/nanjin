package com.github.chenharryhua.nanjin.spark

import java.io.InputStream

import cats.effect.{Blocker, ContextShift, Sync}
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroInputStreamBuilder,
  SchemaFor,
  Decoder => AvroDecoder
}
import fs2.Stream
import fs2.io.readInputStream
import io.circe.parser.decode
import io.circe.{Decoder => JsonDecoder}
import org.apache.spark.sql.SparkSession

private[spark] trait SingleFileSource extends Serializable {

  private def source[F[_]: Sync: ContextShift, A: SchemaFor: AvroDecoder](
    pathStr: String,
    builder: AvroInputStreamBuilder[A])(
    implicit
    sparkSession: SparkSession): Stream[F, A] =
    for {
      blocker <- Stream.resource(Blocker[F])
      ais <- Stream.resource(
        hadoop.avroInputResource(
          pathStr,
          sparkSession.sparkContext.hadoopConfiguration,
          builder,
          blocker))
      data <- Stream.fromIterator(ais.iterator)
    } yield data

  def avroFileSource[F[_]: Sync: ContextShift, A: SchemaFor: AvroDecoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Stream[F, A] =
    source[F, A](pathStr, AvroInputStream.data[A])

  def jacksonFileSource[F[_]: Sync: ContextShift, A: SchemaFor: AvroDecoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Stream[F, A] =
    source[F, A](pathStr, AvroInputStream.json[A])

  def binaryAvroFileSource[F[_]: Sync: ContextShift, A: SchemaFor: AvroDecoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Stream[F, A] =
    source[F, A](pathStr, AvroInputStream.binary[A])

  def jsonFileSource[F[_]: Sync: ContextShift, A: JsonDecoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Stream[F, A] = {
    val hc = sparkSession.sparkContext.hadoopConfiguration
    for {
      blocker <- Stream.resource(Blocker[F])
      is <- Stream.resource(hadoop.inputPathResource(pathStr, hc, blocker))
      data <- readInputStream(Sync[F].pure[InputStream](is), 4096, blocker)
        .through(fs2.text.utf8Decode)
        .through(fs2.text.lines)
        .map(decode[A])
        .rethrow
    } yield data
  }
}
