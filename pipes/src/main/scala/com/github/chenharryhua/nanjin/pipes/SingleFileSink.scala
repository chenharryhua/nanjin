package com.github.chenharryhua.nanjin.pipes

import java.io.OutputStream

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import com.sksamuel.avro4s.{
  AvroOutputStream,
  AvroOutputStreamBuilder,
  DefaultFieldMapper,
  SchemaFor,
  Encoder => AvroEncoder
}
import fs2.io.writeOutputStream
import fs2.{Pipe, Stream}
import io.circe.syntax._
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{rfc, CsvConfiguration, HeaderEncoder}
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration

final class SingleFileSink[F[_]: ContextShift: Sync](hadoopConfiguration: Configuration) {

  private def sink[A](
    pathStr: String,
    schema: Schema,
    builder: AvroOutputStreamBuilder[A]): Pipe[F, A, Unit] = { sa: Stream[F, A] =>
    for {
      blocker <- Stream.resource(Blocker[F])
      aos <- Stream.resource(
        hadoop.avroOutputResource[F, A](pathStr, schema, hadoopConfiguration, builder, blocker))
      data <- sa.chunks
    } yield data.foreach(aos.write)
  }

  def avro[A: AvroEncoder](pathStr: String, schema: Schema): Pipe[F, A, Unit] =
    sink(pathStr, schema, AvroOutputStream.data[A])

  def avro[A: SchemaFor: AvroEncoder](pathStr: String): Pipe[F, A, Unit] =
    avro(pathStr, SchemaFor[A].schema(DefaultFieldMapper))

  def jackson[A: SchemaFor: AvroEncoder](pathStr: String): Pipe[F, A, Unit] =
    sink(pathStr, SchemaFor[A].schema(DefaultFieldMapper), AvroOutputStream.json[A])

  def avroBinary[A: SchemaFor: AvroEncoder](pathStr: String): Pipe[F, A, Unit] =
    sink(pathStr, SchemaFor[A].schema(DefaultFieldMapper), AvroOutputStream.binary[A])

  def json[A: JsonEncoder](pathStr: String): Pipe[F, A, Unit] = { as =>
    for {
      blocker <- Stream.resource(Blocker[F])
      aos <- Stream
        .resource(hadoop.outputPathResource[F](pathStr, hadoopConfiguration, blocker))
        .widen[OutputStream]
      _ <- as
        .map(_.asJson.noSpaces)
        .intersperse("\n")
        .through(fs2.text.utf8Encode)
        .through(writeOutputStream(Sync[F].pure(aos), blocker))
    } yield ()
  }

  def csv[A: HeaderEncoder](
    pathStr: String,
    csvConfig: CsvConfiguration = rfc): Pipe[F, A, Unit] = { as =>
    for {
      blocker <- Stream.resource(Blocker[F])
      aos <- Stream.resource(
        hadoop.csvOutputResource[F, A](pathStr, hadoopConfiguration, blocker, csvConfig))
      data <- as.chunks
    } yield data.foreach(aos.write)
  }
}
