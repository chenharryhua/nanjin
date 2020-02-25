package com.github.chenharryhua.nanjin.pipes

import java.io.OutputStream

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import com.sksamuel.avro4s.{
  AvroOutputStream,
  AvroOutputStreamBuilder,
  SchemaFor,
  Encoder => AvroEncoder
}
import fs2.io.writeOutputStream
import fs2.{Pipe, Stream}
import io.circe.syntax._
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{rfc, CsvConfiguration, HeaderEncoder}
import org.apache.hadoop.conf.Configuration

object sinks {

  private def sink[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](
    pathStr: String,
    builder: AvroOutputStreamBuilder[A],
    hadoopConfiguration: Configuration): Pipe[F, A, Unit] = { sa: Stream[F, A] =>
    for {
      blocker <- Stream.resource(Blocker[F])
      aos <- Stream.resource(
        hadoop.avroOutputResource[F, A](pathStr, hadoopConfiguration, builder, blocker))
      data <- sa.chunks
    } yield data.foreach(aos.write)
  }

  def avroFileSink[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](
    pathStr: String,
    hadoopConfiguration: Configuration): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.data[A], hadoopConfiguration)

  def jacksonFileSink[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](
    pathStr: String,
    hadoopConfiguration: Configuration): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.json[A], hadoopConfiguration)

  def binaryAvroFileSink[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](
    pathStr: String,
    hadoopConfiguration: Configuration): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.binary[A], hadoopConfiguration)

  def jsonFileSink[F[_]: ContextShift: Sync, A: JsonEncoder](
    pathStr: String,
    hadoopConfiguration: Configuration): Pipe[F, A, Unit] = { as =>
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

  def csvFileSink[F[_]: ContextShift: Sync, A: HeaderEncoder](
    pathStr: String,
    hadoopConfiguration: Configuration,
    csvConfig: CsvConfiguration = rfc): Pipe[F, A, Unit] = { as =>
    for {
      blocker <- Stream.resource(Blocker[F])
      aos <- Stream.resource(
        hadoop.csvOutputResource[F, A](pathStr, hadoopConfiguration, blocker, csvConfig))
      data <- as.chunks
    } yield data.foreach(aos.write)
  }
}
