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
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration

final class SingleFileSink(hadoopConfiguration: Configuration) {

  private def sink[F[_]: ContextShift: Sync, A](
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

  def avro[F[_]: ContextShift: Sync, A: AvroEncoder](
    pathStr: String,
    schema: Schema): Pipe[F, A, Unit] =
    sink[F, A](pathStr, schema, AvroOutputStream.data[A])

  def avro[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](pathStr: String): Pipe[F, A, Unit] =
    avro[F, A](pathStr, SchemaFor[A].schema(DefaultFieldMapper))

  def jackson[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](
    pathStr: String): Pipe[F, A, Unit] =
    sink[F, A](pathStr, SchemaFor[A].schema(DefaultFieldMapper), AvroOutputStream.json[A])

  def avroBinary[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](
    pathStr: String): Pipe[F, A, Unit] =
    sink[F, A](pathStr, SchemaFor[A].schema(DefaultFieldMapper), AvroOutputStream.binary[A])

  def parquet[F[_]: ContextShift: Sync, A: AvroEncoder](
    pathStr: String,
    schema: Schema): Pipe[F, A, Unit] = { as =>
    for {
      blocker <- Stream.resource(Blocker[F])
      writer <- Stream.resource(
        hadoop.parquetOutputResource[F](pathStr, schema, hadoopConfiguration, blocker))
      data <- as.chunks
    } yield data.foreach { m =>
      val rec = AvroEncoder[A].encode(m, schema, DefaultFieldMapper) match {
        case gr: GenericRecord => gr
        case _                 => throw new Exception(s"can not be converted to Generic Record. ${m.toString}")
      }
      writer.write(rec)
    }
  }

  def parquet[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](
    pathStr: String): Pipe[F, A, Unit] =
    parquet[F, A](pathStr, SchemaFor[A].schema(DefaultFieldMapper))

  def json[F[_]: ContextShift: Sync, A: JsonEncoder](pathStr: String): Pipe[F, A, Unit] = { as =>
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

  def csv[F[_]: ContextShift: Sync, A: HeaderEncoder](
    pathStr: String,
    csvConfig: CsvConfiguration): Pipe[F, A, Unit] = { as =>
    for {
      blocker <- Stream.resource(Blocker[F])
      aos <- Stream.resource(
        hadoop.csvOutputResource[F, A](pathStr, hadoopConfiguration, blocker, csvConfig))
      data <- as.chunks
    } yield data.foreach(aos.write)
  }

  def csv[F[_]: ContextShift: Sync, A: HeaderEncoder](pathStr: String): Pipe[F, A, Unit] =
    csv[F,A](pathStr, rfc)

}
