package com.github.chenharryhua.nanjin.spark

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
import kantan.csv.HeaderEncoder
import org.apache.spark.sql.SparkSession

private[spark] trait SingleFileSink extends Serializable {

  private def sink[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](
    pathStr: String,
    builder: AvroOutputStreamBuilder[A])(
    implicit
    sparkSession: SparkSession): Pipe[F, A, Unit] = { sa: Stream[F, A] =>
    for {
      blocker <- Stream.resource(Blocker[F])
      aos <- Stream.resource(
        hadoop.avroOutputResource[F, A](
          pathStr,
          sparkSession.sparkContext.hadoopConfiguration,
          builder,
          blocker))
      data <- sa.chunks
    } yield data.foreach(aos.write)
  }

  def avroFileSink[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.data[A])

  def jacksonFileSink[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.json[A])

  def binaryAvroFileSink[F[_]: ContextShift: Sync, A: SchemaFor: AvroEncoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.binary[A])

  def jsonFileSink[F[_]: ContextShift: Sync, A: JsonEncoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Pipe[F, A, Unit] = {
    val hc = sparkSession.sparkContext.hadoopConfiguration
    as =>
      for {
        blocker <- Stream.resource(Blocker[F])
        aos <- Stream
          .resource(hadoop.outputPathResource[F](pathStr, hc, blocker))
          .widen[OutputStream]
        _ <- as
          .map(_.asJson.noSpaces)
          .intersperse("\n")
          .through(fs2.text.utf8Encode)
          .through(writeOutputStream(Sync[F].pure(aos), blocker))
      } yield ()
  }

  def csvFileSink[F[_]: ContextShift: Sync, A: HeaderEncoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Pipe[F, A, Unit] = {
    val hc = sparkSession.sparkContext.hadoopConfiguration
    as =>
      for {
        blocker <- Stream.resource(Blocker[F])
        aos <- Stream.resource(hadoop.csvOutputResource[F, A](pathStr, hc, blocker))
        data <- as.chunks
      } yield data.foreach(aos.write)
  }
}
