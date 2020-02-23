package com.github.chenharryhua.nanjin.spark

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.sksamuel.avro4s._
import fs2.{Pipe, Stream}
import org.apache.spark.sql.SparkSession

private[spark] trait SingleFileSink extends Serializable {

  private def sink[F[_]: ContextShift: Concurrent, A: SchemaFor: Encoder](
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

  def avroFileSink[F[_]: ContextShift: Concurrent, A: SchemaFor: Encoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.data[A])

  def jacksonFileSink[F[_]: ContextShift: Concurrent, A: SchemaFor: Encoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.json[A])

  def binaryFileSink[F[_]: ContextShift: Concurrent, A: SchemaFor: Encoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.binary[A])
}
