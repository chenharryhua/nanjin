package com.github.chenharryhua.nanjin.spark

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.sksamuel.avro4s._
import fs2.{Pipe, Stream}
import org.apache.spark.sql.SparkSession

private[spark] trait AvroableDataSink extends Serializable {

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
      data <- sa.chunkN(4096)
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
}

private[spark] trait AvroableDataSource extends Serializable {

  private def source[F[_]: Concurrent: ContextShift, A: SchemaFor: Decoder](
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

  def avroFileSource[F[_]: Concurrent: ContextShift, A: SchemaFor: Decoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Stream[F, A] = source[F, A](pathStr, AvroInputStream.data[A])

  def jacksonFileSource[F[_]: Concurrent: ContextShift, A: SchemaFor: Decoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Stream[F, A] = source[F, A](pathStr, AvroInputStream.json[A])

}
