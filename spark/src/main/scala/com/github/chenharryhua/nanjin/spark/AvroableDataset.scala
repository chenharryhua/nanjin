package com.github.chenharryhua.nanjin.spark

import cats.effect.Concurrent
import com.sksamuel.avro4s._
import fs2.{Pipe, Stream}
import org.apache.spark.sql.SparkSession

private[spark] trait AvroableDataSink extends Serializable {

  private def sink[F[_], A: SchemaFor: Encoder](
    pathStr: String,
    builder: AvroOutputStreamBuilder[A])(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Pipe[F, A, Unit] =
    (sa: Stream[F, A]) =>
      Stream
        .resource(
          hadoop
            .avroOutputResource(pathStr, sparkSession.sparkContext.hadoopConfiguration, builder))
        .flatMap(os => sa.map(a => os.write(a)))

  def avroFileSink[F[_], A: SchemaFor: Encoder](pathStr: String)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.data[A])

  def jacksonFileSink[F[_], A: SchemaFor: Encoder](pathStr: String)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.json[A])
}

private[spark] trait AvroableDataSource extends Serializable {

  private def source[F[_], A: SchemaFor: Decoder](
    pathStr: String,
    builder: AvroInputStreamBuilder[A])(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Stream[F, A] =
    for {
      is <- Stream.resource(
        hadoop.avroInputResource(pathStr, sparkSession.sparkContext.hadoopConfiguration, builder))
      data <- Stream.fromIterator(is.iterator)
    } yield data

  def avroFileSource[F[_], A: SchemaFor: Decoder](pathStr: String)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Stream[F, A] = source[F, A](pathStr, AvroInputStream.data[A])

  def jacksonFileSource[F[_], A: SchemaFor: Decoder](pathStr: String)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Stream[F, A] = source[F, A](pathStr, AvroInputStream.json[A])

}
