package com.github.chenharryhua.nanjin.spark

import cats.effect.Concurrent
import com.sksamuel.avro4s._
import fs2.{Pipe, Stream}
import org.apache.spark.sql.SparkSession

private[spark] trait AvroableDataSink extends Serializable {

  private def sink[F[_], A, B: SchemaFor: Encoder](
    pathStr: String,
    builder: AvroOutputStreamBuilder[B],
    f: A => B)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Pipe[F, A, Unit] =
    (sa: Stream[F, A]) =>
      Stream
        .resource(
          hadoop
            .avroOutputResource(pathStr, sparkSession.sparkContext.hadoopConfiguration, builder))
        .flatMap(os => sa.map(a => os.write(f(a))))

  def avroFileSink[F[_], A, B: SchemaFor: Encoder](pathStr: String)(f: A => B)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.data[B], f)

  def jacksonFileSink[F[_], A, B: SchemaFor: Encoder](pathStr: String)(f: A => B)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.json[B], f)
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
