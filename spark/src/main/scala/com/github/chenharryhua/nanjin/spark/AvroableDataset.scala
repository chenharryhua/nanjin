package com.github.chenharryhua.nanjin.spark

import cats.effect.{Concurrent, Resource}
import com.sksamuel.avro4s._
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession

private[spark] trait AvroableDataSink extends Serializable {

  private def sink[F[_], A, B: SchemaFor: Encoder](
    pathStr: String,
    builder: AvroOutputStreamBuilder[B],
    f: A => B)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Pipe[F, A, Unit] = {

    val schema: Schema = SchemaFor[B].schema(DefaultFieldMapper)

    val aos: Resource[F, AvroOutputStream[B]] = hadoop
      .outResource(pathStr, sparkSession.sparkContext.hadoopConfiguration)
      .flatMap(os => Resource.make(F.delay(builder.to(os).build(schema)))(a => F.delay(a.close())))

    (sa: Stream[F, A]) => Stream.resource(aos).flatMap(os => sa.map(a => os.write(f(a))))
  }

  def avroSink[F[_], A, B: SchemaFor: Encoder](pathStr: String)(f: A => B)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Pipe[F, A, Unit] =
    sink(pathStr, AvroOutputStream.data[B], f)

  def jacksonSink[F[_], A, B: SchemaFor: Encoder](pathStr: String)(f: A => B)(
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
    F: Concurrent[F]): Stream[F, A] = {

    val schema: Schema = SchemaFor[A].schema(DefaultFieldMapper)

    val ais: Resource[F, AvroInputStream[A]] = hadoop
      .inResource(pathStr, sparkSession.sparkContext.hadoopConfiguration)
      .flatMap(is =>
        Resource.make(F.delay(builder.from(is).build(schema)))(a => F.delay(a.close())))

    for {
      is <- Stream.resource(ais)
      data <- Stream.fromIterator(is.iterator)
    } yield data
  }

  def avroSource[F[_], A: SchemaFor: Decoder](pathStr: String)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Stream[F, A] = source[F, A](pathStr, AvroInputStream.data[A])

  def jacksonSource[F[_], A: SchemaFor: Decoder](pathStr: String)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Stream[F, A] = source[F, A](pathStr, AvroInputStream.json[A])

}
