package com.github.chenharryhua.nanjin.spark

import cats.effect.{Concurrent, Resource}
import com.sksamuel.avro4s._
import frameless.TypedDataset
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession

private[spark] trait AvroableDataSink extends Serializable {

  protected def sink[F[_], A, B: SchemaFor: Encoder](
    pathStr: String,
    builder: AvroOutputStreamBuilder[B],
    f: A => B)(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Pipe[F, TypedDataset[A], Unit] = {

    val schema: Schema = SchemaFor[B].schema(DefaultFieldMapper)

    val aos = hadoop
      .outResource(pathStr, sparkSession.sparkContext.hadoopConfiguration)
      .flatMap(os => Resource.make(F.delay(builder.to(os).build(schema)))(a => F.delay(a.close())))

    tds =>
      for {
        os <- Stream.resource(aos)
        data <- tds.flatMap(_.stream[F].map(f))
      } yield os.write(data)
  }
}

private[spark] trait AvroableDataSource extends Serializable {

  protected def source[F[_], A: SchemaFor: Decoder](
    pathStr: String,
    builder: AvroInputStreamBuilder[A])(
    implicit
    sparkSession: SparkSession,
    F: Concurrent[F]): Stream[F, A] = {

    val schema: Schema = SchemaFor[A].schema(DefaultFieldMapper)

    val ais = hadoop
      .inResource(pathStr, sparkSession.sparkContext.hadoopConfiguration)
      .flatMap(is =>
        Resource.make(F.delay(builder.from(is).build(schema)))(a => F.delay(a.close())))

    for {
      is <- Stream.resource(ais)
      data <- Stream.fromIterator(is.iterator)
    } yield data
  }
}
