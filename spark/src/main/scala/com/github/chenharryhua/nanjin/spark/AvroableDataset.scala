package com.github.chenharryhua.nanjin.spark

import cats.effect.Concurrent
import com.sksamuel.avro4s._
import frameless.TypedDataset
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession

private[spark] trait AvroableDataSink extends Serializable {

  protected def sink[F[_]: Concurrent, A, B: SchemaFor: Encoder](
    pathStr: String,
    builder: AvroOutputStreamBuilder[B],
    f: A => B)(implicit sparkSession: SparkSession): Pipe[F, TypedDataset[A], Unit] = {

    val schema: Schema = SchemaFor[B].schema(DefaultFieldMapper)

    tds =>
      for {
        os <- hadoop
          .outStream(pathStr, sparkSession.sparkContext.hadoopConfiguration)
          .map(builder.to(_).build(schema))
        data <- tds.flatMap(_.stream[F].map(f))
      } yield os.write(data)
  }
}

private[spark] trait AvroableDataSource extends Serializable {

  protected def source[F[_], A: SchemaFor: Decoder](
    pathStr: String,
    builder: AvroInputStreamBuilder[A])(
    implicit sparkSession: SparkSession,
    F: Concurrent[F]): Stream[F, A] = {

    val schema: Schema = SchemaFor[A].schema(DefaultFieldMapper)

    for {
      is <- hadoop
        .inStream(pathStr, sparkSession.sparkContext.hadoopConfiguration)
        .map(builder.from(_).build(schema))
      data <- Stream.fromIterator(is.iterator)
    } yield data
  }
}
