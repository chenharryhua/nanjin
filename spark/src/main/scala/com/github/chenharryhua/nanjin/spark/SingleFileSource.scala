package com.github.chenharryhua.nanjin.spark

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.sksamuel.avro4s.{AvroInputStream, AvroInputStreamBuilder, Decoder, SchemaFor}
import fs2.Stream
import org.apache.spark.sql.SparkSession

private[spark] trait SingleFileSource extends Serializable {

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

  def binaryFileSource[F[_]: Concurrent: ContextShift, A: SchemaFor: Decoder](pathStr: String)(
    implicit
    sparkSession: SparkSession): Stream[F, A] = source[F, A](pathStr, AvroInputStream.binary[A])

}
