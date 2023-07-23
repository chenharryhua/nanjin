package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.Resource
import cats.effect.kernel.Sync
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetReader

import java.io.InputStream

private object NJReader {
  def avro[F[_]](configuration: Configuration, schema: Schema, path: NJPath)(implicit
    F: Sync[F]): Resource[F, DataFileStream[GenericRecord]] =
    for {
      is <- Resource.make(F.blocking(path.hadoopInputFile(configuration).newStream()))(r =>
        F.blocking(r.close()))
      dfs <- Resource.make[F, DataFileStream[GenericRecord]](
        F.blocking(new DataFileStream(is, new GenericDatumReader(schema))))(r => F.blocking(r.close()))
    } yield dfs

  def parquet[F[_]](readBuilder: Reader[NJPath, ParquetReader.Builder[GenericRecord]], path: NJPath)(implicit
    F: Sync[F]): Resource[F, ParquetReader[GenericRecord]] =
    Resource.make(F.blocking(readBuilder.run(path).build()))(r => F.blocking(r.close()))

  def csv[F[_]](configuration: Configuration, path: NJPath)(implicit F: Sync[F]): Resource[F, InputStream] =
    Resource.make(F.blocking(fileInputStream(path, configuration)))(r => F.blocking(r.close()))
}
