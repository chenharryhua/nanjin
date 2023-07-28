package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.Resource
import cats.effect.kernel.Sync
import kantan.csv.ops.toCsvInputOps
import kantan.csv.{CsvConfiguration, CsvReader, HeaderDecoder, ReadResult}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.io.InputStream

private object HadoopReader {

  private def fileInputStream(path: Path, configuration: Configuration): InputStream = {
    val is: InputStream = HadoopInputFile.fromPath(path, configuration).newStream()
    Option(new CompressionCodecFactory(configuration).getCodec(path)) match {
      case Some(cc) => cc.createInputStream(is)
      case None     => is
    }
  }

  def avro[F[_]](configuration: Configuration, schema: Schema, path: Path)(implicit
    F: Sync[F]): Resource[F, DataFileStream[GenericRecord]] =
    for {
      is <- Resource.make(F.blocking(fileInputStream(path, configuration)))(r => F.blocking(r.close()))
      dfs <- Resource.make[F, DataFileStream[GenericRecord]](
        F.blocking(new DataFileStream(is, new GenericDatumReader(schema))))(r => F.blocking(r.close()))
    } yield dfs

  def parquet[F[_]](readBuilder: Reader[Path, ParquetReader.Builder[GenericRecord]], path: Path)(implicit
    F: Sync[F]): Resource[F, ParquetReader[GenericRecord]] =
    Resource.make(F.blocking(readBuilder.run(path).build()))(r => F.blocking(r.close()))

  def kantan[F[_], A: HeaderDecoder](
    configuration: Configuration,
    csvConfiguration: CsvConfiguration,
    path: Path)(implicit F: Sync[F]): Resource[F, CsvReader[ReadResult[A]]] =
    Resource.make(F.blocking(fileInputStream(path, configuration).asCsvReader[A](csvConfiguration)))(r =>
      F.blocking(r.close()))

  def inputStream[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Resource[F, InputStream] =
    Resource.make(F.blocking(fileInputStream(path, configuration)))(r => F.blocking(r.close()))
}
