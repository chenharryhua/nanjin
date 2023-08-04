package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.Resource
import cats.effect.kernel.Sync
import fs2.Stream
import fs2.io.readInputStream
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
import squants.information.Information

import java.io.InputStream

private object HadoopReader {

  def avroR[F[_]](configuration: Configuration, schema: Schema, path: Path)(implicit
    F: Sync[F]): Resource[F, DataFileStream[GenericRecord]] =
    for {
      is <- Resource.make(F.blocking(fileInputStream(path, configuration)))(r => F.blocking(r.close()))
      dfs <- Resource.make[F, DataFileStream[GenericRecord]](
        F.blocking(new DataFileStream(is, new GenericDatumReader(schema))))(r => F.blocking(r.close()))
    } yield dfs

  def parquetR[F[_]](readBuilder: Reader[Path, ParquetReader.Builder[GenericRecord]], path: Path)(implicit
    F: Sync[F]): Resource[F, ParquetReader[GenericRecord]] =
    Resource.make(F.blocking(readBuilder.run(path).build()))(r => F.blocking(r.close()))

  private def fileInputStream(path: Path, configuration: Configuration): InputStream = {
    val is: InputStream = HadoopInputFile.fromPath(path, configuration).newStream()
    Option(new CompressionCodecFactory(configuration).getCodec(path)) match {
      case Some(cc) => cc.createInputStream(is)
      case None     => is
    }
  }

  def inputStreamS[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Stream[F, InputStream] =
    Stream.bracket(F.blocking(fileInputStream(path, configuration)))(r => F.blocking(r.close()))

  def byteS[F[_]](configuration: Configuration, bufferSize: Information, path: Path)(implicit
    F: Sync[F]): Stream[F, Byte] =
    inputStreamS[F](configuration, path).flatMap(is =>
      readInputStream[F](F.pure(is), bufferSize.toBytes.toInt, closeAfterUse = true))

  def kantanS[F[_], A: HeaderDecoder](
    configuration: Configuration,
    csvConfiguration: CsvConfiguration,
    path: Path)(implicit F: Sync[F]): Stream[F, CsvReader[ReadResult[A]]] =
    inputStreamS[F](configuration, path).flatMap(is =>
      Stream.bracket(F.blocking(is.asCsvReader[A](csvConfiguration)))(r => F.blocking(r.close)))

}
