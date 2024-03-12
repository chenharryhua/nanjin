package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.Resource
import cats.effect.kernel.Sync
import fs2.Stream
import fs2.io.readInputStream
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, JsonDecoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import squants.information.Information

import java.io.InputStream
import scala.util.{Failure, Success, Try}

private object HadoopReader {

  def avroR[F[_]](configuration: Configuration, schema: Schema, path: Path)(implicit
    F: Sync[F]): Resource[F, DataFileStream[GenericData.Record]] =
    for {
      is <- Resource.make(F.blocking(HadoopInputFile.fromPath(path, configuration).newStream()))(r =>
        F.blocking(r.close()))
      dfs <- Resource.make[F, DataFileStream[GenericData.Record]](
        F.blocking(new DataFileStream(is, new GenericDatumReader(schema))))(r => F.blocking(r.close()))
    } yield dfs

  def parquetR[F[_]](readBuilder: Reader[Path, ParquetReader.Builder[GenericData.Record]], path: Path)(
    implicit F: Sync[F]): Resource[F, ParquetReader[GenericData.Record]] =
    Resource.make(F.blocking(readBuilder.run(path).build()))(r => F.blocking(r.close()))

  private def fileInputStream(path: Path, configuration: Configuration): InputStream = {
    val is: InputStream = HadoopInputFile.fromPath(path, configuration).newStream()
    Option(new CompressionCodecFactory(configuration).getCodec(path)) match {
      case Some(cc) => cc.createInputStream(is)
      case None     => is
    }
  }

  def inputStreamR[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Resource[F, InputStream] =
    Resource.make(F.blocking(fileInputStream(path, configuration)))(r => F.blocking(r.close()))

  def inputStreamS[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Stream[F, InputStream] = Stream.resource(inputStreamR(configuration, path))

  def byteS[F[_]](configuration: Configuration, bufferSize: Information, path: Path)(implicit
    F: Sync[F]): Stream[F, Byte] =
    inputStreamS[F](configuration, path).flatMap(is =>
      readInputStream[F](F.pure(is), bufferSize.toBytes.toInt, closeAfterUse = true))

  def jacksonS[F[_]](configuration: Configuration, schema: Schema, path: Path)(implicit
    F: Sync[F]): Stream[F, GenericData.Record] =
    inputStreamS[F](configuration, path).flatMap { is =>
      val jsonDecoder: JsonDecoder = DecoderFactory.get().jsonDecoder(schema, is)
      val datumReader: GenericDatumReader[GenericData.Record] =
        new GenericDatumReader[GenericData.Record](schema)

      def next: Option[GenericData.Record] = Try(datumReader.read(null, jsonDecoder)) match {
        case Failure(exception) =>
          exception match {
            case _: java.io.EOFException => None
            case err                     => throw new Exception(path.getName, err)
          }
        case Success(value) => Some(value)
      }

      Stream.unfold(next)(s => s.map(gr => (gr, next)))
    }

  def binAvroS[F[_]](configuration: Configuration, schema: Schema, path: Path)(implicit
    F: Sync[F]): Stream[F, GenericData.Record] =
    inputStreamS[F](configuration, path).flatMap { is =>
      val binDecoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(is, null)
      val datumReader: GenericDatumReader[GenericData.Record] =
        new GenericDatumReader[GenericData.Record](schema)

      def next: Option[GenericData.Record] = Try(datumReader.read(null, binDecoder)) match {
        case Failure(exception) =>
          exception match {
            case _: java.io.EOFException => None
            case err                     => throw new Exception(path.getName, err)
          }
        case Success(value) => Some(value)
      }

      Stream.unfold(next)(s => s.map(gr => (gr, next)))
    }
}
