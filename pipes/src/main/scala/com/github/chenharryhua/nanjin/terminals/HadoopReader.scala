package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.Resource
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.Stream
import fs2.io.readInputStream
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.{Decoder, DecoderFactory}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.SeekableInputStream
import squants.information.Information

import java.io.InputStream
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.IteratorHasAsScala

private object HadoopReader {

  def avroR[F[_]](configuration: Configuration, schema: Schema, path: Path)(implicit
    F: Sync[F]): Resource[F, DataFileStream[GenericData.Record]] =
    for {
      is <- Resource.make(F.blocking(HadoopInputFile.fromPath(path, configuration).newStream()))(r =>
        F.blocking(r.close()))
      dfs <- Resource.make[F, DataFileStream[GenericData.Record]] {
        F.blocking[DataFileStream[GenericData.Record]](new DataFileStream(is, new GenericDatumReader(schema)))
      }(r => F.blocking(r.close()))
    } yield dfs

  def parquetR[F[_]](readBuilder: Reader[Path, ParquetReader.Builder[GenericData.Record]], path: Path)(
    implicit F: Sync[F]): Resource[F, ParquetReader[GenericData.Record]] =
    Resource.make {
      F.blocking[ParquetReader[GenericData.Record]](readBuilder.run(path).build())
    }(r => F.blocking(r.close()))

  private def fileInputStream(path: Path, configuration: Configuration): InputStream = {
    val is: SeekableInputStream = HadoopInputFile.fromPath(path, configuration).newStream()
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
    readInputStream[F](
      fis = F.blocking(fileInputStream(path, configuration)),
      chunkSize = bufferSize.toBytes.toInt,
      closeAfterUse = true)

  def stringS[F[_]](configuration: Configuration, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, String] =
    inputStreamS[F](configuration, path).flatMap { is =>
      val iterator: Iterator[String] = IOUtils.lineIterator(is, StandardCharsets.UTF_8).asScala
      Stream.fromBlockingIterator(iterator, chunkSize.value)
    }

  private def genericRecordReaderS[F[_]](
    getDecoder: InputStream => Decoder,
    configuration: Configuration,
    schema: Schema,
    path: Path,
    chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    inputStreamS[F](configuration, path).flatMap { is =>
      val decoder: Decoder = getDecoder(is)
      val datumReader: GenericDatumReader[GenericData.Record] =
        new GenericDatumReader[GenericData.Record](schema)

      val iterator: Iterator[GenericData.Record] =
        Iterator.continually {
          try {
            val gr = datumReader.read(null, decoder)
            Some(gr)
          } catch {
            case _: java.io.EOFException => None
          }
        }.takeWhile(_.nonEmpty).map(_.get)

      Stream.fromBlockingIterator[F](iterator, chunkSize.value)
    }

  def jacksonS[F[_]](configuration: Configuration, schema: Schema, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, GenericData.Record] =
    genericRecordReaderS[F](
      getDecoder = is => DecoderFactory.get.jsonDecoder(schema, is),
      configuration = configuration,
      schema = schema,
      path = path,
      chunkSize = chunkSize)

  def binAvroS[F[_]](configuration: Configuration, schema: Schema, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, GenericData.Record] =
    genericRecordReaderS[F](
      getDecoder = is => DecoderFactory.get.binaryDecoder(is, null),
      configuration = configuration,
      schema = schema,
      path = path,
      chunkSize = chunkSize)
}
