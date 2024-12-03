package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Sync}
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.Stream
import io.circe.Json
import io.lemonlabs.uri.Url
import kantan.csv.engine.ReaderEngine
import kantan.csv.{CsvConfiguration, CsvReader, ReadResult}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import squants.information.{Bytes, Information}

import java.io.InputStreamReader

final class FileSource[F[_]: Sync] private (configuration: Configuration, path: Url) {
  def avro(chunkSize: ChunkSize): Stream[F, GenericData.Record] =
    HadoopReader.avroS(configuration, toHadoopPath(path), chunkSize)

  def binAvro(chunkSize: ChunkSize, schema: Schema): Stream[F, GenericData.Record] =
    HadoopReader.binAvroS[F](configuration, schema, toHadoopPath(path), chunkSize)

  def bytes(bufferSize: Information): Stream[F, Byte] =
    HadoopReader.byteS(configuration, toHadoopPath(path), ChunkSize(bufferSize))

  val bytes: Stream[F, Byte] = bytes(Bytes(1024 * 512))

  def circe(bufferSize: Information): Stream[F, Json] =
    HadoopReader.jawnS[F](configuration, toHadoopPath(path), bufferSize)

  val circe: Stream[F, Json] = circe(Bytes(1024 * 512))

  def jackson(chunkSize: ChunkSize, schema: Schema)(implicit F: Async[F]): Stream[F, GenericData.Record] =
    HadoopReader.jacksonS[F](configuration, schema, toHadoopPath(path), chunkSize)

  def kantan(chunkSize: ChunkSize, csvConfiguration: CsvConfiguration): Stream[F, Seq[String]] =
    HadoopReader.inputStreamS[F](configuration, toHadoopPath(path)).flatMap { is =>
      val reader: CsvReader[ReadResult[Seq[String]]] = {
        val cr = ReaderEngine.internalCsvReaderEngine.readerFor(new InputStreamReader(is), csvConfiguration)
        if (csvConfiguration.hasHeader) cr.drop(1) else cr
      }

      Stream.fromBlockingIterator[F](reader.iterator, chunkSize.value).rethrow
    }

  def kantan(chunkSize: ChunkSize, f: Endo[CsvConfiguration]): Stream[F, Seq[String]] =
    kantan(chunkSize, f(CsvConfiguration.rfc))

  def kantan(chunkSize: ChunkSize): Stream[F, Seq[String]] =
    kantan(chunkSize, CsvConfiguration.rfc)

  def parquet(
    chunkSize: ChunkSize,
    f: Endo[ParquetReader.Builder[GenericData.Record]] = identity): Stream[F, GenericData.Record] =
    HadoopReader.parquetS(
      Reader((path: Path) =>
        AvroParquetReader
          .builder[GenericData.Record](HadoopInputFile.fromPath(path, configuration))
          .withDataModel(GenericData.get())
          .withConf(configuration)).map(f),
      toHadoopPath(path),
      chunkSize
    )

  def text(chunkSize: ChunkSize): Stream[F, String] =
    HadoopReader.stringS[F](configuration, toHadoopPath(path), chunkSize)

}

object FileSource {
  def apply[F[_]: Sync](configuration: Configuration, path: Url): FileSource[F] =
    new FileSource[F](configuration, path)
}
