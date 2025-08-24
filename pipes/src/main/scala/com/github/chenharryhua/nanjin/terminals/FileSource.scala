package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.Stream
import io.circe.Json
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import squants.information.Information

final class FileSource[F[_]: Sync] private (configuration: Configuration, path: Path) {

  /** [[https://avro.apache.org]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def avro(chunkSize: ChunkSize): Stream[F, GenericData.Record] =
    HadoopReader.avroS(configuration, path, chunkSize)

  /** [[https://avro.apache.org]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def binAvro(chunkSize: ChunkSize, schema: Schema): Stream[F, GenericData.Record] =
    HadoopReader.binAvroS[F](configuration, schema, path, chunkSize)

  /** @param bufferSize
    *   in terms of bytes, bits, kilobytes, megabytes, etc. Each chunk of the stream is of uniform size,
    *   except for the final chunk, which may be smaller depending on the remaining data.
    */
  def bytes(bufferSize: Information): Stream[F, Byte] = {
    require(bufferSize.toBytes > 0, s"bufferSize(${bufferSize.toString()}) should be bigger than zero")
    HadoopReader.byteS(configuration, path, bufferSize)
  }

  /** [[https://github.com/circe/circe]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def circe(chunkSize: ChunkSize): Stream[F, Json] =
    HadoopReader.jawnS[F](configuration, path, chunkSize)

  /** [[https://github.com/FasterXML/jackson]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    * @param schema
    *   the schema of the file
    */
  def jackson(chunkSize: ChunkSize, schema: Schema): Stream[F, GenericData.Record] =
    HadoopReader.jacksonS[F](configuration, schema, path, chunkSize)

  /** [[https://nrinaudo.github.io/kantan.csv]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    * @param csvConfiguration
    *   kantan's CSV configuration
    */
  def kantan(chunkSize: ChunkSize, csvConfiguration: CsvConfiguration): Stream[F, Seq[String]] =
    HadoopReader.kantanS[F](configuration, path, chunkSize, csvConfiguration)

  /** [[https://nrinaudo.github.io/kantan.csv]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def kantan(chunkSize: ChunkSize, f: Endo[CsvConfiguration]): Stream[F, Seq[String]] =
    kantan(chunkSize, f(CsvConfiguration.rfc))

  /** [[https://nrinaudo.github.io/kantan.csv]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def kantan(chunkSize: ChunkSize): Stream[F, Seq[String]] =
    kantan(chunkSize, CsvConfiguration.rfc)

  /** [[https://parquet.apache.org]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def parquet(
    chunkSize: ChunkSize,
    f: Endo[ParquetReader.Builder[GenericData.Record]] = identity): Stream[F, GenericData.Record] =
    HadoopReader.parquetS(
      Reader((path: Path) =>
        AvroParquetReader
          .builder[GenericData.Record](HadoopInputFile.fromPath(path, configuration))
          .withDataModel(GenericData.get())
          .withConf(configuration)).map(f),
      path,
      chunkSize
    )

  /** [[https://parquet.apache.org]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def parquet(chunkSize: ChunkSize): Stream[F, GenericData.Record] =
    parquet(chunkSize, identity)

  /** @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def text(chunkSize: ChunkSize): Stream[F, String] =
    HadoopReader.stringS[F](configuration, path, chunkSize)

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * [[https://protobuf.dev/programming-guides/proto-limits/#total]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def protobuf[A <: GeneratedMessage](chunkSize: ChunkSize)(implicit
    gmc: GeneratedMessageCompanion[A]): Stream[F, A] =
    HadoopReader.protobufS[F, A](configuration, path, chunkSize)
}

private object FileSource {
  def apply[F[_]: Sync](configuration: Configuration, path: Url): FileSource[F] =
    new FileSource[F](configuration, toHadoopPath(path))
}
