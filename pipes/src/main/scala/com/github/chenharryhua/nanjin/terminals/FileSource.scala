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

sealed trait FileSource[F[_]] {

  /** [[https://avro.apache.org]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def avro(chunkSize: ChunkSize): Stream[F, GenericData.Record]

  /** [[https://avro.apache.org]]
    *
    * re-interpret the record according to the reader schema
    *
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    * @param readerSchema
    *   expected schema
    */
  def avro(chunkSize: ChunkSize, readerSchema: Schema): Stream[F, GenericData.Record]

  /** [[https://avro.apache.org]]
    *
    * re-interpret the record according to the reader schema
    *
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    * @param writerSchema
    *   the one the record was originally written with
    * @param readerSchema
    *   the new (upgraded) schema
    */
  def binAvro(chunkSize: ChunkSize, writerSchema: Schema, readerSchema: Schema): Stream[F, GenericData.Record]

  /** [[https://avro.apache.org]]
    *
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    * @param schema
    *   the schema of the file
    */
  def binAvro(chunkSize: ChunkSize, schema: Schema): Stream[F, GenericData.Record]

  /** [[https://github.com/FasterXML/jackson]]
    *
    * re-interpret the record according to the reader schema
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    * @param writerSchema
    *   the one the record was originally written with
    * @param readerSchema
    *   the new (upgraded) schema
    */
  def jackson(chunkSize: ChunkSize, writerSchema: Schema, readerSchema: Schema): Stream[F, GenericData.Record]

  /** [[https://github.com/FasterXML/jackson]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    * @param schema
    *   the schema of the file
    */
  def jackson(chunkSize: ChunkSize, schema: Schema): Stream[F, GenericData.Record]

  /** @param bufferSize
    *   in terms of bytes, bits, kilobytes, megabytes, etc. Each chunk of the stream is of uniform size,
    *   except for the final chunk, which may be smaller depending on the remaining data.
    */
  def bytes(bufferSize: Information): Stream[F, Byte]

  /** [[https://github.com/circe/circe]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def circe(chunkSize: ChunkSize): Stream[F, Json]

  /** [[https://nrinaudo.github.io/kantan.csv]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    * @param csvConfiguration
    *   kantan's CSV configuration
    */
  def kantan(chunkSize: ChunkSize, csvConfiguration: CsvConfiguration): Stream[F, Seq[String]]

  /** [[https://nrinaudo.github.io/kantan.csv]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def kantan(chunkSize: ChunkSize, f: Endo[CsvConfiguration]): Stream[F, Seq[String]]

  /** [[https://nrinaudo.github.io/kantan.csv]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def kantan(chunkSize: ChunkSize): Stream[F, Seq[String]]

  /** [[https://parquet.apache.org]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def parquet(
    chunkSize: ChunkSize,
    f: Endo[ParquetReader.Builder[GenericData.Record]] = identity): Stream[F, GenericData.Record]

  /** [[https://parquet.apache.org]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def parquet(chunkSize: ChunkSize): Stream[F, GenericData.Record]

  /** @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def text(chunkSize: ChunkSize): Stream[F, String]

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * [[https://protobuf.dev/programming-guides/proto-limits/#total]]
    * @param chunkSize
    *   Each chunk of the stream is of uniform size, except for the final chunk, which may be smaller
    *   depending on the remaining data.
    */
  def protobuf[A <: GeneratedMessage](chunkSize: ChunkSize)(implicit
    gmc: GeneratedMessageCompanion[A]): Stream[F, A]
}

final private class FileSourceImpl[F[_]: Sync](configuration: Configuration, url: Url) extends FileSource[F] {

  override def avro(chunkSize: ChunkSize): Stream[F, GenericData.Record] =
    HadoopReader.avroS(configuration, url, chunkSize, None)
  override def avro(chunkSize: ChunkSize, readerSchema: Schema): Stream[F, GenericData.Record] =
    HadoopReader.avroS(configuration, url, chunkSize, Some(readerSchema))

  override def binAvro(
    chunkSize: ChunkSize,
    writerSchema: Schema,
    readerSchema: Schema): Stream[F, GenericData.Record] =
    HadoopReader.binAvroS[F](configuration, writerSchema, readerSchema, url, chunkSize)

  override def binAvro(chunkSize: ChunkSize, schema: Schema): Stream[F, GenericData.Record] =
    binAvro(chunkSize, schema, schema)

  override def bytes(bufferSize: Information): Stream[F, Byte] = {
    require(bufferSize.toBytes > 0, s"bufferSize(${bufferSize.toString()}) should be bigger than zero")
    HadoopReader.byteS(configuration, url, bufferSize)
  }

  override def circe(chunkSize: ChunkSize): Stream[F, Json] =
    HadoopReader.jawnS[F](configuration, url, chunkSize)

  override def jackson(
    chunkSize: ChunkSize,
    writerSchema: Schema,
    readerSchema: Schema): Stream[F, GenericData.Record] =
    HadoopReader.jacksonS[F](configuration, writerSchema, readerSchema, url, chunkSize)

  override def jackson(chunkSize: ChunkSize, schema: Schema): Stream[F, GenericData.Record] =
    jackson(chunkSize, schema, schema)

  override def kantan(chunkSize: ChunkSize, csvConfiguration: CsvConfiguration): Stream[F, Seq[String]] =
    HadoopReader.kantanS[F](configuration, url, chunkSize, csvConfiguration)

  override def kantan(chunkSize: ChunkSize, f: Endo[CsvConfiguration]): Stream[F, Seq[String]] =
    kantan(chunkSize, f(CsvConfiguration.rfc))

  override def kantan(chunkSize: ChunkSize): Stream[F, Seq[String]] =
    kantan(chunkSize, CsvConfiguration.rfc)

  override def parquet(
    chunkSize: ChunkSize,
    f: Endo[ParquetReader.Builder[GenericData.Record]] = identity): Stream[F, GenericData.Record] =
    HadoopReader.parquetS(
      Reader((path: Path) =>
        AvroParquetReader
          .builder[GenericData.Record](HadoopInputFile.fromPath(path, configuration))
          .withDataModel(GenericData.get())
          .withConf(configuration)).map(f),
      url,
      chunkSize
    )

  override def parquet(chunkSize: ChunkSize): Stream[F, GenericData.Record] =
    parquet(chunkSize, identity)

  override def text(chunkSize: ChunkSize): Stream[F, String] =
    HadoopReader.stringS[F](configuration, url, chunkSize)

  override def protobuf[A <: GeneratedMessage](chunkSize: ChunkSize)(implicit
    gmc: GeneratedMessageCompanion[A]): Stream[F, A] =
    HadoopReader.protobufS[F, A](configuration, url, chunkSize)

}
