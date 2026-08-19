package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.Sync
import com.fasterxml.jackson.databind.{JsonNode, ObjectReader}
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

/** A collection of lazy streams for reading typed data from a Hadoop-compatible path.
  *
  * Obtain a source from `Hadoop.source(path)` and compile the stream you need:
  *
  * {{{
  * val records = hadoop.source(path).text(ChunkSize(4096)).compile.toList
  * }}}
  *
  * Opening the file and reading data happen when the stream is run. The underlying reader is closed when the
  * stream finishes. `chunkSize` controls the number of decoded values emitted in each stream chunk and must
  * be positive. `bytes` uses an `Information` value because its chunk size is a byte-buffer size rather than
  * a count of decoded values.
  */
sealed trait FileSource[F[_]] {

  /** Read an Avro data file using its writer schema.
    *
    * Use this as `hadoop.source(path).avro(chunkSize)`. The stream emits generic Avro records in chunks of up
    * to `chunkSize` records.
    *
    * @param chunkSize
    *   Maximum number of records in each emitted chunk. Must be positive.
    * @see
    *   https://avro.apache.org
    */
  def avro(chunkSize: ChunkSize): Stream[F, GenericData.Record]

  /** Read an Avro data file and resolve it with a reader schema.
    *
    * Use this as `hadoop.source(path).avro(chunkSize, readerSchema)` when the application schema differs from
    * the schema stored in the file.
    *
    * @param chunkSize
    *   Maximum number of records in each emitted chunk. Must be positive.
    * @param readerSchema
    *   Schema expected by the application.
    */
  def avro(chunkSize: ChunkSize, readerSchema: Schema): Stream[F, GenericData.Record]

  /** Read a binary Avro file using explicit writer and reader schemas.
    *
    * Use this as `hadoop.source(path).binAvro(chunkSize, writerSchema, readerSchema)`. The writer schema
    * describes the encoded file and the reader schema describes the records returned to the application.
    *
    * @param chunkSize
    *   Maximum number of records in each emitted chunk. Must be positive.
    * @param writerSchema
    *   Schema used to encode the file.
    * @param readerSchema
    *   Schema expected by the application.
    */
  def binAvro(chunkSize: ChunkSize, writerSchema: Schema, readerSchema: Schema): Stream[F, GenericData.Record]

  /** Read a binary Avro file using one schema for both writing and reading.
    *
    * Use this as `hadoop.source(path).binAvro(chunkSize, schema)`.
    *
    * @param chunkSize
    *   Maximum number of records in each emitted chunk. Must be positive.
    * @param schema
    *   Schema used to decode the file.
    */
  def binAvro(chunkSize: ChunkSize, schema: Schema): Stream[F, GenericData.Record]

  /** Read Jackson-encoded Avro records with explicit writer and reader schemas.
    *
    * Use this as `hadoop.source(path).jackson(chunkSize, writerSchema, readerSchema)`.
    *
    * @param chunkSize
    *   Maximum number of records in each emitted chunk. Must be positive.
    * @param writerSchema
    *   Schema used to encode the file.
    * @param readerSchema
    *   Schema expected by the application.
    */
  def jackson(chunkSize: ChunkSize, writerSchema: Schema, readerSchema: Schema): Stream[F, GenericData.Record]

  /** Read Jackson-encoded Avro records using one schema for both sides.
    *
    * Use this as `hadoop.source(path).jackson(chunkSize, schema)`.
    *
    * @param chunkSize
    *   Maximum number of records in each emitted chunk. Must be positive.
    * @param schema
    *   Schema used to decode the file.
    */
  def jackson(chunkSize: ChunkSize, schema: Schema): Stream[F, GenericData.Record]

  /** Read the file as raw bytes.
    *
    * Use this as `hadoop.source(path).bytes(64.bytes)`. The stream emits chunks of approximately `bufferSize`
    * bytes, with a smaller final chunk possible. A buffer smaller than one byte is rejected.
    *
    * @param bufferSize
    *   Size of the byte buffer, expressed with a `squants` information unit.
    */
  def bytes(bufferSize: Information): Stream[F, Byte]

  /** Read newline-delimited JSON values as Circe JSON.
    *
    * Use this as `hadoop.source(path).circe(chunkSize)`.
    *
    * @param chunkSize
    *   Maximum number of JSON values in each emitted chunk. Must be positive.
    * @see
    *   https://github.com/circe/circe
    */
  def circe(chunkSize: ChunkSize): Stream[F, Json]

  /** Read CSV rows using the supplied Kantan CSV configuration.
    *
    * Use this as `hadoop.source(path).kantan(chunkSize, configuration)`. Each emitted value is one row
    * represented as a sequence of column values.
    *
    * @param chunkSize
    *   Maximum number of rows in each emitted chunk. Must be positive.
    * @param csvConfiguration
    *   CSV dialect and parsing configuration.
    * @see
    *   https://nrinaudo.github.io/kantan.csv
    */
  def kantan(chunkSize: ChunkSize, csvConfiguration: CsvConfiguration): Stream[F, Seq[String]]

  /** Read CSV rows using a function that customizes the default RFC configuration.
    *
    * This is equivalent to `kantan(chunkSize, f(CsvConfiguration.rfc))`.
    *
    * @param chunkSize
    *   Maximum number of rows in each emitted chunk. Must be positive.
    * @param f
    *   Function that customizes the default RFC CSV configuration.
    * @see
    *   https://nrinaudo.github.io/kantan.csv
    */
  def kantan(chunkSize: ChunkSize, f: Endo[CsvConfiguration]): Stream[F, Seq[String]]

  /** Read CSV rows using the default RFC configuration.
    *
    * Use this as `hadoop.source(path).kantan(chunkSize)`.
    *
    * @param chunkSize
    *   Maximum number of rows in each emitted chunk. Must be positive.
    */
  def kantan(chunkSize: ChunkSize): Stream[F, Seq[String]]

  /** Read Parquet records with an optional builder customization.
    *
    * Use this as `hadoop.source(path).parquet(chunkSize, identity)` when no customization is needed. The
    * function receives the configured reader builder after the input path, Hadoop configuration, and generic
    * data model have been set.
    *
    * @param chunkSize
    *   Maximum number of records in each emitted chunk. Must be positive.
    * @param f
    *   Function that customizes the configured Parquet reader builder.
    * @see
    *   https://parquet.apache.org
    */
  def parquet(
    chunkSize: ChunkSize,
    f: Endo[ParquetReader.Builder[GenericData.Record]] = identity): Stream[F, GenericData.Record]

  /** Read Parquet records using the default reader configuration.
    *
    * Use this as `hadoop.source(path).parquet(chunkSize)`.
    *
    * @param chunkSize
    *   Maximum number of records in each emitted chunk. Must be positive.
    */
  def parquet(chunkSize: ChunkSize): Stream[F, GenericData.Record]

  /** Read the file as text.
    *
    * Use this as `hadoop.source(path).text(chunkSize)`. Each element represents one line; the final line may
    * be emitted without a trailing line separator.
    *
    * @param chunkSize
    *   Maximum number of lines in each emitted chunk. Must be positive.
    */
  def text(chunkSize: ChunkSize): Stream[F, String]

  /** Read length-delimited Protocol Buffer messages.
    *
    * Use this as `hadoop.source(path).protobuf[MessageType](chunkSize)`. The message companion supplies the
    * decoder for the requested type.
    *
    * Keep serialized messages below 2 GiB, the maximum supported by all implementations.
    *
    * @see
    *   https://protobuf.dev/programming-guides/proto-limits/#total
    * @param chunkSize
    *   Maximum number of messages in each emitted chunk. Must be positive.
    */
  def protobuf[A <: GeneratedMessage: GeneratedMessageCompanion](chunkSize: ChunkSize): Stream[F, A]

  /** Read JSON tree values with a Jackson `ObjectReader`.
    *
    * Use this as `hadoop.source(path).jsonNode(chunkSize, objectReader)`. Input is read as text and an empty
    * line terminates the stream; each non-empty line is parsed into one `JsonNode`.
    *
    * @param chunkSize
    *   Maximum number of JSON values in each emitted chunk. Must be positive.
    * @param or
    *   Jackson reader used to parse each non-empty line.
    * @see
    *   https://github.com/FasterXML/jackson-databind
    */
  def jsonNode(chunkSize: ChunkSize, or: ObjectReader): Stream[F, JsonNode]
}

final private class FileSourceImpl[F[_]: Sync](configuration: Configuration, url: Url) extends FileSource[F] {

  override def avro(chunkSize: ChunkSize): Stream[F, GenericData.Record] =
    HadoopReader.avroS[F](configuration, url, chunkSize, None)
  override def avro(chunkSize: ChunkSize, readerSchema: Schema): Stream[F, GenericData.Record] =
    HadoopReader.avroS[F](configuration, url, chunkSize, Some(readerSchema))

  override def binAvro(
    chunkSize: ChunkSize,
    writerSchema: Schema,
    readerSchema: Schema): Stream[F, GenericData.Record] =
    HadoopReader.binAvroS[F](configuration, writerSchema, readerSchema, url, chunkSize)

  override def binAvro(chunkSize: ChunkSize, schema: Schema): Stream[F, GenericData.Record] =
    binAvro(chunkSize, schema, schema)

  override def bytes(bufferSize: Information): Stream[F, Byte] = {
    val size = bufferSize.toBytes.toInt
    require(size >= 1, s"bufferSize must be at least 1 byte, but was $size")
    HadoopReader.byteS[F](configuration, url, bufferSize)
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
    HadoopReader.parquetS[F](
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

  override def protobuf[A <: GeneratedMessage: GeneratedMessageCompanion](
    chunkSize: ChunkSize): Stream[F, A] =
    HadoopReader.protobufS[F, A](configuration, url, chunkSize)

  override def jsonNode(chunkSize: ChunkSize, or: ObjectReader): Stream[F, JsonNode] =
    text(chunkSize).filter(_.nonEmpty).map(or.readTree)
}
