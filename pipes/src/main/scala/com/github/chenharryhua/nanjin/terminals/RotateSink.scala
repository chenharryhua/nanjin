package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import com.fasterxml.jackson.databind.{JsonNode, ObjectWriter}
import fs2.Pipe
import io.circe.Json
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetWriter.Builder
import scalapb.GeneratedMessage

/** Format-specific rotating sinks that write successive files from an `fs2.Stream`.
  *
  * Create a sink with `Hadoop.rotateSink`, choose a format, and run it as a normal pipe. A size-based sink
  * rotates after a configured number of input values; a policy-based sink rotates according to a time policy.
  * The supplied path builder receives a `CreateRotateFile` for each file and must return its output URL.
  *
  * Each completed rotation emits one `RotateFile`, including the output URL and the number of records
  * written. For example:
  *
  * {{{
  * val completed =
  *   input.through(hadoop.rotateSink(zoneId, 1000)(pathBuilder).text).compile.toList
  * }}}
  *
  * Rotation resources are opened and closed by the stream. The final partially filled file is emitted when
  * the input stream completes.
  */
sealed trait RotateSink[F[_]] {
  protected type Sink[A] = Pipe[F, A, RotateFile]

  /** Write generic Avro records to rotating Avro data files.
    *
    * Use this as `input.through(rotating.avro(codec))`. The default overload writes uncompressed Avro.
    *
    * @param compression
    *   Avro compression configuration.
    * @see
    *   https://avro.apache.org
    */
  def avro(compression: AvroCompression): Pipe[F, GenericRecord, RotateFile]

  /** Select Avro compression settings before creating the rotating sink.
    *
    * Equivalent to `avro(f(AvroCompression))`.
    */
  final def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, RotateFile] =
    avro(f(AvroCompression))

  /** Write uncompressed Avro records to rotating files.
    *
    * Use this as `input.through(rotating.avro)`.
    */
  final def avro: Pipe[F, GenericRecord, RotateFile] =
    avro(AvroCompression.Uncompressed)

  /** Write generic records to rotating binary Avro files.
    *
    * Use this as `input.through(rotating.binAvro)`.
    *
    * @see
    *   https://avro.apache.org
    */
  def binAvro: Pipe[F, GenericRecord, RotateFile]

  /** Write generic records to rotating Jackson JSON files.
    *
    * Use this as `input.through(rotating.jackson)`.
    *
    * @see
    *   https://github.com/FasterXML/jackson
    */
  def jackson: Pipe[F, GenericRecord, RotateFile]

  /** Write generic records to rotating Parquet files with a builder customization.
    *
    * Use this as `input.through(rotating.parquet(customize))`. The function customizes the configured Parquet
    * builder for each newly rotated file.
    *
    * @param f
    *   Function that customizes the Parquet writer builder.
    * @see
    *   https://parquet.apache.org
    */
  def parquet(f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, RotateFile]

  /** Write generic records to rotating Parquet files with default settings.
    *
    * Use this as `input.through(rotating.parquet)`.
    *
    * @see
    *   https://parquet.apache.org
    */
  final def parquet: Pipe[F, GenericRecord, RotateFile] =
    parquet(identity[Builder[GenericRecord]])

  /** Write rows to rotating CSV files using the supplied configuration.
    *
    * Use this as `input.through(rotating.kantan(configuration))`.
    *
    * @param csvConfiguration
    *   CSV dialect and formatting configuration.
    * @see
    *   https://nrinaudo.github.io/kantan.csv
    */
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], RotateFile]

  /** Customize the default RFC CSV configuration for a rotating sink.
    *
    * Equivalent to `kantan(f(CsvConfiguration.rfc))`.
    *
    * @param f
    *   Function that customizes the default CSV configuration.
    * @see
    *   https://nrinaudo.github.io/kantan.csv
    */
  final def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], RotateFile] =
    kantan(f(CsvConfiguration.rfc))

  /** Write rows to rotating CSV files using the default RFC configuration.
    *
    * Use this as `input.through(rotating.kantan)`.
    *
    * @see
    *   https://nrinaudo.github.io/kantan.csv
    */
  final def kantan: Pipe[F, Seq[String], RotateFile] =
    kantan(CsvConfiguration.rfc)

  /** Write bytes to rotating binary files.
    *
    * Use this as `input.through(rotating.bytes)`.
    */
  def bytes: Pipe[F, Byte, RotateFile]

  /** Write Circe JSON values to rotating JSON files.
    *
    * Use this as `input.through(rotating.circe)`.
    *
    * @see
    *   https://github.com/circe/circe
    */
  def circe: Pipe[F, Json, RotateFile]

  /** Write text values to rotating text files.
    *
    * Use this as `input.through(rotating.text)`.
    */
  def text: Pipe[F, String, RotateFile]

  /** Write length-delimited Protocol Buffer messages to rotating files.
    *
    * Use this as `input.through(rotating.protobuf)`. Keep serialized messages below 2 GiB, the maximum
    * supported by all implementations.
    *
    * @see
    *   https://protobuf.dev/programming-guides/proto-limits/#total
    */
  def protobuf: Pipe[F, GeneratedMessage, RotateFile]

  /** Write Jackson JSON trees to rotating text files.
    *
    * Use this as `input.through(rotating.jsonNode(objectWriter))`.
    *
    * @param objectWriter
    *   Jackson writer used to serialize each JSON tree.
    * @see
    *   https://github.com/FasterXML/jackson-databind
    */
  def jsonNode(objectWriter: ObjectWriter): Pipe[F, JsonNode, RotateFile]
}

/** A rotating sink whose files are bounded by the number of input values. */
abstract class RotateBySize[F[_]] extends RotateSink[F] {}

/** A rotating sink whose files are bounded by a time-based policy.
  *
  * The schema-aware methods should be used when each rotation must create a writer with an explicit Avro
  * schema.
  */
abstract class RotateByPolicy[F[_]] extends RotateSink[F] {

  /** Write schema-bound Avro records to rotating files.
    *
    * @param schema
    *   Schema used by every rotated Avro writer.
    * @param compression
    *   Avro compression configuration.
    * @see
    *   https://avro.apache.org
    */
  def avro(schema: Schema, compression: AvroCompression): Pipe[F, GenericRecord, RotateFile]

  /** Select compression settings for schema-bound rotating Avro output. */
  final def avro(
    schema: Schema,
    f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, RotateFile] =
    avro(schema, f(AvroCompression))

  /** Write uncompressed schema-bound Avro records to rotating files. */
  final def avro(schema: Schema): Pipe[F, GenericRecord, RotateFile] =
    avro(schema, AvroCompression.Uncompressed)

  /** Write schema-bound records to rotating binary Avro files.
    *
    * @param schema
    *   Schema used by every rotated writer.
    * @see
    *   https://avro.apache.org
    */
  def binAvro(schema: Schema): Pipe[F, GenericRecord, RotateFile]

  /** Write schema-bound records to rotating Jackson JSON files.
    *
    * @param schema
    *   Schema used by every rotated writer.
    * @see
    *   https://github.com/FasterXML/jackson
    */
  def jackson(schema: Schema): Pipe[F, GenericRecord, RotateFile]

  /** Write schema-bound records to rotating Parquet files.
    *
    * @param schema
    *   Schema used by every rotated writer.
    * @param f
    *   Function that customizes each Parquet writer builder.
    * @see
    *   https://parquet.apache.org
    */
  def parquet(schema: Schema, f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, RotateFile]

  /** Write schema-bound records to rotating Parquet files with default settings.
    *
    * @param schema
    *   Schema used by every rotated writer.
    * @see
    *   https://parquet.apache.org
    */
  final def parquet(schema: Schema): Pipe[F, GenericRecord, RotateFile] =
    parquet(schema, identity[Builder[GenericRecord]])
}
