package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.functor.given
import com.fasterxml.jackson.databind.{JsonNode, ObjectWriter}
import fs2.io.readInputStream
import fs2.{Pipe, Pull, Stream}
import io.circe.Json
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter
import scalapb.GeneratedMessage
import squants.information.Information

import java.io.InputStream

/** A collection of effectful `fs2.Pipe`s for writing one format to a Hadoop-compatible path.
  *
  * Obtain a sink from `Hadoop.sink(path)`, choose the method matching the input stream's element type, and
  * run the pipe with `through`:
  *
  * {{{
  * val written =
  *   input.through(hadoop.sink(path).text).compile.toList
  * }}}
  *
  * Each operation creates or overwrites the configured output when the stream is run, closes its writer when
  * the stream finishes, and emits an `Int` for each input chunk. The `Int` is the number of values or bytes
  * written in that chunk, not a final total. Use `.compile.fold(0)(_ + _)` when a total is required.
  */
sealed trait FileSink[F[_]] {

  /** Write Avro records to the configured file.
    *
    * Use this pipe as `stream.through(hadoop.sink(path).avro(codec))`. The records must use a compatible
    * schema. The output file is created when the stream is run and the writer is closed when the stream
    * finishes.
    *
    * The resulting stream emits the number of records written in each input chunk. The selected codec is
    * applied to the Avro data file.
    *
    * @param compression
    *   Avro compression configuration.
    * @see
    *   https://avro.apache.org
    */
  def avro(compression: AvroCompression): Pipe[F, GenericRecord, Int]

  /** Write Avro records using a function to select the compression settings.
    *
    * This is equivalent to `avro(f(AvroCompression))` and is useful when the compression choice is built from
    * the available Avro options.
    */
  def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, Int]

  /** Write uncompressed Avro records using the default Avro configuration.
    *
    * Use this as `stream.through(hadoop.sink(path).avro)`.
    */
  def avro: Pipe[F, GenericRecord, Int]

  /** Write Avro records in binary data-file format.
    *
    * Use this as `stream.through(hadoop.sink(path).binAvro)`. The output uses Avro's binary encoding and the
    * records must share a compatible schema. The resulting stream emits the number of records written in each
    * input chunk.
    *
    * @see
    *   https://avro.apache.org
    */
  def binAvro: Pipe[F, GenericRecord, Int]

  /** Write Avro records using Jackson's JSON representation.
    *
    * Use this as `stream.through(hadoop.sink(path).jackson)`. The records must share a compatible schema, and
    * the resulting stream emits the number of records written in each input chunk.
    *
    * @see
    *   https://github.com/FasterXML/jackson
    */
  def jackson: Pipe[F, GenericRecord, Int]

  /** Write Avro records as Parquet.
    *
    * Use this as `stream.through(hadoop.sink(path).parquet(identity))` when no Parquet builder customization
    * is needed. The function receives the configured builder after the standard schema, output path,
    * configuration, compression, and overwrite settings have been applied.
    *
    * The resulting stream emits the number of records written in each input chunk.
    *
    * @param f
    *   Function that customizes the configured Parquet writer builder.
    * @see
    *   https://parquet.apache.org
    */
  def parquet(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): Pipe[F, GenericRecord, Int]

  /** Write Avro records as Parquet using the default writer configuration.
    *
    * Use this as `stream.through(hadoop.sink(path).parquet)`. The resulting stream emits the number of
    * records written in each input chunk.
    */
  def parquet: Pipe[F, GenericRecord, Int]

  /** Write byte values to the configured file.
    *
    * Use this as `stream.through(hadoop.sink(path).bytes)`. Compression is selected from the output path and
    * Hadoop configuration. The resulting stream emits the number of bytes written in each input chunk.
    */
  def bytes: Pipe[F, Byte, Int]

  /** Write the contents of input streams to the configured file.
    *
    * Use this as `stream.through(hadoop.sink(path).inputStream(bufferSize))`. Each input stream is read using
    * the supplied byte buffer size and closed after it has been consumed. The resulting stream emits the
    * number of bytes written for each input chunk.
    *
    * @param bufferSize
    *   Positive byte-buffer size expressed as a `squants` information unit.
    */
  def inputStream(bufferSize: Information): Pipe[F, InputStream, Int]

  /** Write Circe JSON values to the configured file as newline-delimited JSON.
    *
    * Use this as `stream.through(hadoop.sink(path).circe)`. Values are written with Circe's compact printer,
    * one value per platform line separator. The resulting stream emits the number of JSON values written in
    * each input chunk.
    *
    * @see
    *   https://github.com/circe/circe
    */
  def circe: Pipe[F, Json, Int]

  /** Write CSV rows using the supplied Kantan CSV configuration.
    *
    * Use this as `stream.through(hadoop.sink(path).kantan(configuration))`. Each input value is a row
    * represented by a sequence of column values. Any configured CSV header is written when the file is
    * opened. The resulting stream emits the number of rows written in each input chunk.
    *
    * @param csvConfiguration
    *   CSV dialect and formatting configuration.
    * @see
    *   https://nrinaudo.github.io/kantan.csv
    */
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], Int]

  /** Write CSV rows using a function that customizes the default configuration.
    *
    * This is equivalent to `kantan(f(CsvConfiguration.rfc))`.
    *
    * @param f
    *   Function that customizes the default RFC CSV configuration.
    * @see
    *   https://nrinaudo.github.io/kantan.csv
    */
  def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], Int]

  /** Write CSV rows using the default RFC CSV configuration.
    *
    * Use this as `stream.through(hadoop.sink(path).kantan)`.
    */
  def kantan: Pipe[F, Seq[String], Int]

  /** Write text values to the configured file, one value per line.
    *
    * Use this as `stream.through(hadoop.sink(path).text)`. A platform line separator is appended to every
    * value. The resulting stream emits the number of text values written in each input chunk.
    */
  def text: Pipe[F, String, Int]

  /** Write Protocol Buffer messages in delimited binary form.
    *
    * Use this as `stream.through(hadoop.sink(path).protobuf)`. Each message is written with its length
    * delimiter, and the resulting stream emits the number of messages written in each input chunk. Keep
    * serialized messages below 2 GiB, the maximum supported by all implementations.
    *
    * @see
    *   https://protobuf.dev/programming-guides/proto-limits/#total
    */
  def protobuf: Pipe[F, GeneratedMessage, Int]

  /** Write JSON tree values using a Jackson `ObjectWriter`, one value per line.
    *
    * Use this as `stream.through(hadoop.sink(path).jsonNode(objectWriter))`. The supplied writer controls
    * JSON serialization, and a line separator is appended after each value. The resulting stream emits the
    * number of JSON values written in each input chunk.
    *
    * @param ow
    *   Jackson writer used to serialize each JSON tree.
    * @see
    *   https://github.com/FasterXML/jackson-databind
    */
  def jsonNode(ow: ObjectWriter): Pipe[F, JsonNode, Int]
}

final private class FileSinkImpl[F[_]: Sync](configuration: Configuration, url: Url) extends FileSink[F] {
  private type GetWriter[A] = Reader[Schema, Resource[F, HadoopWriter[F, A]]]

  private def generic_record_stream_step_leg(writer: GetWriter[GenericRecord])(
    ss: Stream[F, GenericRecord]): Stream[F, Int] =
    ss.pull.stepLeg.flatMap {
      case Some(leg) =>
        val schema = leg.head(0).getSchema
        Stream
          .resource(writer(schema))
          .flatMap(w => leg.stream.cons(leg.head).chunks.evalMap(c => w.write(c).as(c.size)))
          .pull
          .echo
      case None => Pull.done
    }.stream

  override def avro(compression: AvroCompression): Pipe[F, GenericRecord, Int] = {
    val get_writer: GetWriter[GenericRecord] =
      Reader(schema => HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, url))

    generic_record_stream_step_leg(get_writer)
  }

  override def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, Int] =
    avro(f(AvroCompression))

  override val avro: Pipe[F, GenericRecord, Int] =
    avro(AvroCompression.Uncompressed)

  override val binAvro: Pipe[F, GenericRecord, Int] = {
    val get_writer: GetWriter[GenericRecord] =
      Reader(schema => HadoopWriter.binAvroR[F](configuration, schema, url))

    generic_record_stream_step_leg(get_writer)
  }

  override val jackson: Pipe[F, GenericRecord, Int] = {
    val get_writer: GetWriter[GenericRecord] =
      Reader(schema => HadoopWriter.jacksonR[F](configuration, schema, url))

    generic_record_stream_step_leg(get_writer)
  }

  override def parquet(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): Pipe[F, GenericRecord, Int] = {
    val get_writer: GetWriter[GenericRecord] =
      Reader(schema => HadoopWriter.parquetR[F](default_parquet_write_builder(configuration, schema, f), url))

    generic_record_stream_step_leg(get_writer)
  }

  override val parquet: Pipe[F, GenericRecord, Int] =
    parquet(identity[AvroParquetWriter.Builder[GenericRecord]])

  override val bytes: Pipe[F, Byte, Int] = { (ss: Stream[F, Byte]) =>
    Stream
      .resource(HadoopWriter.byteR[F](configuration, url))
      .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  override def inputStream(bufferSize: Information): Pipe[F, InputStream, Int] = {
    (ss: Stream[F, InputStream]) =>
      ss.flatMap(is => readInputStream[F](is.pure[F], bufferSize.value.toInt, true)).through(bytes)
  }

  override val circe: Pipe[F, Json, Int] = { (ss: Stream[F, Json]) =>
    Stream
      .resource(HadoopWriter.circeR[F](configuration, url))
      .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  override def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], Int] = {
    (ss: Stream[F, Seq[String]]) =>
      Stream
        .resource(HadoopWriter.csvR[F](configuration, url, csvConfiguration))
        .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  override def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], Int] =
    kantan(f(CsvConfiguration.rfc))

  override val kantan: Pipe[F, Seq[String], Int] =
    kantan(CsvConfiguration.rfc)

  override val text: Pipe[F, String, Int] = { (ss: Stream[F, String]) =>
    Stream
      .resource(HadoopWriter.stringR[F](configuration, url))
      .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  override val protobuf: Pipe[F, GeneratedMessage, Int] = { (ss: Stream[F, GeneratedMessage]) =>
    Stream.resource(HadoopWriter.protobufR[F](configuration, url)).flatMap { w =>
      ss.chunks.evalMap(c => w.write(c).as(c.size))
    }
  }

  override def jsonNode(ow: ObjectWriter): Pipe[F, JsonNode, Int] = { (ss: Stream[F, JsonNode]) =>
    Stream.resource(HadoopWriter.jsonNodeR[F](configuration, url, ow)).flatMap { w =>
      ss.chunks.evalMap(c => w.write(c).as(c.size))
    }
  }
}
