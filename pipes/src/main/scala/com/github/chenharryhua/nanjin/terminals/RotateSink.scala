package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.implicits.showInterpolator
import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.chrono.TickedValue
import fs2.Pipe
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetWriter.Builder
import scalapb.GeneratedMessage

import java.time.{Duration, LocalDateTime, ZonedDateTime}
import java.util.UUID

/** Instruction to create a new rotated file.
  *
  * This event marks the beginning of a new `Tick` window. A writer opened from this information will accept
  * records that belong to that tick.
  *
  * @param sequenceId
  *   globally unique id for the entire rotation stream
  *
  * @param index
  *   1-based, strictly increasing number of the file within the sequence
  *
  * @param openTime
  *   timestamp when the sink starts writing records for this file
  */
final case class CreateRotateFile(
  sequenceId: UUID,
  index: Long,
  openTime: ZonedDateTime
)

final case class RotateWriteException(tv: TickedValue[Url], cause: Throwable) extends Exception(
      show"failed to write to: ${tv.value} (seq=${tv.tick.sequenceId}, idx=${tv.tick.index})",
      cause)

/** Result of a completed rotation window.
  *
  * One `RotateFile` is emitted for every finished `Tick`.
  *
  * The file identified by `url` contains exactly `recordCount` records written while that tick window was
  * active.
  *
  * Invariants typically guaranteed by rotation sinks:
  *
  *   - there is one `RotateFile` per tick
  *   - tick indices are strictly increasing
  *   - the sum of `recordCount` equals the total number of input records
  *   - records in this file belong exclusively to the time window
  *   - open: the moment when the file open for write
  *   - close: the moment when the file is closed
  *
  * tick represents a plan while RotateFile is what really happen
  */

final case class RotateFile(open: LocalDateTime, close: LocalDateTime, url: Url, recordCount: Long) {
  val window: Duration = Duration.between(open, close)
}

object RotateFile {
  implicit val encoderRotateFile: Encoder[RotateFile] =
    (a: RotateFile) =>
      Json.obj(
        "open" -> a.open.asJson,
        "close" -> a.close.asJson,
        "window" -> DurationFormatter.defaultFormatter.format(a.window).asJson,
        "url" -> a.url.asJson,
        "recordCount" -> Json.fromLong(a.recordCount)
      )

  implicit val decoderRotateFile: Decoder[RotateFile] =
    (c: HCursor) =>
      for {
        open <- c.get[LocalDateTime]("open")
        close <- c.get[LocalDateTime]("close")
        url <- c.get[Url]("url")
        recordCount <- c.get[Long]("recordCount")
      } yield RotateFile(
        open = open,
        close = close,
        url = url,
        recordCount = recordCount
      )
}

sealed trait RotateSink[F[_]] {
  protected type Sink[A] = Pipe[F, A, TickedValue[RotateFile]]

  /** `https://avro.apache.org`
    *
    * An Avro rotate sink that periodically writes generic records to Avro files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def avro(compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[RotateFile]]

  /** `https://avro.apache.org`
    *
    * An Avro rotate sink that periodically writes generic records to Avro files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  final def avro(
    f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[RotateFile]] =
    avro(f(AvroCompression))

  /** `https://avro.apache.org`
    *
    * An Avro rotate sink that periodically writes generic records to Avro files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  final def avro: Pipe[F, GenericRecord, TickedValue[RotateFile]] =
    avro(AvroCompression.Uncompressed)

  /** `https://avro.apache.org`
    *
    * A Binary Avro rotate sink that periodically writes generic records to Binary Avro files, with file
    * rotation controlled by a time-based or size-based policy.
    */
  def binAvro: Pipe[F, GenericRecord, TickedValue[RotateFile]]

  /** `https://github.com/FasterXML/jackson`
    *
    * A Jackson rotate sink that periodically writes generic records to Jackson json files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def jackson: Pipe[F, GenericRecord, TickedValue[RotateFile]]

  /** `https://parquet.apache.org`
    *
    * A Parquet rotate sink that periodically writes generic records to Parquet files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def parquet(f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[RotateFile]]

  /** `https://parquet.apache.org`
    *
    * A Parquet rotate sink that periodically writes generic records to Parquet files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  final def parquet: Pipe[F, GenericRecord, TickedValue[RotateFile]] =
    parquet(identity[Builder[GenericRecord]])

  /** `https://nrinaudo.github.io/kantan.csv`
    *
    * A Kantan rotate sink that periodically writes rows to CSV files, represented by sequence of strings,
    * with file rotation controlled by a time-based or size-based policy.
    */
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], TickedValue[RotateFile]]

  /** `https://nrinaudo.github.io/kantan.csv`
    *
    * A Kantan rotate sink that periodically writes rows to CSV files, represented by sequence of strings,
    * with file rotation controlled by a time-based or size-based policy.
    */
  final def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], TickedValue[RotateFile]] =
    kantan(f(CsvConfiguration.rfc))

  /** `https://nrinaudo.github.io/kantan.csv`
    *
    * A Kantan rotate sink that periodically writes rows to CSV files, represented by sequence of strings,
    * with file rotation controlled by a time-based or size-based policy.
    */
  final def kantan: Pipe[F, Seq[String], TickedValue[RotateFile]] =
    kantan(CsvConfiguration.rfc)

  /** A Bytes rotate sink that periodically writes bytes to binary files, with file rotation controlled by a
    * time-based or size-based policy.
    */
  def bytes: Pipe[F, Byte, TickedValue[RotateFile]]

  /** `https://github.com/circe/circe`
    *
    * A Circe rotate sink that periodically writes json to circe json files, with file rotation controlled by
    * a time-based or size-based policy.
    */
  def circe: Pipe[F, Json, TickedValue[RotateFile]]

  /** A Text rotate sink that periodically writes strings to text files, with file rotation controlled by a
    * time-based or size-based policy.
    */
  def text: Pipe[F, String, TickedValue[RotateFile]]

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * `https://protobuf.dev/programming-guides/proto-limits/#total`
    *
    * A Protobuf rotate sink that periodically writes GeneratedMessage to proto-buf files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def protobuf: Pipe[F, GeneratedMessage, TickedValue[RotateFile]]

  /** `https://github.com/FasterXML/jackson-databind`
    *
    * A JsonNode rotate sink that periodically writes JsonNode to text files, with file rotation controlled by
    * a time-based or size-based policy.
    */
  def jsonNode: Pipe[F, JsonNode, TickedValue[RotateFile]]
}

abstract class RotateBySize[F[_]] extends RotateSink[F] {}

abstract class RotateByPolicy[F[_]] extends RotateSink[F] {

  /** `https://avro.apache.org`
    */
  def avro(schema: Schema, compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[RotateFile]]

  /** `https://avro.apache.org`
    */
  final def avro(
    schema: Schema,
    f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[RotateFile]] =
    avro(schema, f(AvroCompression))

  /** `https://avro.apache.org`
    */
  final def avro(schema: Schema): Pipe[F, GenericRecord, TickedValue[RotateFile]] =
    avro(schema, AvroCompression.Uncompressed)

  /** `https://avro.apache.org`
    */
  def binAvro(schema: Schema): Pipe[F, GenericRecord, TickedValue[RotateFile]]

  /** `https://github.com/FasterXML/jackson`
    */
  def jackson(schema: Schema): Pipe[F, GenericRecord, TickedValue[RotateFile]]

  /** `https://parquet.apache.org`
    */
  def parquet(
    schema: Schema,
    f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[RotateFile]]

  /** `https://parquet.apache.org`
    */
  final def parquet(schema: Schema): Pipe[F, GenericRecord, TickedValue[RotateFile]] =
    parquet(schema, identity[Builder[GenericRecord]])
}
