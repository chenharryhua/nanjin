package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import com.github.chenharryhua.nanjin.common.chrono.TickedValue
import fs2.Pipe
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetWriter.Builder
import scalapb.GeneratedMessage

import java.time.ZonedDateTime
import java.util.UUID

final case class CreateRotateFileEvent(
  sequenceId: UUID,
  index: Long,
  writeTime: ZonedDateTime
)

final case class RotateFileResult(url: Url, count: Int)
object RotateFileResult {
  implicit val encoderRotateFileResult: Encoder[RotateFileResult] =
    (a: RotateFileResult) =>
      Json.obj("url" -> Json.fromString(a.url.toString()), "count" -> Json.fromInt(a.count))

  implicit val decoderRotateFileResult: Decoder[RotateFileResult] =
    (c: HCursor) =>
      for {
        url <- c.get[String]("url").map(Url.parse)
        count <- c.get[Int]("count")
      } yield RotateFileResult(url, count)
}

sealed trait RotateSink[F[_]] {
  protected type Sink[A] = Pipe[F, A, TickedValue[RotateFileResult]]

  /** [[https://avro.apache.org]]
    *
    * An Avro rotate sink that periodically writes generic records to Avro files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def avro(compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[RotateFileResult]]

  /** [[https://avro.apache.org]]
    *
    * An Avro rotate sink that periodically writes generic records to Avro files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  final def avro(
    f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[RotateFileResult]] =
    avro(f(AvroCompression))

  /** [[https://avro.apache.org]]
    *
    * An Avro rotate sink that periodically writes generic records to Avro files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  final def avro: Pipe[F, GenericRecord, TickedValue[RotateFileResult]] =
    avro(AvroCompression.Uncompressed)

  /** [[https://avro.apache.org]]
    *
    * A Binary Avro rotate sink that periodically writes generic records to Binary Avro files, with file
    * rotation controlled by a time-based or size-based policy.
    */
  def binAvro: Pipe[F, GenericRecord, TickedValue[RotateFileResult]]

  /** [[https://github.com/FasterXML/jackson]]
    *
    * A Jackson rotate sink that periodically writes generic records to Jackson json files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def jackson: Pipe[F, GenericRecord, TickedValue[RotateFileResult]]

  /** [[https://parquet.apache.org]]
    *
    * A Parquet rotate sink that periodically writes generic records to Parquet files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def parquet(f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[RotateFileResult]]

  /** [[https://parquet.apache.org]]
    *
    * A Parquet rotate sink that periodically writes generic records to Parquet files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  final def parquet: Pipe[F, GenericRecord, TickedValue[RotateFileResult]] =
    parquet(identity[Builder[GenericRecord]])

  /** [[https://nrinaudo.github.io/kantan.csv]]
    *
    * A Kantan rotate sink that periodically writes rows to CSV files, represented by sequence of strings,
    * with file rotation controlled by a time-based or size-based policy.
    */
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], TickedValue[RotateFileResult]]

  /** [[https://nrinaudo.github.io/kantan.csv]]
    *
    * A Kantan rotate sink that periodically writes rows to CSV files, represented by sequence of strings,
    * with file rotation controlled by a time-based or size-based policy.
    */
  final def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], TickedValue[RotateFileResult]] =
    kantan(f(CsvConfiguration.rfc))

  /** [[https://nrinaudo.github.io/kantan.csv]]
    *
    * A Kantan rotate sink that periodically writes rows to CSV files, represented by sequence of strings,
    * with file rotation controlled by a time-based or size-based policy.
    */
  final def kantan: Pipe[F, Seq[String], TickedValue[RotateFileResult]] =
    kantan(CsvConfiguration.rfc)

  /** A Bytes rotate sink that periodically writes bytes to binary files, with file rotation controlled by a
    * time-based or size-based policy.
    */
  def bytes: Pipe[F, Byte, TickedValue[RotateFileResult]]

  /** [[https://github.com/circe/circe]]
    *
    * A Circe rotate sink that periodically writes json to circe json files, with file rotation controlled by
    * a time-based or size-based policy.
    */
  def circe: Pipe[F, Json, TickedValue[RotateFileResult]]

  /** A Text rotate sink that periodically writes strings to text files, with file rotation controlled by a
    * time-based or size-based policy.
    */
  def text: Pipe[F, String, TickedValue[RotateFileResult]]

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * [[https://protobuf.dev/programming-guides/proto-limits/#total]]
    *
    * A Protobuf rotate sink that periodically writes GeneratedMessage to proto-buf files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def protobuf: Pipe[F, GeneratedMessage, TickedValue[RotateFileResult]]
}

abstract class RotateBySize[F[_]] extends RotateSink[F] {}

abstract class RotateByPolicy[F[_]] extends RotateSink[F] {

  /** [[https://avro.apache.org]]
    */
  def avro(
    schema: Schema,
    compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[RotateFileResult]]

  /** [[https://avro.apache.org]]
    */
  final def avro(
    schema: Schema,
    f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[RotateFileResult]] =
    avro(schema, f(AvroCompression))

  /** [[https://avro.apache.org]]
    */
  final def avro(schema: Schema): Pipe[F, GenericRecord, TickedValue[RotateFileResult]] =
    avro(schema, AvroCompression.Uncompressed)

  /** [[https://avro.apache.org]]
    */
  def binAvro(schema: Schema): Pipe[F, GenericRecord, TickedValue[RotateFileResult]]

  /** [[https://github.com/FasterXML/jackson]]
    */
  def jackson(schema: Schema): Pipe[F, GenericRecord, TickedValue[RotateFileResult]]

  /** [[https://parquet.apache.org]]
    */
  def parquet(
    schema: Schema,
    f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[RotateFileResult]]

  /** [[https://parquet.apache.org]]
    */
  final def parquet(schema: Schema): Pipe[F, GenericRecord, TickedValue[RotateFileResult]] =
    parquet(schema, identity[Builder[GenericRecord]])
}
