package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import com.fasterxml.jackson.databind.JsonNode
import fs2.Pipe
import io.circe.Json
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetWriter.Builder
import scalapb.GeneratedMessage

sealed trait RotateSink[F[_]] {
  protected type Sink[A] = Pipe[F, A, RotateFile]

  /** `https://avro.apache.org`
    *
    * An Avro rotate sink that periodically writes generic records to Avro files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def avro(compression: AvroCompression): Pipe[F, GenericRecord, RotateFile]

  /** `https://avro.apache.org`
    *
    * An Avro rotate sink that periodically writes generic records to Avro files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  final def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, RotateFile] =
    avro(f(AvroCompression))

  /** `https://avro.apache.org`
    *
    * An Avro rotate sink that periodically writes generic records to Avro files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  final def avro: Pipe[F, GenericRecord, RotateFile] =
    avro(AvroCompression.Uncompressed)

  /** `https://avro.apache.org`
    *
    * A Binary Avro rotate sink that periodically writes generic records to Binary Avro files, with file
    * rotation controlled by a time-based or size-based policy.
    */
  def binAvro: Pipe[F, GenericRecord, RotateFile]

  /** `https://github.com/FasterXML/jackson`
    *
    * A Jackson rotate sink that periodically writes generic records to Jackson json files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def jackson: Pipe[F, GenericRecord, RotateFile]

  /** `https://parquet.apache.org`
    *
    * A Parquet rotate sink that periodically writes generic records to Parquet files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def parquet(f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, RotateFile]

  /** `https://parquet.apache.org`
    *
    * A Parquet rotate sink that periodically writes generic records to Parquet files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  final def parquet: Pipe[F, GenericRecord, RotateFile] =
    parquet(identity[Builder[GenericRecord]])

  /** `https://nrinaudo.github.io/kantan.csv`
    *
    * A Kantan rotate sink that periodically writes rows to CSV files, represented by sequence of strings,
    * with file rotation controlled by a time-based or size-based policy.
    */
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], RotateFile]

  /** `https://nrinaudo.github.io/kantan.csv`
    *
    * A Kantan rotate sink that periodically writes rows to CSV files, represented by sequence of strings,
    * with file rotation controlled by a time-based or size-based policy.
    */
  final def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], RotateFile] =
    kantan(f(CsvConfiguration.rfc))

  /** `https://nrinaudo.github.io/kantan.csv`
    *
    * A Kantan rotate sink that periodically writes rows to CSV files, represented by sequence of strings,
    * with file rotation controlled by a time-based or size-based policy.
    */
  final def kantan: Pipe[F, Seq[String], RotateFile] =
    kantan(CsvConfiguration.rfc)

  /** A Bytes rotate sink that periodically writes bytes to binary files, with file rotation controlled by a
    * time-based or size-based policy.
    */
  def bytes: Pipe[F, Byte, RotateFile]

  /** `https://github.com/circe/circe`
    *
    * A Circe rotate sink that periodically writes json to circe json files, with file rotation controlled by
    * a time-based or size-based policy.
    */
  def circe: Pipe[F, Json, RotateFile]

  /** A Text rotate sink that periodically writes strings to text files, with file rotation controlled by a
    * time-based or size-based policy.
    */
  def text: Pipe[F, String, RotateFile]

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * `https://protobuf.dev/programming-guides/proto-limits/#total`
    *
    * A Protobuf rotate sink that periodically writes GeneratedMessage to proto-buf files, with file rotation
    * controlled by a time-based or size-based policy.
    */
  def protobuf: Pipe[F, GeneratedMessage, RotateFile]

  /** `https://github.com/FasterXML/jackson-databind`
    *
    * A JsonNode rotate sink that periodically writes JsonNode to text files, with file rotation controlled by
    * a time-based or size-based policy.
    */
  def jsonNode: Pipe[F, JsonNode, RotateFile]
}

abstract class RotateBySize[F[_]] extends RotateSink[F] {}

abstract class RotateByPolicy[F[_]] extends RotateSink[F] {

  /** `https://avro.apache.org`
    */
  def avro(schema: Schema, compression: AvroCompression): Pipe[F, GenericRecord, RotateFile]

  /** `https://avro.apache.org`
    */
  final def avro(
    schema: Schema,
    f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, RotateFile] =
    avro(schema, f(AvroCompression))

  /** `https://avro.apache.org`
    */
  final def avro(schema: Schema): Pipe[F, GenericRecord, RotateFile] =
    avro(schema, AvroCompression.Uncompressed)

  /** `https://avro.apache.org`
    */
  def binAvro(schema: Schema): Pipe[F, GenericRecord, RotateFile]

  /** `https://github.com/FasterXML/jackson`
    */
  def jackson(schema: Schema): Pipe[F, GenericRecord, RotateFile]

  /** `https://parquet.apache.org`
    */
  def parquet(schema: Schema, f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, RotateFile]

  /** `https://parquet.apache.org`
    */
  final def parquet(schema: Schema): Pipe[F, GenericRecord, RotateFile] =
    parquet(schema, identity[Builder[GenericRecord]])
}
