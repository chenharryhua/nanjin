package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import com.github.chenharryhua.nanjin.common.chrono.TickedValue
import fs2.Pipe
import io.circe.Json
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetWriter.Builder
import scalapb.GeneratedMessage

abstract class RotateSink[F[_]] {
  protected type Sink[A] = Pipe[F, A, TickedValue[Int]]

  /** [[https://avro.apache.org]]
    */
  def avro(compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]]

  /** [[https://avro.apache.org]]
    */
  final def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(f(AvroCompression))

  /** [[https://avro.apache.org]]
    */
  final def avro: Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(AvroCompression.Uncompressed)

  /** [[https://avro.apache.org]]
    */
  def avro(schema: Schema, compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]]

  /** [[https://avro.apache.org]]
    */
  final def avro(
    schema: Schema,
    f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(schema, f(AvroCompression))

  /** [[https://avro.apache.org]]
    */
  final def avro(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(schema, AvroCompression.Uncompressed)

  /** [[https://avro.apache.org]]
    */
  def binAvro: Pipe[F, GenericRecord, TickedValue[Int]]

  /** [[https://avro.apache.org]]
    */
  def binAvro(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]]

  /** [[https://github.com/FasterXML/jackson]]
    */
  def jackson: Pipe[F, GenericRecord, TickedValue[Int]]

  /** [[https://github.com/FasterXML/jackson]]
    */
  def jackson(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]]

  /** [[https://parquet.apache.org]]
    */
  def parquet(f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[Int]]

  /** [[https://parquet.apache.org]]
    */
  final def parquet: Pipe[F, GenericRecord, TickedValue[Int]] =
    parquet(identity[Builder[GenericRecord]])

  /** [[https://parquet.apache.org]]
    */
  def parquet(schema: Schema, f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[Int]]

  /** [[https://parquet.apache.org]]
    */
  final def parquet(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] =
    parquet(schema, identity[Builder[GenericRecord]])

  /** [[https://nrinaudo.github.io/kantan.csv]]
    */
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], TickedValue[Int]]

  /** [[https://nrinaudo.github.io/kantan.csv]]
    */
  final def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], TickedValue[Int]] =
    kantan(f(CsvConfiguration.rfc))

  /** [[https://nrinaudo.github.io/kantan.csv]]
    */
  final def kantan: Pipe[F, Seq[String], TickedValue[Int]] =
    kantan(CsvConfiguration.rfc)

  def bytes: Pipe[F, Byte, TickedValue[Int]]

  /** [[https://github.com/circe/circe]]
    */
  def circe: Pipe[F, Json, TickedValue[Int]]

  /** newline separated string
    */
  def text: Pipe[F, String, TickedValue[Int]]

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * [[https://protobuf.dev/programming-guides/proto-limits/#total]]
    */
  def protobuf: Pipe[F, GeneratedMessage, TickedValue[Int]]
}
