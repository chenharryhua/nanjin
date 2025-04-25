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

trait RotateSink[F[_]] {
  protected type Sink[A] = Pipe[F, A, TickedValue[Int]]

  def avro(compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]]
  final def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(f(AvroCompression))
  final def avro: Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(AvroCompression.Uncompressed)

  def avro(schema: Schema, compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]]
  def avro(
    schema: Schema,
    f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(schema, f(AvroCompression))
  def avro(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(schema, AvroCompression.Uncompressed)

  def binAvro: Pipe[F, GenericRecord, TickedValue[Int]]
  def binAvro(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]]

  def jackson: Pipe[F, GenericRecord, TickedValue[Int]]
  def jackson(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]]

  def parquet(f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[Int]]
  final def parquet: Pipe[F, GenericRecord, TickedValue[Int]] =
    parquet(a => a)
  def parquet(schema: Schema, f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[Int]]
  final def parquet(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] =
    parquet(schema, identity)

  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], TickedValue[Int]]
  final def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], TickedValue[Int]] =
    kantan(f(CsvConfiguration.rfc))
  final def kantan: Pipe[F, Seq[String], TickedValue[Int]] =
    kantan(CsvConfiguration.rfc)

  def bytes: Pipe[F, Byte, TickedValue[Int]]

  def circe: Pipe[F, Json, TickedValue[Int]]

  def text: Pipe[F, String, TickedValue[Int]]

  def protobuf: Pipe[F, GeneratedMessage, TickedValue[Int]]
}
