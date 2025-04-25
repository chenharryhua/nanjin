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

  def avro(compression: AvroCompression): Sink[GenericRecord]
  final def avro(f: AvroCompression.type => AvroCompression): Sink[GenericRecord] =
    avro(f(AvroCompression))
  final def avro: Sink[GenericRecord] =
    avro(AvroCompression.Uncompressed)

  def avro(schema: Schema, compression: AvroCompression): Sink[GenericRecord]
  def avro(schema: Schema, f: AvroCompression.type => AvroCompression): Sink[GenericRecord] =
    avro(schema, f(AvroCompression))
  def avro(schema: Schema): Sink[GenericRecord] =
    avro(schema, AvroCompression.Uncompressed)

  def binAvro: Sink[GenericRecord]
  def binAvro(schema: Schema): Sink[GenericRecord]

  def jackson: Sink[GenericRecord]
  def jackson(schema: Schema): Sink[GenericRecord]

  def parquet(f: Endo[Builder[GenericRecord]]): Sink[GenericRecord]
  final def parquet: Sink[GenericRecord] =
    parquet(a => a)
  def parquet(schema: Schema, f: Endo[Builder[GenericRecord]]): Sink[GenericRecord]
  final def parquet(schema: Schema): Sink[GenericRecord] =
    parquet(schema, identity)

  def kantan(csvConfiguration: CsvConfiguration): Sink[Seq[String]]
  final def kantan(f: Endo[CsvConfiguration]): Sink[Seq[String]] =
    kantan(f(CsvConfiguration.rfc))
  final def kantan: Sink[Seq[String]] =
    kantan(CsvConfiguration.rfc)

  def bytes: Sink[Byte]

  def circe: Sink[Json]

  def text: Sink[String]

  def protobuf: Sink[GeneratedMessage]
}
