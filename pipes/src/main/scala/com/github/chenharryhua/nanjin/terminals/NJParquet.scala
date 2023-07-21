package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.Sync
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetReader}

final class NJParquet[F[_]] private (
  readBuilder: Reader[NJPath, ParquetReader.Builder[GenericRecord]],
  writeBuilder: Reader[NJPath, AvroParquetWriter.Builder[GenericRecord]])(implicit F: Sync[F]) {
  def updateReader(f: Endo[ParquetReader.Builder[GenericRecord]]): NJParquet[F] =
    new NJParquet(readBuilder.map(f), writeBuilder)

  def updateWriter(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): NJParquet[F] =
    new NJParquet(readBuilder, writeBuilder.map(f))

  def source(path: NJPath): Stream[F, GenericRecord] =
    for {
      rd <- Stream.bracket(F.blocking(readBuilder.run(path).build()))(r => F.blocking(r.close()))
      gr <- Stream.repeatEval(F.blocking(Option(rd.read()))).unNoneTerminate
    } yield gr

  def sink(path: NJPath): Pipe[F, GenericRecord, Nothing] = { (ss: Stream[F, GenericRecord]) =>
    Stream
      .resource(NJWriter.parquet[F](writeBuilder, path))
      .flatMap(pw => persistGenericRecord[F](ss, pw).stream)
  }
}

object NJParquet {
  def apply[F[_]: Sync](schema: Schema, cfg: Configuration): NJParquet[F] =
    new NJParquet[F](
      readBuilder = Reader((path: NJPath) =>
        AvroParquetReader
          .builder[GenericRecord](HadoopInputFile.fromPath(path.hadoopPath, cfg))
          .withDataModel(GenericData.get())
          .withConf(cfg)),
      writeBuilder = Reader((path: NJPath) =>
        AvroParquetWriter
          .builder[GenericRecord](HadoopOutputFile.fromPath(path.hadoopPath, cfg))
          .withDataModel(GenericData.get())
          .withConf(cfg)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE))
    )
}
