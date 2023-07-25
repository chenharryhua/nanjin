package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.toFunctorOps
import fs2.Chunk
import kantan.csv.CsvConfiguration
import kantan.csv.ops.toCsvOutputOps
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.apache.parquet.avro.AvroParquetWriter

sealed private trait HadoopWriter[F[_], A] {
  def write(ck: Chunk[A]): F[Unit]
}

private object HadoopWriter {
  def avro[F[_]](
    codecFactory: CodecFactory,
    schema: Schema,
    configuration: Configuration,
    blockSizeHint: Long,
    path: NJPath)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    for {
      dfw <- Resource.make[F, DataFileWriter[GenericRecord]](
        F.blocking(new DataFileWriter(new GenericDatumWriter(schema)).setCodec(codecFactory)))(r =>
        F.blocking(r.close()))
      os <-
        Resource.make(F.blocking(path.hadoopOutputFile(configuration).createOrOverwrite(blockSizeHint)))(r =>
          F.blocking(r.close()))
      writer <- Resource.make(F.blocking(dfw.create(schema, os)))(r => F.blocking(r.close()))
    } yield new HadoopWriter[F, GenericRecord] {
      override def write(ck: Chunk[GenericRecord]): F[Unit] = F.blocking(ck.foreach(writer.append))
    }

  def parquet[F[_]](writeBuilder: Reader[NJPath, AvroParquetWriter.Builder[GenericRecord]], path: NJPath)(
    implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    Resource
      .make(F.blocking(writeBuilder.run(path).build()))(r => F.blocking(r.close()))
      .map(pw =>
        new HadoopWriter[F, GenericRecord] {
          override def write(ck: Chunk[GenericRecord]): F[Unit] = F.blocking(ck.foreach(pw.write))
        })

  def kantan[F[_], A: NJHeaderEncoder](
    configuration: Configuration,
    compressionLevel: CompressionLevel,
    blockSizeHint: Long,
    csvConfiguration: CsvConfiguration,
    path: NJPath)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, A]] =
    Resource
      .make(
        F.blocking(fileOutputStream(path, configuration, compressionLevel, blockSizeHint).asCsvWriter[A](
          csvConfiguration)))(r => F.blocking(r.close()))
      .map(cw =>
        new HadoopWriter[F, A] {
          override def write(ck: Chunk[A]): F[Unit] = F.blocking(cw.write(ck.iterator)).void
        })

  def bytes[F[_]](
    configuration: Configuration,
    compressionLevel: CompressionLevel,
    blockSizeHint: Long,
    path: NJPath)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, Byte]] =
    Resource
      .make(F.blocking(fileOutputStream(path, configuration, compressionLevel, blockSizeHint)))(r =>
        F.blocking(r.close()))
      .map(os =>
        new HadoopWriter[F, Byte] {
          override def write(ck: Chunk[Byte]): F[Unit] = F.blocking(os.write(ck.toArray)).void
        })
}
