package com.github.chenharryhua.nanjin.devices

import java.io.InputStream
import java.net.URI

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroOutputStream,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}

final class NJHadoop[F[_]: Sync: ContextShift](hadoopConfig: Configuration, blocker: Blocker) {

  private def fileSystem(pathStr: String): Resource[F, FileSystem] =
    Resource.fromAutoCloseable(blocker.delay(FileSystem.get(new URI(pathStr), hadoopConfig)))

  private def fsOutput(pathStr: String): Resource[F, FSDataOutputStream] =
    for {
      fs <- fileSystem(pathStr)
      rs <- Resource.fromAutoCloseable(blocker.delay(fs.create(new Path(pathStr))))
    } yield rs

  private def fsInput(pathStr: String): Resource[F, FSDataInputStream] =
    for {
      fs <- fileSystem(pathStr)
      rs <- Resource.fromAutoCloseable(blocker.delay(fs.open(new Path(pathStr))))
    } yield rs

  def sink(pathStr: String): Pipe[F, Byte, Unit] = { (ss: Stream[F, Byte]) =>
    for {
      fs <- Stream.resource(fsOutput(pathStr))
      _ <- ss.through(writeOutputStream[F](Sync[F].pure(fs), blocker))
    } yield ()
  }

  def inputStream(pathStr: String): Stream[F, InputStream] =
    Stream.resource(fsInput(pathStr).widen)

  def source(pathStr: String): Stream[F, Byte] =
    for {
      fs <- inputStream(pathStr)
      b <- readInputStream[F](Sync[F].pure[InputStream](fs), chunkSize, blocker)
    } yield b

  def delete(pathStr: String): F[Boolean] =
    fileSystem(pathStr).use(fs => blocker.delay(fs.delete(new Path(pathStr), true)))

  /// parquet
  def parquetSink(pathStr: String, schema: Schema): Pipe[F, GenericRecord, Unit] = {
    (ss: Stream[F, GenericRecord]) =>
      val outputFile = HadoopOutputFile.fromPath(new Path(pathStr), hadoopConfig)
      for {
        writer <- Stream.resource(
          Resource.fromAutoCloseable(
            blocker.delay(
              AvroParquetWriter
                .builder[GenericRecord](outputFile)
                .withConf(hadoopConfig)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withSchema(schema)
                .withDataModel(GenericData.get())
                .build())))
        _ <- ss.map(writer.write)
      } yield ()
  }

  def parquetSource(pathStr: String): Stream[F, GenericRecord] = {
    val inputFile = HadoopInputFile.fromPath(new Path(pathStr), hadoopConfig)
    for {
      builder <- Stream.resource(
        Resource.fromAutoCloseable(
          blocker.delay(AvroParquetReader.builder[GenericRecord](inputFile).build())))
    } yield builder.read()
  }

  //avro data

  def avroSink[A: AvroEncoder](pathStr: String): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    for {
      os <- Stream.resource(fsOutput(pathStr))
      aos = AvroOutputStream.data[A].to(os).build()
      m <- ss
    } yield aos.write(m)
  }

  def avroSource[A: AvroDecoder](pathStr: String): Stream[F, A] =
    for {
      is <- Stream.resource(fsInput(pathStr))
      a <-
        Stream.fromIterator(AvroInputStream.data[A].from(is).build(AvroDecoder[A].schema).iterator)
    } yield a
}
