package com.github.chenharryhua.nanjin.devices

import java.io.InputStream
import java.net.URI

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}

import scala.collection.JavaConverters._

final class NJHadoop[F[_]: Sync: ContextShift](config: Configuration, blocker: Blocker) {

  private def fileSystem(pathStr: String): Resource[F, FileSystem] =
    Resource.fromAutoCloseableBlocking(blocker)(
      blocker.delay(FileSystem.get(new URI(pathStr), config)))

  private def fsOutput(pathStr: String): Resource[F, FSDataOutputStream] =
    for {
      fs <- fileSystem(pathStr)
      rs <- Resource.fromAutoCloseableBlocking(blocker)(blocker.delay(fs.create(new Path(pathStr))))
    } yield rs

  private def fsInput(pathStr: String): Resource[F, FSDataInputStream] =
    for {
      fs <- fileSystem(pathStr)
      rs <- Resource.fromAutoCloseableBlocking(blocker)(blocker.delay(fs.open(new Path(pathStr))))
    } yield rs

  def byteSink(pathStr: String): Pipe[F, Byte, Unit] = { (ss: Stream[F, Byte]) =>
    for {
      fs <- Stream.resource(fsOutput(pathStr))
      _ <- ss.through(writeOutputStream[F](Sync[F].pure(fs), blocker))
    } yield ()
  }

  def inputStream(pathStr: String): Stream[F, InputStream] =
    Stream.resource(fsInput(pathStr).widen)

  def byteStream(pathStr: String): Stream[F, Byte] =
    for {
      is <- inputStream(pathStr)
      bt <- readInputStream[F](Sync[F].pure(is), chunkSize, blocker)
    } yield bt

  def delete(pathStr: String): F[Boolean] =
    fileSystem(pathStr).use(fs => blocker.delay(fs.delete(new Path(pathStr), true)))

  /// parquet
  def parquetSink(pathStr: String, schema: Schema): Pipe[F, GenericRecord, Unit] = {
    (ss: Stream[F, GenericRecord]) =>
      val outputFile = HadoopOutputFile.fromPath(new Path(pathStr), config)
      for {
        writer <- Stream.resource(
          Resource.fromAutoCloseableBlocking(blocker)(
            blocker.delay(
              AvroParquetWriter
                .builder[GenericRecord](outputFile)
                .withConf(config)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withSchema(schema)
                .withDataModel(GenericData.get())
                .build())))
        _ <- ss.evalMap(a => blocker.delay(writer.write(a)))
      } yield ()
  }

  def parquetSource(pathStr: String): Stream[F, GenericRecord] = {
    def go(as: Stream[F, GenericRecord]): Pull[F, GenericRecord, Unit] =
      as.pull.uncons1.flatMap {
        case Some((h, tl)) =>
          Option(h) match {
            case None    => Pull.done
            case Some(v) => Pull.output1(v) >> go(tl)
          }
        case None => Pull.done
      }
    val inputFile = HadoopInputFile.fromPath(new Path(pathStr), config)
    for {
      reader <- Stream.resource(
        Resource.fromAutoCloseableBlocking(blocker)(
          blocker.delay(
            AvroParquetReader
              .builder[GenericRecord](inputFile)
              .withDataModel(GenericData.get())
              .build())))
      a <- go(Stream.repeatEval(blocker.delay(reader.read()))).stream
    } yield a
  }

  // avro data
  def avroSink(pathStr: String, schema: Schema): Pipe[F, GenericRecord, Unit] = {
    def go(
      grs: Stream[F, GenericRecord],
      writer: DataFileWriter[GenericRecord]): Pull[F, Unit, Unit] =
      grs.pull.uncons.flatMap {
        case Some((hl, tl)) =>
          Pull.eval(hl.traverse(gr => blocker.delay(writer.append(gr)))) >> go(tl, writer)
        case None => Pull.eval(blocker.delay(writer.close())) >> Pull.done
      }
    (ss: Stream[F, GenericRecord]) =>
      for {
        dfw <- Stream.resource(
          Resource.fromAutoCloseableBlocking[F, DataFileWriter[GenericRecord]](blocker)(
            blocker.delay(new DataFileWriter(new GenericDatumWriter(schema)))))
        writer <- Stream.resource(fsOutput(pathStr)).map(os => dfw.create(schema, os))
        _ <- go(ss, writer).stream
      } yield ()
  }

  def avroSource(pathStr: String): Stream[F, GenericRecord] =
    for {
      is <- Stream.resource(fsInput(pathStr))
      dfs <- Stream.resource(
        Resource.fromAutoCloseableBlocking[F, DataFileStream[GenericRecord]](blocker)(
          blocker.delay(new DataFileStream(is, new GenericDatumReader))))
      gr <- Stream.fromIterator(dfs.iterator().asScala)
    } yield gr
}
