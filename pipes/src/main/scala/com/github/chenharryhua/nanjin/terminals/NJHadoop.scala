package com.github.chenharryhua.nanjin.terminals

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.syntax.all._
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}

import java.net.URI
import scala.collection.JavaConverters._

sealed trait NJHadoop[F[_]] {

  def delete(pathStr: String): F[Boolean]
  def isExist(pathStr: String): F[Boolean]

  def byteSink(pathStr: String): Pipe[F, Byte, Unit]
  def byteSource(pathStr: String): Stream[F, Byte]

  def parquetSink(pathStr: String, schema: Schema, ccn: CompressionCodecName): Pipe[F, GenericRecord, Unit]

  def parquetSource(pathStr: String, schema: Schema): Stream[F, GenericRecord]

  def avroSink(pathStr: String, schema: Schema, cf: CodecFactory): Pipe[F, GenericRecord, Unit]
  def avroSource(pathStr: String, schema: Schema): Stream[F, GenericRecord]
}

object NJHadoop {

  def apply[F[_]: ContextShift](config: Configuration, blocker: Blocker)(implicit F: Sync[F]): NJHadoop[F] =
    new NJHadoop[F] with Serializable {

      /** Notes: do not close file-system.
        */
      private def fileSystem(pathStr: String): Resource[F, FileSystem] =
        Resource.make(blocker.delay(FileSystem.get(new URI(pathStr), config)))(_ => blocker.delay(()))

      private def fsOutput(pathStr: String): Resource[F, FSDataOutputStream] =
        for {
          fs <- fileSystem(pathStr)
          rs <- Resource.fromAutoCloseableBlocking[F, FSDataOutputStream](blocker)(
            blocker.delay(fs.create(new Path(pathStr))))
        } yield rs

      private def fsInput(pathStr: String): Resource[F, FSDataInputStream] =
        for {
          fs <- fileSystem(pathStr)
          rs <- Resource.fromAutoCloseableBlocking[F, FSDataInputStream](blocker)(
            blocker.delay(fs.open(new Path(pathStr))))
        } yield rs

      override def delete(pathStr: String): F[Boolean] =
        fileSystem(pathStr).use(fs => blocker.delay(fs.delete(new Path(pathStr), true)))

      override def isExist(pathStr: String): F[Boolean] =
        fileSystem(pathStr).use(fs => blocker.delay(fs.exists(new Path(pathStr))))

      override def byteSink(pathStr: String): Pipe[F, Byte, Unit] = { (ss: Stream[F, Byte]) =>
        for {
          fs <- Stream.resource(fsOutput(pathStr))
          _ <- ss.through(writeOutputStream[F](blocker.delay(fs), blocker))
        } yield ()
      }

      override def byteSource(pathStr: String): Stream[F, Byte] =
        for {
          is <- Stream.resource(fsInput(pathStr))
          bt <- readInputStream[F](blocker.delay(is), chunkSize, blocker)
        } yield bt

      override def parquetSink(
        pathStr: String,
        schema: Schema,
        ccn: CompressionCodecName): Pipe[F, GenericRecord, Unit] = {
        def go(grs: Stream[F, GenericRecord], writer: ParquetWriter[GenericRecord]): Pull[F, Unit, Unit] =
          grs.pull.uncons.flatMap {
            case Some((hl, tl)) =>
              Pull.eval(hl.traverse(gr => blocker.delay(writer.write(gr)))) >> go(tl, writer)
            case None =>
              Pull.eval(blocker.delay(writer.close())) >> Pull.done
          }
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
                    .withCompressionCodec(ccn)
                    .withSchema(schema)
                    .withDataModel(GenericData.get())
                    .build())))
            _ <- go(ss, writer).stream
          } yield ()
      }

      override def parquetSource(pathStr: String, schema: Schema): Stream[F, GenericRecord] = {
        val inputFile = HadoopInputFile.fromPath(new Path(pathStr), config)
        AvroReadSupport.setAvroReadSchema(config, schema)
        for {
          reader <- Stream.resource(
            Resource.fromAutoCloseableBlocking(blocker)(
              blocker.delay(
                AvroParquetReader
                  .builder[GenericRecord](inputFile)
                  .withDataModel(GenericData.get())
                  .withConf(config)
                  .build())))
          gr <- Stream.repeatEval(blocker.delay(Option(reader.read()))).unNoneTerminate
        } yield gr
      }

      override def avroSink(pathStr: String, schema: Schema, cf: CodecFactory): Pipe[F, GenericRecord, Unit] = {
        def go(grs: Stream[F, GenericRecord], writer: DataFileWriter[GenericRecord]): Pull[F, Unit, Unit] =
          grs.pull.uncons.flatMap {
            case Some((hl, tl)) =>
              Pull.eval(hl.traverse(gr => blocker.delay(writer.append(gr)))) >> go(tl, writer)
            case None => Pull.eval(blocker.delay(writer.close())) >> Pull.done
          }
        (ss: Stream[F, GenericRecord]) =>
          for {
            dfw <- Stream.resource(
              Resource.fromAutoCloseableBlocking[F, DataFileWriter[GenericRecord]](blocker)(
                blocker.delay(new DataFileWriter(new GenericDatumWriter(schema)).setCodec(cf))))
            writer <- Stream.resource(fsOutput(pathStr)).map(os => dfw.create(schema, os))
            _ <- go(ss, writer).stream
          } yield ()
      }

      override def avroSource(pathStr: String, schema: Schema): Stream[F, GenericRecord] = for {
        is <- Stream.resource(fsInput(pathStr))
        dfs <- Stream.resource(
          Resource.fromAutoCloseableBlocking[F, DataFileStream[GenericRecord]](blocker)(
            blocker.delay(new DataFileStream(is, new GenericDatumReader(schema)))))
        gr <- Stream.fromIterator(dfs.iterator().asScala)
      } yield gr
    }
}
