package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}

import java.net.URI
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

sealed trait NJHadoop[F[_]] {

  def delete(pathStr: String): F[Boolean]
  def isExist(pathStr: String): F[Boolean]
  def locatedFileStatus(pathStr: String): F[List[LocatedFileStatus]]
  def dataFolders(pathStr: String): F[List[Path]]

  def byteSink(pathStr: String): Pipe[F, Byte, Unit]
  def byteSource(pathStr: String, chunkSize: Int): Stream[F, Byte]

  def parquetSink(pathStr: String, schema: Schema, ccn: CompressionCodecName): Pipe[F, GenericRecord, Unit]
  def parquetSource(pathStr: String, schema: Schema): Stream[F, GenericRecord]

  def avroSink(pathStr: String, schema: Schema, cf: CodecFactory): Pipe[F, GenericRecord, Unit]
  def avroSource(pathStr: String, schema: Schema, chunkSize: Int): Stream[F, GenericRecord]
}

object NJHadoop {

  def apply[F[_]](config: Configuration)(implicit F: Sync[F]): NJHadoop[F] =
    new NJHadoop[F] with Serializable {

      /** Notes: do not close file-system.
        */
      private def fileSystem(pathStr: String): Resource[F, FileSystem] =
        Resource.make(F.blocking(FileSystem.get(new URI(pathStr), config)))(_ => F.pure(()))

      private def fsOutput(pathStr: String): Resource[F, FSDataOutputStream] =
        for {
          fs <- fileSystem(pathStr)
          rs <- Resource.make[F, FSDataOutputStream](F.blocking(fs.create(new Path(pathStr))))(r =>
            F.blocking(r.close()).attempt.void)
        } yield rs

      private def fsInput(pathStr: String): Resource[F, FSDataInputStream] =
        for {
          fs <- fileSystem(pathStr)
          rs <- Resource.make[F, FSDataInputStream](F.blocking(fs.open(new Path(pathStr))))(r =>
            F.blocking(r.close()).attempt.void)
        } yield rs

      override def delete(pathStr: String): F[Boolean] =
        fileSystem(pathStr).use(fs => F.blocking(fs.delete(new Path(pathStr), true)))

      override def isExist(pathStr: String): F[Boolean] =
        fileSystem(pathStr).use(fs => F.blocking(fs.exists(new Path(pathStr))))

      override def locatedFileStatus(pathStr: String): F[List[LocatedFileStatus]] =
        fileSystem(pathStr).use { fs =>
          F.blocking {
            val ri = fs.listFiles(new Path(pathStr), true)
            val lb = ListBuffer.empty[LocatedFileStatus]
            while (ri.hasNext) lb.addOne(ri.next())
            lb.toList
          }
        }

      // folders which contain data files
      override def dataFolders(pathStr: String): F[List[Path]] =
        fileSystem(pathStr).use { fs =>
          F.blocking {
            val ri = fs.listFiles(new Path(pathStr), true)
            val lb = collection.mutable.Set.empty[Path]
            while (ri.hasNext) lb.addOne(ri.next().getPath.getParent)
            lb.toList.sortBy(_.toString)
          }
        }

      override def byteSink(pathStr: String): Pipe[F, Byte, Unit] = { (ss: Stream[F, Byte]) =>
        (for {
          fs <- Stream.resource(fsOutput(pathStr))
          res <- ss.through(writeOutputStream[F](F.delay(fs)))
        } yield res).drain
      }

      override def byteSource(pathStr: String, chunkSize: Int): Stream[F, Byte] =
        for {
          is <- Stream.resource(fsInput(pathStr))
          bt <- readInputStream[F](F.delay(is), chunkSize)
        } yield bt

      override def parquetSink(
        pathStr: String,
        schema: Schema,
        ccn: CompressionCodecName): Pipe[F, GenericRecord, Unit] = {
        def go(grs: Stream[F, GenericRecord], writer: ParquetWriter[GenericRecord]): Pull[F, Unit, Unit] =
          grs.pull.uncons.flatMap {
            case Some((hl, tl)) =>
              Pull.eval(hl.traverse(gr => F.delay(writer.write(gr)))) >> go(tl, writer)
            case None =>
              Pull.eval(F.blocking(writer.close())) >> Pull.done
          }
        val outputFile = HadoopOutputFile.fromPath(new Path(pathStr), config)
        (ss: Stream[F, GenericRecord]) =>
          (for {
            writer <- Stream.resource(
              Resource.make(
                F.blocking(
                  AvroParquetWriter
                    .builder[GenericRecord](outputFile)
                    .withConf(config)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withCompressionCodec(ccn)
                    .withSchema(schema)
                    .withDataModel(GenericData.get())
                    .build()))(r => F.blocking(r.close()).attempt.void))
            _ <- go(ss, writer).stream
          } yield ()).drain
      }

      override def parquetSource(pathStr: String, schema: Schema): Stream[F, GenericRecord] = {
        val inputFile = HadoopInputFile.fromPath(new Path(pathStr), config)
        AvroReadSupport.setAvroReadSchema(config, schema)
        for {
          reader <- Stream.resource(
            Resource.make(
              F.blocking(
                AvroParquetReader
                  .builder[GenericRecord](inputFile)
                  .withDataModel(GenericData.get())
                  .withConf(config)
                  .build()))(r => F.blocking(r.close()).attempt.void))
          gr <- Stream.repeatEval(F.delay(Option(reader.read()))).unNoneTerminate
        } yield gr
      }

      override def avroSink(pathStr: String, schema: Schema, cf: CodecFactory): Pipe[F, GenericRecord, Unit] = {
        def go(grs: Stream[F, GenericRecord], writer: DataFileWriter[GenericRecord]): Pull[F, Unit, Unit] =
          grs.pull.uncons.flatMap {
            case Some((hl, tl)) =>
              Pull.eval(hl.traverse(gr => F.delay(writer.append(gr)))) >> go(tl, writer)
            case None => Pull.eval(F.blocking(writer.close())) >> Pull.done
          }
        (ss: Stream[F, GenericRecord]) =>
          (for {
            dfw <- Stream.resource(
              Resource.make[F, DataFileWriter[GenericRecord]](
                F.blocking(new DataFileWriter(new GenericDatumWriter(schema)).setCodec(cf)))(r =>
                F.blocking(r.close()).attempt.void))
            writer <- Stream.resource(fsOutput(pathStr)).map(os => dfw.create(schema, os))
            _ <- go(ss, writer).stream
          } yield ()).drain
      }

      override def avroSource(pathStr: String, schema: Schema, chunkSize: Int): Stream[F, GenericRecord] = for {
        is <- Stream.resource(fsInput(pathStr))
        dfs <- Stream.resource(
          Resource.make[F, DataFileStream[GenericRecord]](
            F.blocking(new DataFileStream(is, new GenericDatumReader(schema))))(r =>
            F.blocking(r.close()).attempt.void))
        gr <- Stream.fromIterator(dfs.iterator().asScala, chunkSize)
      } yield gr
    }
}
