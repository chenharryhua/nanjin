package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Resource, Sync}
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import squants.information.Information
import squants.information.InformationConversions.InformationConversions

import java.net.URI
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

sealed trait NJHadoop[F[_]] {

  def delete(path: NJPath): F[Boolean]
  def isExist(path: NJPath): F[Boolean]
  def locatedFileStatus(path: NJPath): F[List[LocatedFileStatus]]
  def dataFolders(path: NJPath): F[List[Path]]

  def byteSink(path: NJPath): Pipe[F, Byte, Unit]
  def byteSource(path: NJPath, byteBuffer: Information): Stream[F, Byte]
  def byteSource(path: NJPath): Stream[F, Byte]

  def avroSink(path: NJPath, schema: Schema, cf: CodecFactory): Pipe[F, GenericRecord, Unit]
  def avroSource(path: NJPath, schema: Schema, chunkSize: ChunkSize): Stream[F, GenericRecord]

  def akka: AkkaHadoop
}

object NJHadoop {

  def apply[F[_]](config: Configuration)(implicit F: Sync[F]): NJHadoop[F] =
    new NJHadoop[F] with Serializable {

      /** Notes: do not close file-system.
        */
      private def fileSystem(uri: URI): Resource[F, FileSystem] =
        Resource.make(F.blocking(FileSystem.get(uri, config)))(_ => F.pure(()))

      private def fsOutput(path: NJPath): Resource[F, FSDataOutputStream] =
        for {
          fs <- fileSystem(path.uri)
          rs <- Resource.make[F, FSDataOutputStream](F.blocking(fs.create(path.hadoopPath)))(r => F.blocking(r.close()))
        } yield rs

      private def fsInput(path: NJPath): Resource[F, FSDataInputStream] =
        for {
          fs <- fileSystem(path.uri)
          rs <- Resource.make[F, FSDataInputStream](F.blocking(fs.open(path.hadoopPath)))(r => F.blocking(r.close()))
        } yield rs

      override val akka: AkkaHadoop = new AkkaHadoop(config)

      // disk operations

      override def delete(path: NJPath): F[Boolean] =
        fileSystem(path.uri).use(fs => F.blocking(fs.delete(path.hadoopPath, true)))

      override def isExist(path: NJPath): F[Boolean] =
        fileSystem(path.uri).use(fs => F.blocking(fs.exists(path.hadoopPath)))

      override def locatedFileStatus(path: NJPath): F[List[LocatedFileStatus]] =
        fileSystem(path.uri).use { fs =>
          F.blocking {
            val ri = fs.listFiles(path.hadoopPath, true)
            val lb = ListBuffer.empty[LocatedFileStatus]
            while (ri.hasNext) lb.addOne(ri.next())
            lb.toList
          }
        }

      // folders which contain data files
      override def dataFolders(path: NJPath): F[List[Path]] =
        fileSystem(path.uri).use { fs =>
          F.blocking {
            val ri = fs.listFiles(path.hadoopPath, true)
            val lb = collection.mutable.Set.empty[Path]
            while (ri.hasNext) lb.addOne(ri.next().getPath.getParent)
            lb.toList.sortBy(_.toString)
          }
        }

      // pipes and sinks

      override def byteSink(path: NJPath): Pipe[F, Byte, Unit] = { (ss: Stream[F, Byte]) =>
        for {
          fsOut <- Stream.resource(fsOutput(path))
          r <- ss.through(writeOutputStream[F](F.blocking(fsOut)))
        } yield r
      }

      override def byteSource(path: NJPath, byteBuffer: Information): Stream[F, Byte] =
        for {
          fsIn <- Stream.resource(fsInput(path))
          bt <- readInputStream[F](F.blocking(fsIn), byteBuffer.toBytes.toInt)
        } yield bt

      // best performance
      override def byteSource(path: NJPath): Stream[F, Byte] = byteSource(path, 20.kb)

      override def avroSink(path: NJPath, schema: Schema, cf: CodecFactory): Pipe[F, GenericRecord, Unit] = {
        def go(grs: Stream[F, GenericRecord], writer: DataFileWriter[GenericRecord]): Pull[F, Unit, Unit] =
          grs.pull.uncons.flatMap {
            case Some((hl, tl)) => Pull.eval(F.blocking(hl.foreach(writer.append))) >> go(tl, writer)
            case None           => Pull.eval(F.blocking(writer.close())) >> Pull.done
          }

        (ss: Stream[F, GenericRecord]) =>
          for {
            dfw <- Stream.resource(
              Resource.make[F, DataFileWriter[GenericRecord]](
                F.blocking(new DataFileWriter(new GenericDatumWriter(schema)).setCodec(cf)))(r =>
                F.blocking(r.close())))
            writer <- Stream.resource(fsOutput(path)).map(os => dfw.create(schema, os))
            _ <- go(ss, writer).stream
          } yield ()
      }

      override def avroSource(path: NJPath, schema: Schema, chunkSize: ChunkSize): Stream[F, GenericRecord] =
        for {
          is <- Stream.resource(fsInput(path))
          dfs <- Stream.resource(
            Resource.make[F, DataFileStream[GenericRecord]](
              F.blocking(new DataFileStream(is, new GenericDatumReader(schema))))(r => F.blocking(r.close())))
          gr <- Stream.fromBlockingIterator(dfs.iterator().asScala, chunkSize.value)
        } yield gr
    }
}
