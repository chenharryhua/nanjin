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
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile, HiddenFileFilter}

import java.net.URI
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

sealed trait NJHadoop[F[_]] {

  def delete(path: NJPath): F[Boolean]
  def isExist(path: NJPath): F[Boolean]
  def locatedFileStatus(path: NJPath): F[List[LocatedFileStatus]]
  def dataFolders(path: NJPath): F[List[Path]]
  def hadoopInputFiles(path: NJPath): F[List[HadoopInputFile]]

  def byteSink(path: NJPath): Pipe[F, Byte, Unit]
  def byteSink(output: HadoopOutputFile): Pipe[F, Byte, Unit]

  def byteSource(path: NJPath): Stream[F, Byte]
  def byteSource(path: HadoopInputFile): Stream[F, Byte]

  def avroSink(path: NJPath, schema: Schema, cf: CodecFactory): Pipe[F, GenericRecord, Unit]
  def avroSink(path: HadoopOutputFile, schema: Schema, cf: CodecFactory): Pipe[F, GenericRecord, Unit]

  def avroSource(path: NJPath, schema: Schema, chunkSize: ChunkSize): Stream[F, GenericRecord]
  def avroSource(path: HadoopInputFile, schema: Schema, chunkSize: ChunkSize): Stream[F, GenericRecord]

  def akka: AkkaHadoop
}

object NJHadoop {

  def apply[F[_]](config: Configuration)(implicit F: Sync[F]): NJHadoop[F] =
    new NJHadoop[F] with Serializable {

      /** Notes: do not close file-system.
        */
      private def fileSystem(uri: URI): Resource[F, FileSystem] =
        Resource.make(F.blocking(FileSystem.get(uri, config)))(_ => F.pure(()))

      override val akka: AkkaHadoop = AkkaHadoop(config)

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

      def hadoopInputFiles(path: NJPath): F[List[HadoopInputFile]] = F.blocking {
        val fs: FileSystem   = path.hadoopPath.getFileSystem(config)
        val stat: FileStatus = fs.getFileStatus(path.hadoopPath)
        if (stat.isFile)
          List(HadoopInputFile.fromStatus(stat, config))
        else
          fs.listStatus(path.hadoopPath, HiddenFileFilter.INSTANCE)
            .filter(_.isFile)
            .sortBy(_.getModificationTime)
            .map(HadoopInputFile.fromStatus(_, config))
            .toList
      }

      // pipes and sinks

      override def byteSink(path: NJPath): Pipe[F, Byte, Unit] =
        byteSink(HadoopOutputFile.fromPath(path.hadoopPath, config))

      override def byteSink(output: HadoopOutputFile): Pipe[F, Byte, Unit] = { (ss: Stream[F, Byte]) =>
        Stream
          .bracket(F.blocking(output.createOrOverwrite(output.defaultBlockSize())))(r => F.blocking(r.close()))
          .flatMap(out => ss.through(writeOutputStream(F.pure(out))))
      }

      override def byteSource(input: HadoopInputFile): Stream[F, Byte] =
        Stream
          .bracket(F.blocking(input.newStream()))(r => F.blocking(r.close()))
          .flatMap(in => readInputStream[F](F.pure(in), 8192))

      override def byteSource(path: NJPath): Stream[F, Byte] =
        byteSource(HadoopInputFile.fromPath(path.hadoopPath, config))

      override def avroSink(path: NJPath, schema: Schema, codecFactory: CodecFactory): Pipe[F, GenericRecord, Unit] =
        avroSink(HadoopOutputFile.fromPath(path.hadoopPath, config), schema, codecFactory)

      override def avroSink(
        output: HadoopOutputFile,
        schema: Schema,
        codecFactory: CodecFactory): Pipe[F, GenericRecord, Unit] = {
        def go(grs: Stream[F, GenericRecord], writer: DataFileWriter[GenericRecord]): Pull[F, Unit, Unit] =
          grs.pull.uncons.flatMap {
            case Some((hl, tl)) => Pull.eval(F.blocking(hl.foreach(writer.append))) >> go(tl, writer)
            case None           => Pull.eval(F.blocking(writer.close())) >> Pull.done
          }

        (ss: Stream[F, GenericRecord]) =>
          for {
            dfw <- Stream.bracket[F, DataFileWriter[GenericRecord]](
              F.blocking(new DataFileWriter(new GenericDatumWriter(schema)).setCodec(codecFactory)))(r =>
              F.blocking(r.close()))
            os <- Stream.bracket(F.blocking(output.createOrOverwrite(output.defaultBlockSize())))(r =>
              F.blocking(r.close()))
            writer <- Stream.bracket(F.blocking(dfw.create(schema, os)))(r => F.blocking(r.close()))
            _ <- go(ss, writer).stream
          } yield ()

      }

      override def avroSource(path: NJPath, schema: Schema, chunkSize: ChunkSize): Stream[F, GenericRecord] =
        avroSource(HadoopInputFile.fromPath(path.hadoopPath, config), schema, chunkSize)

      override def avroSource(input: HadoopInputFile, schema: Schema, chunkSize: ChunkSize): Stream[F, GenericRecord] =
        for {
          is <- Stream.bracket(F.blocking(input.newStream()))(r => F.blocking(r.close()))
          dfs <- Stream.bracket[F, DataFileStream[GenericRecord]](
            F.blocking(new DataFileStream(is, new GenericDatumReader(schema))))(r => F.blocking(r.close()))
          gr <- Stream.fromBlockingIterator(dfs.iterator().asScala, chunkSize.value)
        } yield gr

    }
}
