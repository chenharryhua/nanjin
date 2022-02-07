package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import cats.syntax.functor.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile, HiddenFileFilter}

import java.io.{InputStream, OutputStream}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

object NJHadoop {

  def apply[F[_]: Sync](config: Configuration): NJHadoop[F] = new NJHadoop[F](config)
}

final class NJHadoop[F[_]] private (config: Configuration)(implicit F: Sync[F]) {

  val akka: AkkaHadoop = AkkaHadoop(config)

  // disk operations

  def delete(path: NJPath): F[Boolean] = F.blocking {
    val fs = path.hadoopPath.getFileSystem(config)
    fs.delete(path.hadoopPath, true)
  }

  def isExist(path: NJPath): F[Boolean] = F.blocking {
    val fs = path.hadoopPath.getFileSystem(config)
    fs.exists(path.hadoopPath)
  }

  def locatedFileStatus(path: NJPath): F[List[LocatedFileStatus]] = F.blocking {
    val fs = path.hadoopPath.getFileSystem(config)
    val ri = fs.listFiles(path.hadoopPath, true)
    val lb = ListBuffer.empty[LocatedFileStatus]
    while (ri.hasNext) lb.addOne(ri.next())
    lb.toList
  }

  // folders which contain data files
  def dataFolders(path: NJPath): F[List[Path]] = F.blocking {
    val fs = path.hadoopPath.getFileSystem(config)
    val ri = fs.listFiles(path.hadoopPath, true)
    val lb = collection.mutable.Set.empty[Path]
    while (ri.hasNext) lb.addOne(ri.next().getPath.getParent)
    lb.toList.sortBy(_.toString)
  }

  def hadoopInputFiles[A: Ordering](path: NJPath, sort: FileStatus => A): F[List[HadoopInputFile]] = F.blocking {
    val fs: FileSystem   = path.hadoopPath.getFileSystem(config)
    val stat: FileStatus = fs.getFileStatus(path.hadoopPath)
    if (stat.isFile)
      List(HadoopInputFile.fromStatus(stat, config))
    else
      fs.listStatus(path.hadoopPath, HiddenFileFilter.INSTANCE)
        .filter(_.isFile)
        .sortBy(sort(_))
        .map(HadoopInputFile.fromStatus(_, config))
        .toList
  }
  def hadoopInputFilesByTime(path: NJPath): F[List[HadoopInputFile]] = hadoopInputFiles(path, _.getModificationTime)
  def hadoopInputFilesByName(path: NJPath): F[List[HadoopInputFile]] = hadoopInputFiles(path, _.getPath.getName)

  // byte

  def byteSink(path: NJPath): Pipe[F, Byte, Unit] = byteSink(path, None)

  def byteSink(path: NJPath, compress: Option[ConfigurableCodec]): Pipe[F, Byte, Unit] =
    byteSink(HadoopOutputFile.fromPath(path.hadoopPath, config), compress)

  def byteSink(output: HadoopOutputFile, compress: Option[ConfigurableCodec]): Pipe[F, Byte, Unit] = {
    def compressOutputStream(stream: OutputStream): OutputStream =
      compress.fold(stream) { codec =>
        codec.setConf(config)
        codec.createOutputStream(stream)
      }

    (ss: Stream[F, Byte]) =>
      Stream
        .bracket(F.blocking(output.createOrOverwrite(output.defaultBlockSize())))(r => F.blocking(r.close()))
        .map(compressOutputStream)
        .flatMap(out => ss.through(writeOutputStream(F.pure(out))))
  }

  def byteSource(input: F[HadoopInputFile]): Stream[F, Byte] =
    for {
      hif <- Stream.eval(input)
      is: InputStream <- Stream.bracket(F.blocking(hif.newStream()))(r => F.blocking(r.close()))
      compressed: F[InputStream] = Option(new CompressionCodecFactory(config).getCodec(hif.getPath)).fold(F.pure(is))(
        factory => F.blocking(factory.createInputStream(is)))
      byte <- readInputStream[F](compressed, chunkSize = 8192, closeAfterUse = true)
    } yield byte

  def byteSource(input: HadoopInputFile): Stream[F, Byte] = byteSource(F.pure(input))
  def byteSource(path: NJPath): Stream[F, Byte] = byteSource(F.delay(HadoopInputFile.fromPath(path.hadoopPath, config)))

  // avro

  def avroSink(path: NJPath, schema: Schema, codecFactory: CodecFactory): Pipe[F, GenericRecord, Unit] =
    avroSink(HadoopOutputFile.fromPath(path.hadoopPath, config), schema, codecFactory)

  def avroSink(output: HadoopOutputFile, schema: Schema, codecFactory: CodecFactory): Pipe[F, GenericRecord, Unit] = {
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

  def avroSource(path: NJPath, schema: Schema, chunkSize: ChunkSize): Stream[F, GenericRecord] =
    avroSource(F.delay(HadoopInputFile.fromPath(path.hadoopPath, config)), schema, chunkSize)

  def avroSource(input: HadoopInputFile, schema: Schema, chunkSize: ChunkSize): Stream[F, GenericRecord] =
    avroSource(F.pure(input), schema, chunkSize)

  def avroSource(input: F[HadoopInputFile], schema: Schema, chunkSize: ChunkSize): Stream[F, GenericRecord] =
    for {
      is <- Stream.bracket(input.map(_.newStream()))(r => F.blocking(r.close()))
      dfs <- Stream.bracket[F, DataFileStream[GenericRecord]](
        F.blocking(new DataFileStream(is, new GenericDatumReader(schema))))(r => F.blocking(r.close()))
      gr <- Stream.fromBlockingIterator(dfs.iterator().asScala, chunkSize.value)
    } yield gr

}
