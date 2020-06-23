package com.github.chenharryhua.nanjin.devices

import java.io.InputStream
import java.net.URI

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroInputStreamBuilder,
  AvroOutputStream,
  Record,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}

final class NJHadoop[F[_]: Sync: ContextShift](config: Configuration, blocker: Blocker) {

  private def fileSystem(pathStr: String): Resource[F, FileSystem] =
    Resource.fromAutoCloseable(blocker.delay(FileSystem.get(new URI(pathStr), config)))

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
  def parquetSink[A: AvroEncoder](pathStr: String): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    val outputFile = HadoopOutputFile.fromPath(new Path(pathStr), config)
    for {
      writer <- Stream.resource(
        Resource.fromAutoCloseable(
          blocker.delay(
            AvroParquetWriter
              .builder[GenericRecord](outputFile)
              .withConf(config)
              .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
              .withCompressionCodec(CompressionCodecName.SNAPPY)
              .withSchema(AvroEncoder[A].schema)
              .withDataModel(GenericData.get())
              .build())))
      _ <- ss.evalMap { a =>
        blocker.delay(writer.write(AvroEncoder[A].encode(a) match {
          case record: Record => record
          case output         => sys.error(s"Cannot marshall $output")
        }))
      }
    } yield ()
  }

  def parquetSource[A: AvroDecoder](pathStr: String): Stream[F, A] = {
    val inputFile = HadoopInputFile.fromPath(new Path(pathStr), config)
    def go(as: Stream[F, GenericRecord]): Pull[F, A, Unit] =
      as.pull.uncons1.flatMap {
        case Some((h, tl)) =>
          Option(h) match {
            case None    => Pull.done
            case Some(v) => Pull.output1(AvroDecoder[A].decode(v)) >> go(tl)
          }
        case None => Pull.done
      }
    for {
      reader <- Stream.resource(
        Resource.fromAutoCloseable(
          blocker.delay(
            AvroParquetReader
              .builder[GenericRecord](inputFile)
              .withDataModel(GenericData.get())
              .build())))
      a <- go(Stream.repeatEval(blocker.delay(reader.read()))).stream
    } yield a
  }

  //avro data
  def avroSink[A: AvroEncoder](pathStr: String): Pipe[F, A, Unit] = { (ss: Stream[F, A]) =>
    def go(as: Stream[F, A], aos: AvroOutputStream[A]): Pull[F, Unit, Unit] =
      as.pull.uncons.flatMap {
        case Some((hl, tl)) =>
          Pull.eval(hl.traverse(a => blocker.delay(aos.write(a)))) >> go(tl, aos)
        case None => Pull.eval(blocker.delay(aos.close)) >> Pull.done
      }
    for {
      aos <- Stream.resource(fsOutput(pathStr).map(os => AvroOutputStream.data[A].to(os).build()))
      _ <- go(ss, aos).stream
    } yield ()
  }

  def avroSource[A: AvroDecoder](pathStr: String): Stream[F, A] = {
    val schema: Schema                  = AvroDecoder[A].schema
    val aisb: AvroInputStreamBuilder[A] = AvroInputStream.data[A]
    for {
      is <- Stream.resource(fsInput(pathStr))
      a <- Stream.fromIterator(aisb.from(is).build(schema).iterator)
    } yield a
  }
}
