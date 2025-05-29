package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.kernel.{Resource, Sync}
import fs2.Chunk
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{Encoder, EncoderFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.PositionOutputStream
import scalapb.GeneratedMessage

import java.io.{OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

sealed private trait HadoopWriter[F[_], A] {
  def write(ck: Chunk[A]): F[Unit]
}

private object HadoopWriter {
  final private[this] val BLOCK_SIZE_HINT: Long = -1

  def avroR[F[_]](codecFactory: CodecFactory, schema: Schema, configuration: Configuration, path: Path)(
    implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    Resource
      .make[F, DataFileWriter[GenericRecord]](F.blocking {
        val dfw: DataFileWriter[GenericRecord] =
          new DataFileWriter(new GenericDatumWriter[GenericRecord](schema)).setCodec(codecFactory)
        val pos: PositionOutputStream =
          HadoopOutputFile.fromPath(path, configuration).createOrOverwrite(BLOCK_SIZE_HINT)
        dfw.create(schema, pos)
      })(r => F.blocking(r.close()))
      .map { (dfw: DataFileWriter[GenericRecord]) =>
        new HadoopWriter[F, GenericRecord] {
          override def write(cgr: Chunk[GenericRecord]): F[Unit] =
            F.blocking {
              cgr.foreach(dfw.append)
              dfw.flush()
            }
        }
      }

  def parquetR[F[_]](writeBuilder: Reader[Path, AvroParquetWriter.Builder[GenericRecord]], path: Path)(
    implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    Resource
      .make(F.blocking(writeBuilder.run(path).build()))(r => F.blocking(r.close()))
      .map((pw: ParquetWriter[GenericRecord]) =>
        new HadoopWriter[F, GenericRecord] {
          override def write(cgr: Chunk[GenericRecord]): F[Unit] =
            F.blocking(cgr.foreach(pw.write))
        })

  private def fileOutputStream(path: Path, configuration: Configuration): OutputStream = {
    val os: OutputStream = HadoopOutputFile.fromPath(path, configuration).createOrOverwrite(BLOCK_SIZE_HINT)
    Option(new CompressionCodecFactory(configuration).getCodec(path)) match {
      case Some(cc) => cc.createOutputStream(os)
      case None     => os
    }
  }

  def outputStreamR[F[_]](path: Path, configuration: Configuration)(implicit
    F: Sync[F]): Resource[F, OutputStream] =
    Resource.make(F.blocking(fileOutputStream(path, configuration)))(r => F.blocking(r.close()))

  def byteR[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, Byte]] =
    outputStreamR(path, configuration).map(os =>
      new HadoopWriter[F, Byte] {
        override def write(cb: Chunk[Byte]): F[Unit] =
          F.blocking {
            os.write(cb.toArray)
            os.flush()
          }
      })

  private def outputStreamWriterR[F[_]](path: Path, configuration: Configuration)(implicit
    F: Sync[F]): Resource[F, OutputStreamWriter] =
    Resource.make(
      F.blocking(new OutputStreamWriter(fileOutputStream(path, configuration), StandardCharsets.UTF_8)))(r =>
      F.blocking(r.close()))

  def stringR[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, String]] =
    outputStreamWriterR(path, configuration).map(writer =>
      new HadoopWriter[F, String] {
        override def write(cs: Chunk[String]): F[Unit] =
          F.blocking {
            cs.foreach { s =>
              writer.write(s)
              writer.write(System.lineSeparator())
            }
            writer.flush()
          }
      })

  def csvStringR[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, String]] =
    outputStreamWriterR(path, configuration).map(writer =>
      new HadoopWriter[F, String] {
        override def write(cs: Chunk[String]): F[Unit] =
          // already has new line separator
          F.blocking {
            cs.foreach(s => writer.write(s))
            writer.flush()
          }
      })

  def protobufR[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, GeneratedMessage]] =
    outputStreamR(path, configuration).map { os =>
      new HadoopWriter[F, GeneratedMessage] {
        override def write(cgm: Chunk[GeneratedMessage]): F[Unit] =
          F.delay {
            cgm.foreach(_.writeDelimitedTo(os))
            os.flush()
          }
      }
    }

  private def genericRecordWriterR[F[_]](
    getEncoder: OutputStream => Encoder,
    configuration: Configuration,
    schema: Schema,
    path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    outputStreamR(path, configuration).map { os =>
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val encoder     = getEncoder(os)
      new HadoopWriter[F, GenericRecord] {
        override def write(cgr: Chunk[GenericRecord]): F[Unit] =
          F.blocking {
            cgr.foreach(gr => datumWriter.write(gr, encoder))
            encoder.flush()
          }
      }
    }

  def jacksonR[F[_]](configuration: Configuration, schema: Schema, path: Path)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    genericRecordWriterR[F](
      (os: OutputStream) => EncoderFactory.get().jsonEncoder(schema, os),
      configuration,
      schema,
      path)

  def binAvroR[F[_]](configuration: Configuration, schema: Schema, path: Path)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    genericRecordWriterR[F](
      (os: OutputStream) => EncoderFactory.get().binaryEncoder(os, null),
      configuration,
      schema,
      path)

}
