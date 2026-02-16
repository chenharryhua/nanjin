package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.kernel.{Resource, Sync}
import com.fasterxml.jackson.databind.{JsonNode, ObjectWriter}
import fs2.Chunk
import io.circe.{Json, Printer}
import io.lemonlabs.uri.Url
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{Encoder, EncoderFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import scalapb.GeneratedMessage

import java.io.{OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

sealed private trait HadoopWriter[F[_], A] {
  def fileUrl: Url
  def write(ck: Chunk[A]): F[Unit]
}

private object HadoopWriter {

  def avroR[F[_]](codecFactory: CodecFactory, schema: Schema, configuration: Configuration, url: Url)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    Resource
      .fromAutoCloseable(F.blocking {
        val path: Path = toHadoopPath(url)
        val dfw: DataFileWriter[GenericRecord] =
          new DataFileWriter(new GenericDatumWriter[GenericRecord](schema)).setCodec(codecFactory)
        val os: FSDataOutputStream = path.getFileSystem(configuration).create(path, true)
        dfw.create(schema, os)
      })
      .map { (dfw: DataFileWriter[GenericRecord]) =>
        new HadoopWriter[F, GenericRecord] {
          override val fileUrl: Url = url
          override def write(cgr: Chunk[GenericRecord]): F[Unit] =
            F.blocking {
              cgr.foreach(dfw.append)
              dfw.flush()
            }
        }
      }

  def parquetR[F[_]](writeBuilder: Reader[Url, AvroParquetWriter.Builder[GenericRecord]], url: Url)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    Resource
      .fromAutoCloseable(F.blocking(writeBuilder.run(url).build()))
      .map((pw: ParquetWriter[GenericRecord]) =>
        new HadoopWriter[F, GenericRecord] {
          override val fileUrl: Url = url
          override def write(cgr: Chunk[GenericRecord]): F[Unit] =
            F.blocking(cgr.foreach(pw.write))
        })

  /*
   * output stream based
   */

  private[this] def fileOutputStream(configuration: Configuration, url: Url): OutputStream = {
    val path: Path = toHadoopPath(url)
    val os: FSDataOutputStream = path.getFileSystem(configuration).create(path, true)
    Option(new CompressionCodecFactory(configuration).getCodec(path)) match {
      case Some(cc) => cc.createOutputStream(os)
      case None     => os
    }
  }

  private def outputStreamR[F[_]](configuration: Configuration, url: Url)(implicit
    F: Sync[F]): Resource[F, OutputStream] =
    Resource.fromAutoCloseable(F.blocking(fileOutputStream(configuration, url)))

  def byteR[F[_]](configuration: Configuration, url: Url)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, Byte]] =
    outputStreamR[F](configuration, url).map(os =>
      new HadoopWriter[F, Byte] {
        override val fileUrl: Url = url
        override def write(cb: Chunk[Byte]): F[Unit] =
          F.blocking {
            os.write(cb.toArray)
            os.flush()
          }
      })

  def protobufR[F[_]](configuration: Configuration, url: Url)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, GeneratedMessage]] =
    outputStreamR[F](configuration, url).map { os =>
      new HadoopWriter[F, GeneratedMessage] {
        override val fileUrl: Url = url
        override def write(cgm: Chunk[GeneratedMessage]): F[Unit] =
          F.blocking {
            cgm.foreach(_.writeDelimitedTo(os))
            os.flush()
          }
      }
    }

  private def genericRecordWriterR[F[_]](
    getEncoder: OutputStream => Encoder,
    configuration: Configuration,
    schema: Schema,
    url: Url)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    outputStreamR[F](configuration, url).map { os =>
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val encoder = getEncoder(os)
      new HadoopWriter[F, GenericRecord] {
        override val fileUrl: Url = url
        override def write(cgr: Chunk[GenericRecord]): F[Unit] =
          F.blocking {
            cgr.foreach(gr => datumWriter.write(gr, encoder))
            encoder.flush()
          }
      }
    }

  def jacksonR[F[_]](configuration: Configuration, schema: Schema, url: Url)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    genericRecordWriterR[F](
      (os: OutputStream) => EncoderFactory.get().jsonEncoder(schema, os),
      configuration,
      schema,
      url)

  def binAvroR[F[_]](configuration: Configuration, schema: Schema, url: Url)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    genericRecordWriterR[F](
      (os: OutputStream) => EncoderFactory.get().binaryEncoder(os, null),
      configuration,
      schema,
      url)

  def jsonNodeR[F[_]](configuration: Configuration, url: Url)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, JsonNode]] =
    outputStreamR[F](configuration, url).map { os =>
      val writer: ObjectWriter = objectMapper.writer()
      new HadoopWriter[F, JsonNode] {
        override val fileUrl: Url = url
        override def write(cjn: Chunk[JsonNode]): F[Unit] =
          F.blocking {
            cjn.foreach { jn =>
              os.write(writer.writeValueAsBytes(jn))
              os.write('\n')
            }
            os.flush()
          }
      }
    }

  /*
   * output stream writer based
   */

  private def outputStreamWriterR[F[_]](configuration: Configuration, url: Url)(implicit
    F: Sync[F]): Resource[F, OutputStreamWriter] =
    Resource.fromAutoCloseable(
      F.blocking(new OutputStreamWriter(fileOutputStream(configuration, url), StandardCharsets.UTF_8)))

  def stringR[F[_]](configuration: Configuration, url: Url)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, String]] =
    outputStreamWriterR[F](configuration, url).map(writer =>
      new HadoopWriter[F, String] {
        override val fileUrl: Url = url
        override def write(cs: Chunk[String]): F[Unit] =
          F.blocking {
            cs.foreach { s =>
              writer.write(s)
              writer.write(System.lineSeparator())
            }
            writer.flush()
          }
      })

  def csvStringR[F[_]](configuration: Configuration, url: Url)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, String]] =
    outputStreamWriterR[F](configuration, url).map(writer =>
      new HadoopWriter[F, String] {
        override val fileUrl: Url = url
        override def write(cs: Chunk[String]): F[Unit] =
          // already has new line separator
          F.blocking {
            cs.foreach(s => writer.write(s))
            writer.flush()
          }
      })

  def circeR[F[_]](configuration: Configuration, url: Url)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, Json]] =
    outputStreamWriterR[F](configuration, url).map { writer =>
      val printer = Printer.noSpaces
      new HadoopWriter[F, Json] {
        override val fileUrl: Url = url
        override def write(cs: Chunk[Json]): F[Unit] =
          F.blocking {
            cs.foreach { json =>
              printer.unsafePrintToAppendable(json, writer)
              writer.write(System.lineSeparator())
            }
            writer.flush()
          }
      }
    }
}
