package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.kernel.{Resource, Sync}
import com.fasterxml.jackson.databind.{JsonNode, ObjectWriter}
import fs2.Chunk
import io.circe.{Json, Printer}
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration.Header
import kantan.csv.{CsvConfiguration, CsvWriter}
import kantan.csv.engine.WriterEngine.internalCsvWriterEngine
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

/** A blocking sink over a single Hadoop file: `write` appends one fs2 `Chunk` of `A` to the open file.
  *
  * Instances are obtained from the `*R` builders in the companion object, each of which yields a
  * `Resource[F, HadoopWriter[F, A]]` so the underlying stream/writer is flushed and closed on release. A
  * `write` typically flushes at the end of the chunk, so a chunk is durable once its effect completes, but
  * the file is only truly finalized when the owning `Resource` is released.
  */
sealed private trait HadoopWriter[F[_], A] {
  def write(ck: Chunk[A]): F[Unit]
}

/** Internal collection of blocking Hadoop writers, mirroring the readers in `HadoopReader`.
  *
  * Shared shape across every builder:
  *   - all I/O runs inside `F.blocking`;
  *   - the target file is (re)created with `overwrite = true`, so an existing file at `url` is replaced;
  *   - the sink is wrapped in `Resource.fromAutoCloseable`, guaranteeing close on release even under
  *     cancellation or error;
  *   - `url` is resolved via `toHadoopPath` (rewriting the `s3` scheme to `s3a`);
  *   - for the output-stream and writer-based sinks, compression is chosen from the path extension by
  *     `CompressionCodecFactory`, so naming the file with a codec suffix transparently compresses the output.
  *     The Avro (`avroR`) and Parquet (`parquetR`) sinks handle their own codecs instead.
  *
  * The `R` suffix marks these as `Resource`-valued builders, paired with the `S` (`Stream`) readers in
  * `HadoopReader`.
  */
private object HadoopWriter {

  /** Write an Avro object-container file with the schema embedded in the header.
    *
    * `DataFileWriter` is configured with `codecFactory` (its own block compression, independent of any
    * path-extension codec) and creates the file over a freshly created `FSDataOutputStream`. Each `write`
    * appends the chunk's records and flushes so the block is pushed to the stream.
    */
  def avroR[F[_]](codecFactory: CodecFactory, schema: Schema, configuration: Configuration, url: Url)(using
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
          override def write(cgr: Chunk[GenericRecord]): F[Unit] =
            F.blocking {
              cgr.foreach(dfw.append)
              dfw.flush()
            }
        }
      }

  /** Write a Parquet file from Avro `GenericRecord`s.
    *
    * `writeBuilder` turns `url` into a configured `AvroParquetWriter.Builder` (schema, compression, row-group
    * settings, Hadoop `Configuration`), so the caller controls the Parquet layout. Parquet buffers rows
    * internally and writes row groups on its own schedule, so `write` does not flush per chunk; the trailing
    * data and footer are written when the `Resource` closes the `ParquetWriter`.
    */
  def parquetR[F[_]](writeBuilder: Reader[Url, AvroParquetWriter.Builder[GenericRecord]], url: Url)(using
    F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    Resource
      .fromAutoCloseable(F.blocking(writeBuilder.run(url).build()))
      .map((pw: ParquetWriter[GenericRecord]) =>
        new HadoopWriter[F, GenericRecord] {
          override def write(cgr: Chunk[GenericRecord]): F[Unit] =
            F.blocking(cgr.foreach(pw.write))
        })

  /*
   * output stream based
   */

  /** Create (overwriting) the file at `url` and return its `OutputStream`, wrapping it in a compressing
    * stream when the path extension names a codec. Blocking; callers wrap it in `Resource`/`F.blocking`.
    */
  private def file_output_stream(configuration: Configuration, url: Url): OutputStream = {
    val path: Path = toHadoopPath(url)
    val os: FSDataOutputStream = path.getFileSystem(configuration).create(path, true)
    Option(new CompressionCodecFactory(configuration).getCodec(path)) match {
      case Some(cc) => cc.createOutputStream(os)
      case None     => os
    }
  }

  /** `file_output_stream` acquired as an auto-closeable `Resource`; the shared entry point for the
    * output-stream-based sinks below.
    */
  private def outputStreamR[F[_]](configuration: Configuration, url: Url)(using
    F: Sync[F]): Resource[F, OutputStream] =
    Resource.fromAutoCloseable(F.blocking(file_output_stream(configuration, url)))

  /** Write raw bytes: each chunk is written to the output stream and flushed. */
  def byteR[F[_]](configuration: Configuration, url: Url)(using
    F: Sync[F]): Resource[F, HadoopWriter[F, Byte]] =
    outputStreamR[F](configuration, url).map(os =>
      new HadoopWriter[F, Byte] {
        override def write(cb: Chunk[Byte]): F[Unit] =
          F.blocking {
            os.write(cb.toArray)
            os.flush()
          }
      })

  /** Write Protobuf messages in length-delimited framing via `writeDelimitedTo`, matching what
    * `HadoopReader.protobufS` expects. Each chunk is written then flushed.
    */
  def protobufR[F[_]](configuration: Configuration, url: Url)(using
    F: Sync[F]): Resource[F, HadoopWriter[F, GeneratedMessage]] =
    outputStreamR[F](configuration, url).map { os =>
      new HadoopWriter[F, GeneratedMessage] {
        override def write(cgm: Chunk[GeneratedMessage]): F[Unit] =
          F.blocking {
            cgm.foreach(_.writeDelimitedTo(os))
            os.flush()
          }
      }
    }

  /** Shared engine for the schema-driven Avro writers (`jacksonR` and `binAvroR`).
    *
    * Builds a `GenericDatumWriter` for `schema` and an `Encoder` produced by `get_encoder` (JSON or binary
    * Avro) over the file's output stream. Each `write` encodes every record in the chunk and flushes the
    * encoder. Unlike `avroR` this writes bare records with no object-container header, so the reader must be
    * told the schema out of band (see `HadoopReader.jacksonS`/`binAvroS`).
    */
  private def genericRecordWriterR[F[_]](
    get_encoder: OutputStream => Encoder,
    configuration: Configuration,
    schema: Schema,
    url: Url)(using F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    outputStreamR[F](configuration, url).map { os =>
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val encoder = get_encoder(os)
      new HadoopWriter[F, GenericRecord] {
        override def write(cgr: Chunk[GenericRecord]): F[Unit] =
          F.blocking {
            cgr.foreach(gr => datumWriter.write(gr, encoder))
            encoder.flush()
          }
      }
    }

  /** Write Avro records as Avro JSON (`jsonEncoder`), the schema-shaped textual form read back by
    * `HadoopReader.jacksonS`.
    */
  def jacksonR[F[_]](configuration: Configuration, schema: Schema, url: Url)(using
    F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    genericRecordWriterR[F](
      (os: OutputStream) => EncoderFactory.get().jsonEncoder(schema, os),
      configuration,
      schema,
      url)

  /** Write bare binary Avro records (no container header), read back by `HadoopReader.binAvroS`. The `null`
    * argument lets Avro allocate a fresh `BinaryEncoder`.
    */
  def binAvroR[F[_]](configuration: Configuration, schema: Schema, url: Url)(using
    F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    genericRecordWriterR[F](
      (os: OutputStream) => EncoderFactory.get().binaryEncoder(os, null),
      configuration,
      schema,
      url)

  /** Write Jackson `JsonNode`s as newline-delimited JSON: each node is serialized with `objectWriter` and
    * followed by a `'\n'`, so one JSON document is emitted per line.
    */
  def jsonNodeR[F[_]](configuration: Configuration, url: Url, objectWriter: ObjectWriter)(using
    F: Sync[F]): Resource[F, HadoopWriter[F, JsonNode]] =
    outputStreamR[F](configuration, url).map { os =>
      new HadoopWriter[F, JsonNode] {
        override def write(cjn: Chunk[JsonNode]): F[Unit] =
          F.blocking {
            cjn.foreach { jn =>
              os.write(objectWriter.writeValueAsBytes(jn))
              os.write('\n')
            }
            os.flush()
          }
      }
    }

  /*
   * output stream writer based
   */

  /** The file output stream wrapped in a UTF-8 `OutputStreamWriter`; the shared entry point for the
    * character-based sinks (`stringR`, `kantanR`, `circeR`).
    */
  private def outputStreamWriterR[F[_]](configuration: Configuration, url: Url)(using
    F: Sync[F]): Resource[F, OutputStreamWriter] =
    Resource.fromAutoCloseable(
      F.blocking(new OutputStreamWriter(file_output_stream(configuration, url), StandardCharsets.UTF_8)))

  /** Write strings as text, appending the platform line separator after each element, so each written string
    * occupies its own line (round-trips with `HadoopReader.stringS`).
    */
  def stringR[F[_]](configuration: Configuration, url: Url)(using
    F: Sync[F]): Resource[F, HadoopWriter[F, String]] =
    outputStreamWriterR[F](configuration, url).map(writer =>
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

  /** Write CSV rows using kantan's engine configured by `csvConfiguration`.
    *
    * A header row is emitted once, up front, via the `evalTap` on the built writer: an explicit header uses
    * its configured columns, an implicit header writes a placeholder row, and `Header.None` writes nothing.
    * Each subsequent `write` appends the chunk's rows and flushes.
    */
  def kantanR[F[_]](configuration: Configuration, url: Url, csvConfiguration: CsvConfiguration)(using
    F: Sync[F]): Resource[F, HadoopWriter[F, Seq[String]]] = {
    val header: Chunk[Seq[String]] = csvConfiguration.header match {
      case Header.None             => Chunk.empty
      case Header.Implicit         => Chunk.singleton(List("implicit header"))
      case Header.Explicit(header) => Chunk.singleton(header)
    }
    outputStreamWriterR[F](configuration, url).map { osw =>
      val writer: CsvWriter[Seq[String]] = internalCsvWriterEngine.writerFor(osw, csvConfiguration)
      new HadoopWriter[F, Seq[String]] {
        override def write(css: Chunk[Seq[String]]): F[Unit] =
          F.blocking {
            css.foreach(writer.write(_): Unit)
            osw.flush()
          }
      }
    }.evalTap(_.write(header))
  }

  /** Write circe `Json` as newline-delimited JSON using a no-spaces `Printer`: each value is printed
    * compactly and followed by the platform line separator, one JSON document per line (round-trips with
    * `HadoopReader.circeS`).
    */
  def circeR[F[_]](configuration: Configuration, url: Url)(using
    F: Sync[F]): Resource[F, HadoopWriter[F, Json]] =
    outputStreamWriterR[F](configuration, url).map { writer =>
      val printer = Printer.noSpaces
      new HadoopWriter[F, Json] {
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
