package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.toFunctorOps
import fs2.{Pipe, Pull, Stream}
import io.circe.Json
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import scalapb.GeneratedMessage

import java.io.OutputStream

final class FileSink[F[_]: Sync] private (configuration: Configuration, path: Path) {

  /** [[https://avro.apache.org]]
    */
  def avro(compression: AvroCompression): Pipe[F, GenericRecord, Int] = { (ss: Stream[F, GenericRecord]) =>
    ss.pull.peek1.flatMap {
      case Some((gr, stream)) =>
        val schema = gr.getSchema
        Stream
          .resource(HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, path))
          .flatMap(w => stream.chunks.evalMap(c => w.write(c).as(c.size)))
          .pull
          .echo
      case None => Pull.done
    }.stream
  }

  /** [[https://avro.apache.org]]
    */
  def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, Int] =
    avro(f(AvroCompression))

  /** [[https://avro.apache.org]]
    */
  val avro: Pipe[F, GenericRecord, Int] =
    avro(AvroCompression.Uncompressed)

  /** [[https://avro.apache.org]]
    */
  val binAvro: Pipe[F, GenericRecord, Int] = { (ss: Stream[F, GenericRecord]) =>
    ss.pull.peek1.flatMap {
      case Some((gr, stream)) =>
        val schema = gr.getSchema
        Stream
          .resource(HadoopWriter.binAvroR[F](configuration, schema, path))
          .flatMap(w => stream.chunks.evalMap(c => w.write(c).as(c.size)))
          .pull
          .echo
      case None => Pull.done
    }.stream
  }

  /** [[https://github.com/FasterXML/jackson]]
    */
  val jackson: Pipe[F, GenericRecord, Int] = { (ss: Stream[F, GenericRecord]) =>
    ss.pull.peek1.flatMap {
      case Some((gr, stream)) =>
        val schema = gr.getSchema
        Stream
          .resource(HadoopWriter.jacksonR[F](configuration, schema, path))
          .flatMap(w => stream.chunks.evalMap(c => w.write(c).as(c.size)))
          .pull
          .echo
      case None => Pull.done
    }.stream
  }

  /** [[https://parquet.apache.org]]
    */
  def parquet(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): Pipe[F, GenericRecord, Int] = {
    (ss: Stream[F, GenericRecord]) =>
      ss.pull.peek1.flatMap {
        case Some((gr, stream)) =>
          val schema = gr.getSchema
          val writeBuilder = Reader((path: Path) =>
            AvroParquetWriter
              .builder[GenericRecord](HadoopOutputFile.fromPath(path, configuration))
              .withDataModel(GenericData.get())
              .withConf(configuration)
              .withSchema(schema)
              .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
              .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)

          Stream
            .resource(HadoopWriter.parquetR[F](writeBuilder, path))
            .flatMap(w => stream.chunks.evalMap(c => w.write(c).as(c.size)))
            .pull
            .echo
        case None => Pull.done
      }.stream
  }

  /** [[https://parquet.apache.org]]
    */
  val parquet: Pipe[F, GenericRecord, Int] =
    parquet(identity)

  // bytes
  val bytes: Pipe[F, Byte, Int] = { (ss: Stream[F, Byte]) =>
    Stream
      .resource(HadoopWriter.byteR[F](configuration, path))
      .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  /** [[https://github.com/circe/circe]]
    */
  val circe: Pipe[F, Json, Int] = { (ss: Stream[F, Json]) =>
    Stream
      .resource(HadoopWriter.stringR[F](configuration, path))
      .flatMap(w => ss.map(_.noSpaces).chunks.evalMap(c => w.write(c).as(c.size)))
  }

  /** [[https://nrinaudo.github.io/kantan.csv]]
    */
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], Int] = {
    (ss: Stream[F, Seq[String]]) =>
      Stream
        .resource(
          HadoopWriter.csvStringR[F](configuration, path).evalTap(_.write(csvHeader(csvConfiguration))))
        .flatMap { w =>
          ss.map(csvRow(csvConfiguration)).chunks.evalMap(c => w.write(c).as(c.size))
        }
  }

  /** [[https://nrinaudo.github.io/kantan.csv]]
    */
  def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], Int] =
    kantan(f(CsvConfiguration.rfc))

  /** [[https://nrinaudo.github.io/kantan.csv]]
    */
  val kantan: Pipe[F, Seq[String], Int] =
    kantan(CsvConfiguration.rfc)

  // text
  val text: Pipe[F, String, Int] = { (ss: Stream[F, String]) =>
    Stream
      .resource(HadoopWriter.stringR[F](configuration, path))
      .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * https://protobuf.dev/programming-guides/proto-limits/#total
    */
  val protobuf: Pipe[F, GeneratedMessage, Int] = { (ss: Stream[F, GeneratedMessage]) =>
    Stream.resource(HadoopWriter.protobufR(configuration, path)).flatMap { w =>
      ss.chunks.evalMap(c => w.write(c).as(c.size))
    }
  }

  // java OutputStream
  val outputStream: Resource[F, OutputStream] =
    HadoopWriter.outputStreamR[F](path, configuration)
}

private object FileSink {
  def apply[F[_]: Sync](configuration: Configuration, path: Url): FileSink[F] =
    new FileSink[F](configuration, toHadoopPath(path))
}
