package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.chrono.TickedValue
import fs2.{Chunk, Pipe, Pull, Stream}
import io.circe.Json
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.avro.AvroParquetWriter.Builder
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import scalapb.GeneratedMessage

sealed private trait RotateType
private object RotateType {
  case object PolicyBased extends RotateType
  case object ChunkBased extends RotateType
}

final class FileRotateSink[F[_]: Async] private (
  rotateType: RotateType,
  configuration: Configuration,
  ticks: Stream[F, TickedValue[Path]]) {
  // avro - schema-less
  def avro(compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(schema: Schema)(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, GenericRecord]) =>
          ss.pull.peek1.flatMap {
            case Some((grs, stream)) =>
              periodically.merge.persist(stream.chunks, ticks, get_writer(grs.getSchema))
            case None => Pull.done
          }.stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, GenericRecord]) =>
          ss.pull.stepLeg.flatMap {
            case Some(leg) =>
              val schema = leg.head(0).getSchema
              periodically.zip.persist(
                get_writer(schema),
                (Stream.chunk(leg.head) ++ leg.stream).chunks.zip(ticks))
            case None => Pull.done
          }.stream
    }
  }

  def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(f(AvroCompression))

  val avro: Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(AvroCompression.Uncompressed)

  // avro schema
  def avro(schema: Schema, compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, GenericRecord]) => periodically.merge.persist(ss.chunks, ticks, get_writer).stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, GenericRecord]) => periodically.zip.persist(get_writer, ss.chunks.zip(ticks)).stream
    }
  }

  def avro(
    schema: Schema,
    f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(schema, f(AvroCompression))

  def avro(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(schema, AvroCompression.Uncompressed)

  // binary avro
  val binAvro: Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(schema: Schema)(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.binAvroR[F](configuration, schema, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, GenericRecord]) =>
          ss.pull.peek1.flatMap {
            case Some((grs, stream)) =>
              periodically.merge.persist(stream.chunks, ticks, get_writer(grs.getSchema))
            case None => Pull.done
          }.stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, GenericRecord]) =>
          ss.pull.stepLeg.flatMap {
            case Some(leg) =>
              val schema: Schema = leg.head(0).getSchema
              periodically.zip.persist(
                get_writer(schema),
                (Stream.chunk(leg.head) ++ leg.stream).chunks.zip(ticks))
            case None => Pull.done
          }.stream

    }
  }

  def binAvro(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.binAvroR[F](configuration, schema, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, GenericRecord]) => periodically.merge.persist(ss.chunks, ticks, get_writer).stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, GenericRecord]) => periodically.zip.persist(get_writer, ss.chunks.zip(ticks)).stream
    }
  }

  // jackson json
  val jackson: Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(schema: Schema)(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, GenericRecord]) =>
          ss.pull.peek1.flatMap {
            case Some((grs, stream)) =>
              periodically.merge.persist(stream.chunks, ticks, get_writer(grs.getSchema))
            case None => Pull.done
          }.stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, GenericRecord]) =>
          ss.pull.stepLeg.flatMap {
            case Some(leg) =>
              val schema: Schema = leg.head(0).getSchema
              periodically.zip.persist(
                get_writer(schema),
                (Stream.chunk(leg.head) ++ leg.stream).chunks.zip(ticks))
            case None => Pull.done
          }.stream
    }
  }

  def jackson(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, GenericRecord]) => periodically.merge.persist(ss.chunks, ticks, get_writer).stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, GenericRecord]) => periodically.zip.persist(get_writer, ss.chunks.zip(ticks)).stream
    }
  }

  // parquet
  def parquet(f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[Int]] = {

    def get_writer(schema: Schema)(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] = {
      val writeBuilder: Reader[Path, Builder[GenericRecord]] = Reader((path: Path) =>
        AvroParquetWriter
          .builder[GenericRecord](HadoopOutputFile.fromPath(path, configuration))
          .withDataModel(GenericData.get())
          .withConf(configuration)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)

      HadoopWriter.parquetR[F](writeBuilder, url)
    }

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, GenericRecord]) =>
          ss.pull.peek1.flatMap {
            case Some((grs, stream)) =>
              periodically.merge.persist(stream.chunks, ticks, get_writer(grs.getSchema))
            case None => Pull.done
          }.stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, GenericRecord]) =>
          ss.pull.stepLeg.flatMap {
            case Some(leg) =>
              val schema: Schema = leg.head(0).getSchema
              periodically.zip.persist(
                get_writer(schema),
                (Stream.chunk(leg.head) ++ leg.stream).chunks.zip(ticks))
            case None => Pull.done
          }.stream
    }
  }

  val parquet: Pipe[F, GenericRecord, TickedValue[Int]] =
    parquet(a => a)

  def parquet(schema: Schema, f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[Int]] = {

    def get_writer(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] = {
      val writeBuilder: Reader[Path, Builder[GenericRecord]] = Reader((path: Path) =>
        AvroParquetWriter
          .builder[GenericRecord](HadoopOutputFile.fromPath(path, configuration))
          .withDataModel(GenericData.get())
          .withConf(configuration)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)

      HadoopWriter.parquetR[F](writeBuilder, url)
    }

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, GenericRecord]) => periodically.merge.persist(ss.chunks, ticks, get_writer).stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, GenericRecord]) => periodically.zip.persist(get_writer, ss.chunks.zip(ticks)).stream
    }
  }

  def parquet(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] =
    parquet(schema, identity)

  // bytes
  val bytes: Pipe[F, Byte, TickedValue[Int]] = {
    def get_writer(url: Path): Resource[F, HadoopWriter[F, Byte]] =
      HadoopWriter.byteR[F](configuration, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, Byte]) => periodically.merge.persist(ss.chunks, ticks, get_writer).stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, Byte]) => periodically.zip.persist(get_writer, ss.chunks.zip(ticks)).stream
    }
  }

  // circe json
  val circe: Pipe[F, Json, TickedValue[Int]] = {

    def get_writer(url: Path): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](configuration, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, Json]) =>
          periodically.merge.persist(ss.chunks.map(_.map(_.noSpaces)), ticks, get_writer).stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, Json]) =>
          periodically.zip.persist(get_writer, ss.chunks.map(_.map(_.noSpaces)).zip(ticks)).stream
    }
  }

  // kantan csv
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], TickedValue[Int]] = {
    def get_writer(url: Path): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.csvStringR[F](configuration, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, Seq[String]]) =>
          val encodedSrc: Stream[F, Chunk[String]] = ss.chunks.map(_.map(csvRow(csvConfiguration)))
          if (csvConfiguration.hasHeader) {
            val header: Chunk[String] = csvHeader(csvConfiguration)
            ticks.pull.uncons1.flatMap {
              case Some((head, tail)) =>
                Stream
                  .resource(Hotswap(get_writer(head.value)))
                  .flatMap { case (hotswap, writer) =>
                    Stream.eval(writer.write(header)) >>
                      periodically
                        .persistCsvWithHeader[F, Path](
                          head.tick,
                          get_writer,
                          hotswap,
                          writer,
                          encodedSrc.map(Left(_)).mergeHaltBoth(tail.map(Right(_))),
                          header
                        )
                        .stream
                  }
                  .pull
                  .echo
              case None => Pull.done
            }.stream
          } else periodically.merge.persist(encodedSrc, ticks, get_writer).stream

      case RotateType.ChunkBased =>
        val header: Chunk[String] = csvHeader(csvConfiguration)
        (ss: Stream[F, Seq[String]]) =>
          val encodedSrc: Stream[F, Chunk[String]] = if (csvConfiguration.hasHeader) {
            ss.chunks.map(ck => header ++ ck.map(csvRow(csvConfiguration)))
          } else {
            ss.chunks.map(ck => ck.map(csvRow(csvConfiguration)))
          }
          periodically.zip.persist(get_writer, encodedSrc.zip(ticks)).stream
    }
  }

  def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], TickedValue[Int]] =
    kantan(f(CsvConfiguration.rfc))

  val kantan: Pipe[F, Seq[String], TickedValue[Int]] =
    kantan(CsvConfiguration.rfc)

  // text
  val text: Pipe[F, String, TickedValue[Int]] = {
    def get_writer(url: Path): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, String]) => periodically.merge.persist(ss.chunks, ticks, get_writer).stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, String]) => periodically.zip.persist(get_writer, ss.chunks.zip(ticks)).stream
    }
  }

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * https://protobuf.dev/programming-guides/proto-limits/#total
    */
  val protobuf: Pipe[F, GeneratedMessage, TickedValue[Int]] = {
    def get_writer(url: Path): Resource[F, HadoopWriter[F, GeneratedMessage]] =
      HadoopWriter.protobufR(configuration, url)

    rotateType match {
      case RotateType.PolicyBased =>
        (ss: Stream[F, GeneratedMessage]) => periodically.merge.persist(ss.chunks, ticks, get_writer).stream
      case RotateType.ChunkBased =>
        (ss: Stream[F, GeneratedMessage]) => periodically.zip.persist(get_writer, ss.chunks.zip(ticks)).stream
    }
  }
}

private object FileRotateSink {
  def apply[F[_]: Async](
    rotateType: RotateType,
    configuration: Configuration,
    paths: Stream[F, TickedValue[Url]]): FileRotateSink[F] =
    new FileRotateSink(
      rotateType,
      configuration,
      paths.map(tv => TickedValue(tv.tick, toHadoopPath(tv.value))))
}
