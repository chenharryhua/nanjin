package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick, TickStatus, TickedValue}
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

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

final class RotateBySizeSink[F[_]] private (
  configuration: Configuration,
  pathBuilder: Tick => Path,
  sizeLimit: Int)(implicit F: Async[F]) {

  private def doWork[A](
    getWriter: Tick => Resource[F, HadoopWriter[F, A]],
    hotswap: Hotswap[F, HadoopWriter[F, A]],
    writer: HadoopWriter[F, A],
    data: Stream[F, A],
    status: TickStatus,
    count: Int
  ): Pull[F, TickedValue[Int], Unit] =
    data.pull.uncons.flatMap {
      case Some((as, stream)) =>
        val chunkSize = as.size
        if ((chunkSize + count) < sizeLimit) {
          Pull.output1(TickedValue(status.tick, chunkSize)) >>
            Pull.eval(writer.write(as)) >>
            doWork(getWriter, hotswap, writer, stream, status, chunkSize + count)
        } else {
          val (first, second) = as.splitAt(sizeLimit - count)
          Pull.output1(TickedValue(status.tick, first.size)) >>
            Pull.eval(writer.write(first)) >>
            Pull.eval(F.realTimeInstant.map(status.next)).flatMap {
              case Some(ts) =>
                Pull.eval(hotswap.swap(getWriter(ts.tick))).flatMap { writer =>
                  doWork(getWriter, hotswap, writer, Stream.chunk(second) ++ stream, ts, 0)
                }
              case None => Pull.done // never happen
            }
        }
      case None => Pull.done
    }

  private def persist[A](
    data: Stream[F, A],
    getWriter: Tick => Resource[F, HadoopWriter[F, A]]): Pull[F, TickedValue[Int], Unit] =
    Stream
      .eval(TickStatus.zeroth(Policy.fixedDelay(0.seconds), ZoneId.systemDefault()))
      .flatMap { tickStatus =>
        Stream.resource(Hotswap(getWriter(tickStatus.tick))).flatMap { case (hotswap, writer) =>
          doWork(getWriter, hotswap, writer, data, tickStatus, 0).stream
        }
      }
      .pull
      .echo

  // avro schema-less

  def avro(compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(schema: Schema)(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          val schema = leg.head(0).getSchema
          persist(Stream.chunk(leg.head) ++ leg.stream, get_writer(schema))
        case None => Pull.done
      }.stream
  }

  def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(f(AvroCompression))

  val avro: Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(AvroCompression.Uncompressed)

  // avro schema
  def avro(schema: Schema, compression: AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) => persist(ss, get_writer).stream
  }

  def avro(
    schema: Schema,
    f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(schema, f(AvroCompression))

  def avro(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] =
    avro(schema, AvroCompression.Uncompressed)

  // binary avro
  val binAvro: Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(schema: Schema)(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.binAvroR[F](configuration, schema, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          val schema: Schema = leg.head(0).getSchema
          persist(Stream.chunk(leg.head) ++ leg.stream, get_writer(schema))
        case None => Pull.done
      }.stream

  }

  def binAvro(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.binAvroR[F](configuration, schema, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) => persist(ss, get_writer).stream
  }

  // jackson
  val jackson: Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(schema: Schema)(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          val schema: Schema = leg.head(0).getSchema
          persist(ss, get_writer(schema))
        case None => Pull.done
      }.stream
  }

  def jackson(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) => persist(ss, get_writer).stream
  }

  // parquet
  def parquet(f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[Int]] = {

    def get_writer(schema: Schema)(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] = {
      val writeBuilder: Reader[Path, Builder[GenericRecord]] = Reader((path: Path) =>
        AvroParquetWriter
          .builder[GenericRecord](HadoopOutputFile.fromPath(path, configuration))
          .withDataModel(GenericData.get())
          .withConf(configuration)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)

      HadoopWriter.parquetR[F](writeBuilder, pathBuilder(tick))
    }

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          val schema: Schema = leg.head(0).getSchema
          persist(Stream.chunk(leg.head) ++ leg.stream, get_writer(schema))
        case None => Pull.done
      }.stream
  }

  val parquet: Pipe[F, GenericRecord, TickedValue[Int]] =
    parquet(a => a)

  def parquet(schema: Schema, f: Endo[Builder[GenericRecord]]): Pipe[F, GenericRecord, TickedValue[Int]] = {

    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] = {
      val writeBuilder: Reader[Path, Builder[GenericRecord]] = Reader((path: Path) =>
        AvroParquetWriter
          .builder[GenericRecord](HadoopOutputFile.fromPath(path, configuration))
          .withDataModel(GenericData.get())
          .withConf(configuration)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)

      HadoopWriter.parquetR[F](writeBuilder, pathBuilder(tick))
    }

    (ss: Stream[F, GenericRecord]) => persist(ss, get_writer).stream
  }

  def parquet(schema: Schema): Pipe[F, GenericRecord, TickedValue[Int]] =
    parquet(schema, identity)

  // bytes
  val bytes: Pipe[F, Byte, TickedValue[Int]] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, Byte]] =
      HadoopWriter.byteR[F](configuration, pathBuilder(tick))

    (ss: Stream[F, Byte]) => persist(ss, get_writer).stream
  }

  // circe json
  val circe: Pipe[F, Json, TickedValue[Int]] = {

    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](configuration, pathBuilder(tick))

    (ss: Stream[F, Json]) => persist(ss.mapChunks(_.map(_.noSpaces)), get_writer).stream
  }

  // kantan csv
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], TickedValue[Int]] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.csvStringR[F](configuration, pathBuilder(tick))

    val header: Chunk[String] = csvHeader(csvConfiguration)
    (ss: Stream[F, Seq[String]]) =>
      val encodedSrc: Stream[F, String] = if (csvConfiguration.hasHeader) {
        Stream.chunk(header) ++ ss.map(csvRow(csvConfiguration))
      } else {
        ss.map(csvRow(csvConfiguration))
      }
      persist(encodedSrc, get_writer).stream
  }

  def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], TickedValue[Int]] =
    kantan(f(CsvConfiguration.rfc))

  val kantan: Pipe[F, Seq[String], TickedValue[Int]] =
    kantan(CsvConfiguration.rfc)

  // text
  val text: Pipe[F, String, TickedValue[Int]] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, pathBuilder(tick))

    (ss: Stream[F, String]) => persist(ss, get_writer).stream

  }

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * https://protobuf.dev/programming-guides/proto-limits/#total
    */
  val protobuf: Pipe[F, GeneratedMessage, TickedValue[Int]] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GeneratedMessage]] =
      HadoopWriter.protobufR(configuration, pathBuilder(tick))

    (ss: Stream[F, GeneratedMessage]) => persist(ss, get_writer).stream
  }
}

private object RotateBySizeSink {
  def apply[F[_]: Async](configuration: Configuration, pb: Tick => Url, size: Int): RotateBySizeSink[F] =
    new RotateBySizeSink[F](configuration, pb.andThen(toHadoopPath), size)
}
