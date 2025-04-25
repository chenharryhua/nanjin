package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick, TickStatus, TickedValue}
import fs2.{Pull, Stream}
import io.circe.Json
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

final private class RotateBySizeSink[F[_]](
  configuration: Configuration,
  pathBuilder: Tick => Path,
  sizeLimit: Int)(implicit F: Async[F])
    extends RotateSink[F] {

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

  override def avro(compression: AvroCompression): Sink[GenericRecord] = {
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

  // avro schema
  override def avro(schema: Schema, compression: AvroCompression): Sink[GenericRecord] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) => persist(ss, get_writer).stream
  }

  // binary avro
  override val binAvro: Sink[GenericRecord] = {
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

  override def binAvro(schema: Schema): Sink[GenericRecord] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.binAvroR[F](configuration, schema, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) => persist(ss, get_writer).stream
  }

  // jackson
  override val jackson: Sink[GenericRecord] = {
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

  override def jackson(schema: Schema): Sink[GenericRecord] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) => persist(ss, get_writer).stream
  }

  // parquet
  override def parquet(f: Endo[Builder[GenericRecord]]): Sink[GenericRecord] = {

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

  override def parquet(schema: Schema, f: Endo[Builder[GenericRecord]]): Sink[GenericRecord] = {

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

  // bytes
  override val bytes: Sink[Byte] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, Byte]] =
      HadoopWriter.byteR[F](configuration, pathBuilder(tick))

    (ss: Stream[F, Byte]) => persist(ss, get_writer).stream
  }

  // circe json
  override val circe: Sink[Json] = {

    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](configuration, pathBuilder(tick))

    (ss: Stream[F, Json]) => persist(ss.mapChunks(_.map(_.noSpaces)), get_writer).stream
  }

  // kantan csv
  override def kantan(csvConfiguration: CsvConfiguration): Sink[Seq[String]] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter
        .csvStringR[F](configuration, pathBuilder(tick))
        .evalTap(_.write(csvHeader(csvConfiguration)))

    (ss: Stream[F, Seq[String]]) => persist(ss.map(csvRow(csvConfiguration)), get_writer).stream
  }

  // text
  override val text: Sink[String] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, pathBuilder(tick))

    (ss: Stream[F, String]) => persist(ss, get_writer).stream
  }


  override val protobuf: Sink[GeneratedMessage] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GeneratedMessage]] =
      HadoopWriter.protobufR(configuration, pathBuilder(tick))

    (ss: Stream[F, GeneratedMessage]) => persist(ss, get_writer).stream
  }
}
