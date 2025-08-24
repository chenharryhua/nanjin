package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import fs2.{Chunk, Pull, Stream}
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

final private class RotateByPolicySink[F[_]: Async](
  configuration: Configuration,
  tickedUrl: Stream[F, TickedValue[Url]])
    extends RotateByPolicy[F] {

  private def doWork[A](
    currentTick: Tick,
    getWriter: Url => Resource[F, HadoopWriter[F, A]],
    hotswap: Hotswap[F, HadoopWriter[F, A]],
    writer: HadoopWriter[F, A],
    merged: Stream[F, Either[Chunk[A], TickedValue[Url]]],
    count: Int
  ): Pull[F, TickedValue[RotateFileResult], Unit] =
    merged.pull.uncons1.flatMap {
      case None =>
        Pull.eval(hotswap.clear) >> Pull.output1(
          TickedValue(currentTick, RotateFileResult(writer.fileUrl, count)))
      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            Pull.eval(writer.write(data)) >>
              doWork(currentTick, getWriter, hotswap, writer, tail, count + data.size)
          case Right(ticked) =>
            Pull.eval(hotswap.swap(getWriter(ticked.value))).flatMap { newWriter =>
              Pull.output1(TickedValue(currentTick, RotateFileResult(writer.fileUrl, count))) >>
                doWork(ticked.tick, getWriter, hotswap, newWriter, tail, 0)
            }
        }
    }

  private def persist[A](
    data: Stream[F, Chunk[A]],
    ticks: Stream[F, TickedValue[Url]],
    getWriter: Url => Resource[F, HadoopWriter[F, A]]): Pull[F, TickedValue[RotateFileResult], Unit] =
    ticks.pull.uncons1.flatMap {
      case None               => Pull.done
      case Some((head, tail)) => // use the very first tick to build writer and hotswap
        Stream
          .resource(Hotswap(getWriter(head.value)))
          .flatMap { case (hotswap, writer) =>
            doWork(
              currentTick = head.tick,
              getWriter = getWriter,
              hotswap = hotswap,
              writer = writer,
              merged = data.map(Left(_)).mergeHaltBoth(tail.map(Right(_))),
              count = 0).stream
          }
          .pull
          .echo
    }

  /*
   * sinks
   */

  // avro - schema-less
  override def avro(compression: AvroCompression): Sink[GenericRecord] = {
    def get_writer(schema: Schema)(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, url)

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.peek1.flatMap {
        case Some((gr, stream)) =>
          persist(stream.chunks, tickedUrl, get_writer(gr.getSchema))
        case None => Pull.done
      }.stream
  }

  // avro schema
  override def avro(schema: Schema, compression: AvroCompression): Sink[GenericRecord] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, url)

    (ss: Stream[F, GenericRecord]) => persist(ss.chunks, tickedUrl, get_writer).stream
  }

  // binary avro
  override val binAvro: Sink[GenericRecord] = {
    def get_writer(schema: Schema)(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.binAvroR[F](configuration, schema, url)

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.peek1.flatMap {
        case Some((gr, stream)) =>
          persist(stream.chunks, tickedUrl, get_writer(gr.getSchema))
        case None => Pull.done
      }.stream
  }

  override def binAvro(schema: Schema): Sink[GenericRecord] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.binAvroR[F](configuration, schema, url)

    (ss: Stream[F, GenericRecord]) => persist(ss.chunks, tickedUrl, get_writer).stream
  }

  // jackson json
  override val jackson: Sink[GenericRecord] = {
    def get_writer(schema: Schema)(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, url)

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.peek1.flatMap {
        case Some((gr, stream)) =>
          persist(stream.chunks, tickedUrl, get_writer(gr.getSchema))
        case None => Pull.done
      }.stream
  }

  override def jackson(schema: Schema): Sink[GenericRecord] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, url)

    (ss: Stream[F, GenericRecord]) => persist(ss.chunks, tickedUrl, get_writer).stream
  }

  // parquet
  override def parquet(f: Endo[Builder[GenericRecord]]): Sink[GenericRecord] = {

    def get_writer(schema: Schema)(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] = {
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

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.peek1.flatMap {
        case Some((gr, stream)) =>
          persist(stream.chunks, tickedUrl, get_writer(gr.getSchema))
        case None => Pull.done
      }.stream
  }

  override def parquet(schema: Schema, f: Endo[Builder[GenericRecord]]): Sink[GenericRecord] = {

    def get_writer(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] = {
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

    (ss: Stream[F, GenericRecord]) => persist(ss.chunks, tickedUrl, get_writer).stream
  }

  // bytes
  override val bytes: Sink[Byte] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, Byte]] =
      HadoopWriter.byteR[F](configuration, url)

    (ss: Stream[F, Byte]) => persist(ss.chunks, tickedUrl, get_writer).stream
  }

  // circe json
  override val circe: Sink[Json] = {

    def get_writer(url: Url): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](configuration, url)

    (ss: Stream[F, Json]) => persist(ss.map(_.noSpaces).chunks, tickedUrl, get_writer).stream
  }

  // kantan csv
  override def kantan(csvConfiguration: CsvConfiguration): Sink[Seq[String]] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.csvStringR[F](configuration, url).evalTap(_.write(csvHeader(csvConfiguration)))

    (ss: Stream[F, Seq[String]]) =>
      persist(ss.map(csvRow(csvConfiguration)).chunks, tickedUrl, get_writer).stream
  }

  // text
  override val text: Sink[String] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, url)

    (ss: Stream[F, String]) => persist(ss.chunks, tickedUrl, get_writer).stream
  }

  override val protobuf: Sink[GeneratedMessage] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, GeneratedMessage]] =
      HadoopWriter.protobufR(configuration, url)

    (ss: Stream[F, GeneratedMessage]) => persist(ss.chunks, tickedUrl, get_writer).stream
  }
}
