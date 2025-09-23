package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import cats.implicits.toFunctorOps
import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import fs2.{Pipe, Pull, Stream}
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

import java.time.{Duration, ZoneId}

final private class RotateBySizeSink[F[_]](
  configuration: Configuration,
  zoneId: ZoneId,
  pathBuilder: CreateRotateFile => Url,
  sizeLimit: Int)(implicit F: Async[F])
    extends RotateBySize[F] {

  private def createRotateFileEvent(tick: Tick): CreateRotateFile =
    CreateRotateFile(tick.sequenceId, tick.index + 1, tick.zoned(_.conclude))

  private def doWork[A](
    getWriter: CreateRotateFile => Resource[F, HadoopWriter[F, A]],
    hotswap: Hotswap[F, HadoopWriter[F, A]],
    writer: HadoopWriter[F, A],
    data: Stream[F, A],
    currentTick: Tick,
    count: Int
  ): Pull[F, TickedValue[RotateFile], Unit] =
    data.pull.uncons.flatMap {
      case None =>
        Pull.eval(hotswap.clear) >> Pull
          .eval(F.realTimeInstant.map(currentTick.nextTick(_, Duration.ZERO)))
          .flatMap { newTick =>
            Pull.output1(TickedValue(newTick, RotateFile(writer.fileUrl, count)))
          }
      case Some((as, stream)) =>
        val dataSize = as.size
        if ((dataSize + count) < sizeLimit) {
          Pull.eval(writer.write(as)) >>
            doWork(getWriter, hotswap, writer, stream, currentTick, dataSize + count)
        } else {
          val (first, second) = as.splitAt(sizeLimit - count)

          Pull.eval(writer.write(first)) >>
            Pull.eval(F.realTimeInstant.map(currentTick.nextTick(_, Duration.ZERO))).flatMap { newTick =>
              Pull.eval(hotswap.swap(getWriter(createRotateFileEvent(newTick)))).flatMap { newWriter =>
                Pull.output1(TickedValue(newTick, RotateFile(writer.fileUrl, count + first.size))) >>
                  doWork(getWriter, hotswap, newWriter, stream.cons(second), newTick, 0)
              }
            }
        }
    }

  private def persist[A](data: Stream[F, A], getWriter: CreateRotateFile => Resource[F, HadoopWriter[F, A]])
    : Pull[F, TickedValue[RotateFile], Unit] = {
    val resources: Resource[F, ((Hotswap[F, HadoopWriter[F, A]], HadoopWriter[F, A]), Tick)] =
      Resource.eval(Tick.zeroth(zoneId)).flatMap { tick =>
        Hotswap(getWriter(createRotateFileEvent(tick))).map((_, tick))
      }

    Stream
      .resource(resources)
      .flatMap { case ((hotswap, writer), tick) =>
        doWork(
          getWriter = getWriter,
          hotswap = hotswap,
          writer = writer,
          data = data,
          currentTick = tick,
          count = 0).stream
      }
      .pull
      .echo
  }

  // avro schema-less

  override def avro(compression: AvroCompression): Sink[GenericRecord] = {
    def get_writer(schema: Schema)(tick: CreateRotateFile): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          persist(leg.stream.cons(leg.head), get_writer(leg.head(0).getSchema))
        case None => Pull.done
      }.stream
  }

  // binary avro
  override val binAvro: Sink[GenericRecord] = {
    def get_writer(schema: Schema)(tick: CreateRotateFile): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.binAvroR[F](configuration, schema, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          persist(leg.stream.cons(leg.head), get_writer(leg.head(0).getSchema))
        case None => Pull.done
      }.stream
  }

  // jackson
  override val jackson: Sink[GenericRecord] = {
    def get_writer(schema: Schema)(tick: CreateRotateFile): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, pathBuilder(tick))

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          persist(leg.stream.cons(leg.head), get_writer(leg.head(0).getSchema))
        case None => Pull.done
      }.stream
  }

  // parquet
  override def parquet(f: Endo[Builder[GenericRecord]]): Sink[GenericRecord] = {

    def get_writer(schema: Schema)(tick: CreateRotateFile): Resource[F, HadoopWriter[F, GenericRecord]] = {
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
          persist(leg.stream.cons(leg.head), get_writer(leg.head(0).getSchema))
        case None => Pull.done
      }.stream
  }

  // bytes
  override val bytes: Sink[Byte] = {
    def get_writer(cfe: CreateRotateFile): Resource[F, HadoopWriter[F, Byte]] =
      HadoopWriter.byteR[F](configuration, pathBuilder(cfe))

    (ss: Stream[F, Byte]) => persist(ss, get_writer).stream
  }

  // circe json
  override val circe: Sink[Json] = {

    def get_writer(cfe: CreateRotateFile): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](configuration, pathBuilder(cfe))

    (ss: Stream[F, Json]) => persist(ss.map(_.noSpaces), get_writer).stream
  }

  // kantan csv
  override def kantan(csvConfiguration: CsvConfiguration): Sink[Seq[String]] = {
    def get_writer(cfe: CreateRotateFile): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter
        .csvStringR[F](configuration, pathBuilder(cfe))
        .evalTap(_.write(csvHeader(csvConfiguration)))

    (ss: Stream[F, Seq[String]]) => persist(ss.map(csvRow(csvConfiguration)), get_writer).stream
  }

  // text
  override val text: Sink[String] = {
    def get_writer(cfe: CreateRotateFile): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, pathBuilder(cfe))

    (ss: Stream[F, String]) => persist(ss, get_writer).stream
  }

  override val protobuf: Sink[GeneratedMessage] = {
    def get_writer(cfe: CreateRotateFile): Resource[F, HadoopWriter[F, GeneratedMessage]] =
      HadoopWriter.protobufR(configuration, pathBuilder(cfe))

    (ss: Stream[F, GeneratedMessage]) => persist(ss, get_writer).stream
  }

  // json node
  override def jsonNode: Pipe[F, JsonNode, TickedValue[RotateFile]] = {
    def get_writer(cfe: CreateRotateFile): Resource[F, HadoopWriter[F, JsonNode]] =
      HadoopWriter.jsonNodeR(configuration, pathBuilder(cfe))

    (ss: Stream[F, JsonNode]) => persist(ss, get_writer).stream
  }
}
