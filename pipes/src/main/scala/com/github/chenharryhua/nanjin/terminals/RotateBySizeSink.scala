package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import cats.implicits.catsSyntaxMonadError
import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
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

final private class RotateBySizeSink[F[_]](
  configuration: Configuration,
  zoneId: ZoneId,
  pathBuilder: CreateRotateFile => Url,
  sizeLimit: Long)(implicit F: Async[F])
    extends RotateBySize[F] {

  /** Recursively writes data from the stream to disk, rotating files based on size.
    *
    * @param getWriter
    *   function to create a new HadoopWriter given a CreateRotateFile event
    * @param hotswap
    *   current Hotswap managing the writer resource
    * @param writer
    *   current writer for the active file
    * @param data
    *   stream of elements to write
    * @param previousTick
    *   tick representing the previous file's timing window
    * @param count
    *   number of elements already written to the current file
    *
    * @return
    *   Pull emitting one TickedValue per file, where each TickedValue contains:
    *   - tick: the temporal window (commence, acquires, conclude)
    *   - value: RotateFile with file URL and number of records written
    *
    * ===Semantics===
    *
    *   1. Each file corresponds to one TickedValue.
    *   2. `previousTick` represents the previous file; `nextTick` generates the new tick for the current
    *      file.
    *   3. `acquires` marks the moment writing starts, `conclude` marks when writing ends.
    *   4. Files that exceed `sizeLimit` are split across ticks, generating multiple TickedValues.
    *   5. The tick index increments sequentially starting from 1.
    */
  private def doWork[A](
    getWriter: CreateRotateFile => Resource[F, HadoopWriter[F, A]],
    hotswap: Hotswap[F, HadoopWriter[F, A]],
    writer: HadoopWriter[F, A],
    data: Stream[F, A],
    previousTick: Tick,
    count: Long
  ): Pull[F, TickedValue[RotateFile], Unit] = {
    def writeChunk(as: Chunk[A]): Pull[F, Nothing, Unit] =
      Pull
        .eval(writer.write(as))
        .adaptError(ex => RotateWriteException(TickedValue(previousTick, writer.fileUrl), ex))

    data.pull.uncons.flatMap {
      case None =>
        for {
          _ <- Pull.eval(hotswap.clear)
          now <- Pull.eval(F.realTimeInstant)
          currentTick = previousTick.nextTick(previousTick.conclude, now)
          _ <- Pull.output1[F, TickedValue[RotateFile]](
            TickedValue(
              currentTick,
              RotateFile(
                open = previousTick.local(_.conclude),
                close = now.atZone(previousTick.zoneId).toLocalDateTime,
                url = writer.fileUrl,
                recordCount = count
              )
            ))
        } yield ()

      case Some((as, stream)) =>
        val dataSize = as.size
        // invariant: count is always < sizeLimit, therefore splitAt always consumes > 0
        if ((dataSize + count) < sizeLimit) {
          writeChunk(as) >> doWork(getWriter, hotswap, writer, stream, previousTick, dataSize + count)
        } else {
          val (first, second) = as.splitAt((sizeLimit - count).toInt)

          for {
            _ <- writeChunk(first)
            now <- Pull.eval(F.realTimeInstant)
            currentTick = previousTick.nextTick(previousTick.conclude, now)
            newWriter <- Pull.eval(
              hotswap.swap(
                getWriter(
                  CreateRotateFile(
                    sequenceId = currentTick.sequenceId,
                    index = currentTick.index + 1,
                    openTime = currentTick.zoned(_.conclude)))))
            _ <- Pull.output1[F, TickedValue[RotateFile]](
              TickedValue(
                currentTick,
                RotateFile(
                  open = previousTick.local(_.conclude),
                  close = currentTick.local(_.conclude),
                  url = writer.fileUrl,
                  recordCount = count + first.size
                )
              ))
            _ <- doWork(getWriter, hotswap, newWriter, stream.cons(second), currentTick, 0L)
          } yield ()
        }
    }
  }

  private def persist[A](data: Stream[F, A], getWriter: CreateRotateFile => Resource[F, HadoopWriter[F, A]])
    : Pull[F, TickedValue[RotateFile], Unit] = {
    val resources: Resource[F, ((Hotswap[F, HadoopWriter[F, A]], HadoopWriter[F, A]), Tick)] =
      Resource.eval(Tick.zeroth[F](zoneId)).flatMap { tick =>
        Hotswap(
          getWriter(
            CreateRotateFile(
              sequenceId = tick.sequenceId,
              index = tick.index + 1,
              openTime = tick.zoned(_.conclude)))).map((_, tick))
      }

    Stream
      .resource(resources)
      .flatMap { case ((hotswap, writer), tick) =>
        doWork(
          getWriter = getWriter,
          hotswap = hotswap,
          writer = writer,
          data = data,
          previousTick = tick,
          count = 0L).stream
      }
      .pull
      .echo
  }

  // avro schema-less

  override def avro(compression: AvroCompression): Sink[GenericRecord] = {
    def get_writer(schema: Schema)(crf: CreateRotateFile): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, pathBuilder(crf))

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          persist(leg.stream.cons(leg.head), get_writer(leg.head(0).getSchema))
        case None => Pull.done
      }.stream
  }

  // binary avro
  override val binAvro: Sink[GenericRecord] = {
    def get_writer(schema: Schema)(crf: CreateRotateFile): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.binAvroR[F](configuration, schema, pathBuilder(crf))

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          persist(leg.stream.cons(leg.head), get_writer(leg.head(0).getSchema))
        case None => Pull.done
      }.stream
  }

  // jackson
  override val jackson: Sink[GenericRecord] = {
    def get_writer(schema: Schema)(crf: CreateRotateFile): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, pathBuilder(crf))

    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          persist(leg.stream.cons(leg.head), get_writer(leg.head(0).getSchema))
        case None => Pull.done
      }.stream
  }

  // parquet
  override def parquet(f: Endo[Builder[GenericRecord]]): Sink[GenericRecord] = {

    def get_writer(schema: Schema)(crf: CreateRotateFile): Resource[F, HadoopWriter[F, GenericRecord]] = {
      val writeBuilder: Reader[Path, Builder[GenericRecord]] = Reader((path: Path) =>
        AvroParquetWriter
          .builder[GenericRecord](HadoopOutputFile.fromPath(path, configuration))
          .withDataModel(GenericData.get())
          .withConf(configuration)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)

      HadoopWriter.parquetR[F](writeBuilder, pathBuilder(crf))
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
    def get_writer(crf: CreateRotateFile): Resource[F, HadoopWriter[F, Byte]] =
      HadoopWriter.byteR[F](configuration, pathBuilder(crf))

    (ss: Stream[F, Byte]) => persist(ss, get_writer).stream
  }

  // circe json
  override val circe: Sink[Json] = {

    def get_writer(crf: CreateRotateFile): Resource[F, HadoopWriter[F, Json]] =
      HadoopWriter.circeR[F](configuration, pathBuilder(crf))

    (ss: Stream[F, Json]) => persist(ss, get_writer).stream
  }

  // kantan csv
  override def kantan(csvConfiguration: CsvConfiguration): Sink[Seq[String]] = {
    def get_writer(crf: CreateRotateFile): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter
        .csvStringR[F](configuration, pathBuilder(crf))
        .evalTap(_.write(headerWithCrlf(csvConfiguration)))

    (ss: Stream[F, Seq[String]]) => persist(ss.map(csvRow(csvConfiguration)), get_writer).stream
  }

  // text
  override val text: Sink[String] = {
    def get_writer(crf: CreateRotateFile): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, pathBuilder(crf))

    (ss: Stream[F, String]) => persist(ss, get_writer).stream
  }

  override val protobuf: Sink[GeneratedMessage] = {
    def get_writer(crf: CreateRotateFile): Resource[F, HadoopWriter[F, GeneratedMessage]] =
      HadoopWriter.protobufR(configuration, pathBuilder(crf))

    (ss: Stream[F, GeneratedMessage]) => persist(ss, get_writer).stream
  }

  // json node
  override def jsonNode: Pipe[F, JsonNode, TickedValue[RotateFile]] = {
    def get_writer(crf: CreateRotateFile): Resource[F, HadoopWriter[F, JsonNode]] =
      HadoopWriter.jsonNodeR(configuration, pathBuilder(crf))

    (ss: Stream[F, JsonNode]) => persist(ss, get_writer).stream
  }
}
