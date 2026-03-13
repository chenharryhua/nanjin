package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.NonEmptyHotswap
import cats.syntax.functor.toFunctorOps
import cats.syntax.monadError.catsSyntaxMonadError
import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import fs2.{Chunk, Pipe, Pull, Stream}
import io.circe.Json
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter.Builder
import scalapb.GeneratedMessage

import java.time.ZoneId

final private class RotateBySizeSink[F[_]](
  configuration: Configuration,
  zoneId: ZoneId,
  pathBuilder: CreateRotateFile => Url,
  sizeLimit: Long)(using F: Async[F])
    extends RotateBySize[F] {

  private type GetWriter[A] = Reader[Url, Resource[F, HadoopWriter[F, A]]]

  /** Recursively writes data from the stream to disk, rotating files based on size.
    *
    * @param getWriter
    *   function to create a new HadoopWriter given a CreateRotateFile event
    * @param hotswap
    *   current Hotswap managing the writer resource
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
    getWriter: GetWriter[A],
    hotswap: NonEmptyHotswap[F, HadoopWriter[F, A]],
    data: Stream[F, A],
    previousTick: TickedValue[Url],
    count: Long
  ): Pull[F, TickedValue[RotateFile], Unit] = {
    def writeChunk(as: Chunk[A]): Pull[F, Nothing, Unit] =
      Pull.eval(hotswap.get.use(_.write(as)))
        .adaptError(ex => RotateWriteException(previousTick, ex))

    data.pull.uncons.flatMap {
      case None =>
        for {
          now <- Pull.eval(F.realTimeInstant)
          currentTick = previousTick.advanceTick(previousTick.tick.conclude, now).map { url =>
            RotateFile(
              open = previousTick.tick.local(_.conclude),
              close = now.atZone(previousTick.tick.zoneId).toLocalDateTime,
              url = url,
              recordCount = count
            )
          }
          _ <- Pull.output1[F, TickedValue[RotateFile]](currentTick)
        } yield ()

      case Some((as, stream)) =>
        val dataSize = as.size
        // invariant: count is always < sizeLimit, therefore splitAt always consumes > 0
        if ((dataSize + count) < sizeLimit) {
          writeChunk(as) >> doWork(getWriter, hotswap, stream, previousTick, dataSize + count)
        } else {
          val (first, second) = as.splitAt((sizeLimit - count).toInt)

          for {
            _ <- writeChunk(first)
            now <- Pull.eval(F.realTimeInstant)
            currentTick = previousTick.advanceTick(previousTick.tick.conclude, now)
            url = pathBuilder(CreateRotateFile(currentTick.tick))
            _ <- Pull.eval(hotswap.swap(getWriter(url)))
            _ <- Pull.output1[F, TickedValue[RotateFile]](
              currentTick.map { url =>
                RotateFile(
                  open = currentTick.tick.local(_.commence),
                  close = currentTick.tick.local(_.conclude),
                  url = url,
                  recordCount = count + first.size
                )
              }
            )
            _ <- doWork(getWriter, hotswap, stream.cons(second), currentTick.as(url), 0L)
          } yield ()
        }
    }
  }

  private def persist[A](data: Stream[F, A], getWriter: GetWriter[A]): Stream[F, TickedValue[RotateFile]] = {
    val resources: Resource[F, (NonEmptyHotswap[F, HadoopWriter[F, A]], TickedValue[Url])] =
      Resource.eval(Tick.zeroth[F](zoneId)).flatMap { tick =>
        val url = pathBuilder(CreateRotateFile(tick))
        NonEmptyHotswap(getWriter(url)).map((_, TickedValue(tick, url)))
      }

    Stream.resource(resources).flatMap { case (hotswap, tv) =>
      doWork(getWriter = getWriter, hotswap = hotswap, data = data, previousTick = tv, count = 0L).stream
    }
  }

  private def generic_record_stream_step_leg(get_writer: Schema => GetWriter[GenericRecord])(
    ss: Stream[F, GenericRecord]): Stream[F, TickedValue[RotateFile]] =
    ss.pull.stepLeg.flatMap {
      case Some(leg) =>
        persist(leg.stream.cons(leg.head), get_writer(leg.head(0).getSchema)).pull.echo
      case None => Pull.done
    }.stream

  // avro schema-less

  override def avro(compression: AvroCompression): Sink[GenericRecord] = {
    def get_writer(schema: Schema): GetWriter[GenericRecord] =
      Reader(url => HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, url))

    generic_record_stream_step_leg(get_writer)
  }

  // binary avro
  override val binAvro: Sink[GenericRecord] = {
    def get_writer(schema: Schema): GetWriter[GenericRecord] =
      Reader(url => HadoopWriter.binAvroR[F](configuration, schema, url))

    generic_record_stream_step_leg(get_writer)
  }

  // jackson
  override val jackson: Sink[GenericRecord] = {
    def get_writer(schema: Schema): GetWriter[GenericRecord] =
      Reader(url => HadoopWriter.jacksonR[F](configuration, schema, url))

    generic_record_stream_step_leg(get_writer)
  }

  // parquet
  override def parquet(f: Endo[Builder[GenericRecord]]): Sink[GenericRecord] = {
    def get_writer(schema: Schema): GetWriter[GenericRecord] = Reader { url =>
      HadoopWriter.parquetR[F](default_parquet_write_builder(configuration, schema, f), url)
    }

    generic_record_stream_step_leg(get_writer)
  }

  // bytes
  override val bytes: Sink[Byte] = {
    val get_writer: GetWriter[Byte] =
      Reader(url => HadoopWriter.byteR[F](configuration, url))

    (ss: Stream[F, Byte]) => persist(ss, get_writer)
  }

  // circe json
  override val circe: Sink[Json] = {
    val get_writer: GetWriter[Json] =
      Reader(url => HadoopWriter.circeR[F](configuration, url))

    (ss: Stream[F, Json]) => persist(ss, get_writer)
  }

  // kantan csv
  override def kantan(csvConfiguration: CsvConfiguration): Sink[Seq[String]] = {
    val get_writer: GetWriter[String] =
      Reader(url =>
        HadoopWriter
          .csvStringR[F](configuration, url)
          .evalTap(_.write(headerWithCrlf(csvConfiguration))))

    (ss: Stream[F, Seq[String]]) => persist(ss.map(csvRow(csvConfiguration)), get_writer)
  }

  // text
  override val text: Sink[String] = {
    val get_writer: GetWriter[String] =
      Reader(url => HadoopWriter.stringR(configuration, url))

    (ss: Stream[F, String]) => persist(ss, get_writer)
  }

  override val protobuf: Sink[GeneratedMessage] = {
    val get_writer: GetWriter[GeneratedMessage] =
      Reader(url => HadoopWriter.protobufR(configuration, url))

    (ss: Stream[F, GeneratedMessage]) => persist(ss, get_writer)
  }

  // json node
  override def jsonNode: Pipe[F, JsonNode, TickedValue[RotateFile]] = {
    val get_writer: GetWriter[JsonNode] =
      Reader(url => HadoopWriter.jsonNodeR(configuration, url))

    (ss: Stream[F, JsonNode]) => persist(ss, get_writer)
  }
}
