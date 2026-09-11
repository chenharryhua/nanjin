package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.NonEmptyHotswap
import cats.syntax.monadError.given
import com.fasterxml.jackson.databind.{JsonNode, ObjectWriter}
import com.github.chenharryhua.nanjin.common.chrono.Tick
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

/** `RotateBySize` implementation that starts a new file every `sizeLimit` records.
  *
  * This is the size-driven counterpart to `RotateByPolicyImpl`. Where the policy version is driven by an
  * external stream of time ticks, this one has no tick stream: it seeds a single starting `CreateRotateFile`
  * and then mints each subsequent file boundary itself by incrementing the tick `index` whenever the running
  * count reaches `sizeLimit`. Every emitted `RotateFile` holds exactly `sizeLimit` records except the last,
  * which holds the remainder.
  *
  * Rotation mechanics:
  *   - `do_work` folds over the raw data stream (no data/tick merge is needed since boundaries are computed
  *     from the count, not from external ticks) and maintains the invariant `count <= sizeLimit`.
  *   - When an incoming chunk would push the count past `sizeLimit`, it is split exactly at the boundary: the
  *     first part fills and closes the current file, the remainder is pushed back onto the stream (`cons`) to
  *     open the next file. This is why a single logical file can be filled from multiple chunks and a single
  *     chunk can span multiple files.
  *   - A `NonEmptyHotswap` holds the one open writer; each boundary swaps in a writer for the next file,
  *     finalizing the previous one, and emits its `RotateFile`.
  *   - Timing is recorded as wall-clock instants: the closing time of each file (and the opening time of the
  *     next, via `current.copy`) is the `realTimeInstant` captured at the split, so `RotateFile.window`
  *     reflects actual write duration rather than a scheduled tick.
  *
  * @param zoneId
  *   time zone used to stamp the seed tick and each rotation boundary
  * @param pathBuilder
  *   maps each `CreateRotateFile` (seed or minted) to the output `Url`
  * @param sizeLimit
  *   maximum records per file; must be positive (checked by the caller in `Hadoop.rotateSink`)
  */
final private class RotateBySizeImpl[F[_]](
  configuration: Configuration,
  zoneId: ZoneId,
  pathBuilder: CreateRotateFile => Url,
  sizeLimit: Long)(using F: Async[F])
    extends RotateBySize[F] {

  /** How to obtain a writer for a resolved `Url`: a `Reader` from `Url` to a `Resource`-managed
    * `HadoopWriter`. Deferring on `Url` lets each rotation open a distinct file with the same recipe.
    */
  private type GetWriter[A] = Reader[Url, Resource[F, HadoopWriter[F, A]]]

  /** The core rotation loop: a `Pull` that folds over the data stream and outputs one `RotateFile` per
    * completed file, splitting on the `sizeLimit` boundary.
    *
    * On each `uncons` of the data:
    *   - end of stream (`None`): emit a final `RotateFile` for the still-open `current` file with a
    *     wall-clock close time, since no size boundary was reached.
    *   - a chunk that keeps the count within `sizeLimit`: write it whole and recurse with the advanced count.
    *   - a chunk that would exceed `sizeLimit`: split at `sizeLimit - count` so the current file lands
    *     exactly on the limit; write the first part, capture `now` as its close instant, mint the next tick
    *     (`index + 1`, timed at `now`), swap the hotswap to the next file's writer, emit the closed
    *     `RotateFile`, and recurse over `stream.cons(second)` with the count reset to `0` so the remainder
    *     opens the next file. A large chunk therefore cascades through as many files as needed.
    *
    * A write failure is wrapped in `RotateWriteException` carrying the current tick, path, and the count so
    * far (the offset at which the write failed).
    *
    * @param current
    *   the tick that opened the file currently being written
    * @param count
    *   records written to the current file so far; the loop keeps `count <= sizeLimit`
    */
  private def do_work[A](
    get_writer: GetWriter[A],
    hotswap: NonEmptyHotswap[F, HadoopWriter[F, A]],
    data: Stream[F, A],
    current: CreateRotateFile,
    count: Long
  ): Pull[F, RotateFile, Unit] = {
    def writeChunk(as: Chunk[A]): Pull[F, Nothing, Unit] =
      Pull.eval(hotswap.get.use(_.write(as)))
        .adaptError(ex => RotateWriteException(current, pathBuilder(current), count, ex))

    data.pull.uncons.flatMap {
      case None =>
        for {
          now <- Pull.eval(F.realTimeInstant)
          _ <- Pull.output1[F, RotateFile](
            RotateFile(
              create = current,
              closed = now,
              url = pathBuilder(current),
              recordCount = count
            ))
        } yield ()

      case Some((as, stream)) =>
        val dataSize = as.size
        // invariant: count is always <= sizeLimit
        if ((dataSize + count) <= sizeLimit) {
          writeChunk(as) >> do_work(get_writer, hotswap, stream, current, dataSize + count)
        } else {
          val splitIndex = math.min(sizeLimit - count, dataSize.toLong).toInt
          val (first, second) = as.splitAt(splitIndex)

          for {
            _ <- writeChunk(first)
            now <- Pull.eval(F.realTimeInstant) // end of current tick
            next = current.copy(index = current.index + 1, time = now.atZone(current.time.getZone))
            _ <- Pull.eval(hotswap.swap(get_writer(pathBuilder(next))))
            _ <- Pull.output1[F, RotateFile](
              RotateFile(
                create = current,
                closed = now,
                url = pathBuilder(current),
                recordCount = count + first.size
              )
            )
            _ <- do_work(get_writer, hotswap, stream.cons(second), next, 0L)
          } yield ()
        }
    }
  }

  /** Bootstrap the rotation: mint a seed `CreateRotateFile` from a fresh `Tick.seed`, open the first file,
    * and run `do_work` over the data.
    *
    * Unlike `RotateByPolicyImpl`, there is no external tick stream to draw the first boundary from, so the
    * seed tick is generated here (its `index + 1` becomes the first file's index, `zoned(_.commence)` its
    * opening instant) and seeds the `NonEmptyHotswap`. All later boundaries are minted inside `do_work`.
    */
  private def persist[A](data: Stream[F, A], get_writer: GetWriter[A]): Stream[F, RotateFile] = {
    val resources: Resource[F, (NonEmptyHotswap[F, HadoopWriter[F, A]], CreateRotateFile)] =
      Resource.eval(Tick.seed[F](zoneId)).flatMap { tick =>
        val crf = CreateRotateFile(tick.sequenceId, tick.index + 1, tick.zoned(_.commence))
        NonEmptyHotswap(get_writer(pathBuilder(crf))).map((_, crf))
      }

    Stream.resource(resources).flatMap { case (hotswap, crf) =>
      do_work(get_writer = get_writer, hotswap = hotswap, data = data, current = crf, count = 0L).stream
    }
  }

  /** Schema-less `GenericRecord` sinks: pull the first non-empty chunk to read its Avro `Schema`, then
    * persist the (undisturbed) stream with a writer built for that schema.
    *
    * `stepLeg` yields the first leg without consuming it; the schema is taken from its head record and the
    * leg is put back via `cons(leg.head)` so no record is lost. This assumes every record shares the first
    * one's schema. An empty stream yields no file. The schema-bound overloads (in the parent trait) skip this
    * step because the caller supplies the schema up front.
    */
  private def generic_record_stream_step_leg(get_writer: Schema => GetWriter[GenericRecord])(
    ss: Stream[F, GenericRecord]): Stream[F, RotateFile] =
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
    val get_writer: GetWriter[Seq[String]] =
      Reader(url => HadoopWriter.kantanR[F](configuration, url, csvConfiguration))

    (ss: Stream[F, Seq[String]]) => persist(ss, get_writer)
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
  override def jsonNode(objectWriter: ObjectWriter): Pipe[F, JsonNode, RotateFile] = {
    val get_writer: GetWriter[JsonNode] =
      Reader(url => HadoopWriter.jsonNodeR(configuration, url, objectWriter))

    (ss: Stream[F, JsonNode]) => persist(ss, get_writer)
  }
}
