package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.NonEmptyHotswap
import cats.syntax.monadError.given
import com.fasterxml.jackson.databind.{JsonNode, ObjectWriter}
import fs2.{Chunk, Pipe, Pull, Stream}
import io.circe.Json
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter.Builder
import scalapb.GeneratedMessage

/** `RotateByPolicy` implementation that rotates output files on a time/policy schedule.
  *
  * The driver is `rotateSequence`, a stream of `CreateRotateFile` ticks produced from a `Policy` (see
  * `Hadoop.rotateSink`); each tick marks the boundary at which the current file should be closed and the next
  * one opened. Every sink method reduces to the same core: build a `GetWriter[A]` (how to open a
  * `HadoopWriter` for a given `Url`) and hand the incoming records plus the tick stream to `persist`.
  *
  * Rotation mechanics:
  *   - Incoming data chunks and rotation ticks are merged into one stream of `Either[Chunk[A],
  *     CreateRotateFile]` (`Left` = data, `Right` = rotate) via `mergeHaltBoth`, so the loop stops when
  *     '''either''' the data or the tick stream ends.
  *   - A `NonEmptyHotswap` holds the currently open writer. On a `Right` tick the loop swaps in a fresh
  *     writer for the next file (releasing the previous one) and emits a `RotateFile` describing the file
  *     just closed. On a `Left` chunk it writes to the current writer and advances the running record count.
  *   - Because it is hotswap-backed, exactly one writer (hence one open file) exists at a time, and the swap
  *     guarantees the previous file is finalized before the next is created.
  *
  * File naming and boundaries come from `pathBuilder`, applied to the `CreateRotateFile` of the file being
  * written; the emitted `RotateFile` carries the opening tick (`create`), the closing instant, the resolved
  * `url`, and the record count for that file.
  *
  * @param pathBuilder
  *   maps each rotation tick to the output `Url` for the file it opens
  * @param rotateSequence
  *   the stream of rotation ticks; its first element opens the first file and each subsequent element closes
  *   the current file and opens the next
  */
final private class RotateByPolicyImpl[F[_]: Async](
  configuration: Configuration,
  pathBuilder: CreateRotateFile => Url,
  rotateSequence: Stream[F, CreateRotateFile])
    extends RotateByPolicy[F] {

  /** How to obtain a writer for a resolved `Url`: a `Reader` from `Url` to a `Resource`-managed
    * `HadoopWriter`. Deferring on `Url` lets each rotation open a distinct file with the same recipe.
    */
  private type GetWriter[A] = Reader[Url, Resource[F, HadoopWriter[F, A]]]

  /** The core rotation loop, expressed as a `Pull` that folds over the merged data/tick stream and outputs
    * one `RotateFile` per completed file.
    *
    * On each element:
    *   - `Left(data)`: write the chunk to the current writer (obtained from the `hotswap`) and recurse with
    *     `count + data.size`. A write failure is wrapped in `RotateWriteException` carrying the current tick,
    *     path, and the count '''so far''' (the offset at which the write failed, not a grand total).
    *   - `Right(next)`: swap the hotswap to a writer for `next`'s path (finalizing the current file), emit a
    *     `RotateFile` for the file just closed (opened at `current`, closed at `next.time`, with `count`
    *     records), then recurse with `current = next` and the count reset to `0`.
    *   - end of stream (`None`): emit a final `RotateFile` for the still-open `current` file, timestamped
    *     with the current wall clock since no closing tick arrived.
    *
    * @param current
    *   the tick that opened the file currently being written
    * @param count
    *   records written to the current file so far
    */
  private def do_work[A](
    get_writer: GetWriter[A],
    hotswap: NonEmptyHotswap[F, HadoopWriter[F, A]],
    merged: Stream[F, Either[Chunk[A], CreateRotateFile]],
    current: CreateRotateFile,
    count: Long
  ): Pull[F, RotateFile, Unit] =
    merged.pull.uncons1.flatMap {
      case None =>
        Pull.eval(Async[F].realTimeInstant).flatMap(now =>
          Pull.output1[F, RotateFile](RotateFile(current, now, pathBuilder(current), count)))

      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            Pull.eval(hotswap.get.use(_.write(data)).adaptError(ex =>
              RotateWriteException(current, pathBuilder(current), count, ex))) >>
              do_work(get_writer, hotswap, tail, current, count + data.size)
          case Right(next) =>
            for {
              _ <- Pull.eval(hotswap.swap(get_writer(pathBuilder(next))))
              _ <- Pull.output1[F, RotateFile](
                RotateFile(
                  create = current,
                  closed = next.time.toInstant,
                  url = pathBuilder(current),
                  recordCount = count
                ))
              _ <- do_work(get_writer, hotswap, tail, next, 0L)
            } yield ()
        }
    }

  /** Bootstrap the rotation: pull the first tick off `rotateSequence` to open the initial file, then run
    * `do_work` over the merge of `data` (as `Left`) and the remaining ticks (as `Right`).
    *
    * The first tick (`head`) both names the initial file and seeds the `NonEmptyHotswap` (which requires an
    * initial resource, hence "non-empty"). If `rotateSequence` is empty there is nothing to write and the
    * pull completes immediately. The `data` stream is chunk-preserving: each upstream chunk becomes one
    * `write`.
    */
  private def persist[A](data: Stream[F, Chunk[A]], get_writer: GetWriter[A]): Pull[F, RotateFile, Unit] =
    rotateSequence.pull.uncons1.flatMap {
      case None               => Pull.done
      case Some((head, tail)) => // use the very first tick to build writer and hotswap
        Stream
          .resource(NonEmptyHotswap(get_writer(pathBuilder(head))))
          .flatMap { hotswap =>
            do_work(
              get_writer = get_writer,
              hotswap = hotswap,
              merged = data.map(Left(_)).mergeHaltBoth(tail.map(Right(_))),
              current = head,
              count = 0L).stream
          }
          .pull
          .echo
    }

  /** Schema-less `GenericRecord` sinks: peek the first record to obtain its Avro `Schema`, then persist the
    * (undisturbed) stream with a writer built for that schema.
    *
    * `peek1` inspects the head without consuming it, so the record used to derive the schema is still
    * written. This assumes every record shares the first record's schema. An empty stream yields no file. The
    * schema-bound overloads skip this step because the caller supplies the schema up front.
    */
  private def generic_record_stream_peek_one(get_writer: Schema => GetWriter[GenericRecord])(
    ss: Stream[F, GenericRecord]): Stream[F, RotateFile] =
    ss.pull.peek1.flatMap {
      case Some((gr, stream)) =>
        persist(stream.chunks, get_writer(gr.getSchema))
      case None => Pull.done
    }.stream

  /*
   * sinks
   */

  // avro - schema-less
  override def avro(compression: AvroCompression): Sink[GenericRecord] = {
    def get_writer(schema: Schema): GetWriter[GenericRecord] =
      Reader(url => HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, url))

    generic_record_stream_peek_one(get_writer)
  }

  // avro schema
  override def avro(schema: Schema, compression: AvroCompression): Sink[GenericRecord] = {
    val get_writer: GetWriter[GenericRecord] =
      Reader(url => HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, url))

    (ss: Stream[F, GenericRecord]) => persist(ss.chunks, get_writer).stream
  }

  // binary avro
  override val binAvro: Sink[GenericRecord] = {
    def get_writer(schema: Schema): GetWriter[GenericRecord] =
      Reader(url => HadoopWriter.binAvroR[F](configuration, schema, url))

    generic_record_stream_peek_one(get_writer)
  }

  override def binAvro(schema: Schema): Sink[GenericRecord] = {
    val get_writer: GetWriter[GenericRecord] =
      Reader(url => HadoopWriter.binAvroR[F](configuration, schema, url))

    (ss: Stream[F, GenericRecord]) => persist(ss.chunks, get_writer).stream
  }

  // jackson json
  override val jackson: Sink[GenericRecord] = {
    def get_writer(schema: Schema): GetWriter[GenericRecord] =
      Reader(url => HadoopWriter.jacksonR[F](configuration, schema, url))

    generic_record_stream_peek_one(get_writer)
  }

  override def jackson(schema: Schema): Sink[GenericRecord] = {
    val get_writer: GetWriter[GenericRecord] =
      Reader(url => HadoopWriter.jacksonR[F](configuration, schema, url))

    (ss: Stream[F, GenericRecord]) => persist(ss.chunks, get_writer).stream
  }

  // parquet
  override def parquet(f: Endo[Builder[GenericRecord]]): Sink[GenericRecord] = {
    def get_writer(schema: Schema): GetWriter[GenericRecord] = Reader { url =>
      HadoopWriter.parquetR[F](default_parquet_write_builder(configuration, schema, f), url)
    }

    generic_record_stream_peek_one(get_writer)
  }

  override def parquet(schema: Schema, f: Endo[Builder[GenericRecord]]): Sink[GenericRecord] = {
    val get_writer: GetWriter[GenericRecord] = Reader { url =>
      HadoopWriter.parquetR[F](default_parquet_write_builder(configuration, schema, f), url)
    }

    (ss: Stream[F, GenericRecord]) => persist(ss.chunks, get_writer).stream
  }

  // bytes
  override val bytes: Sink[Byte] = {
    val get_writer: GetWriter[Byte] =
      Reader(url => HadoopWriter.byteR[F](configuration, url))

    (ss: Stream[F, Byte]) => persist(ss.chunks, get_writer).stream
  }

  // circe json
  override val circe: Sink[Json] = {
    val get_writer: GetWriter[Json] =
      Reader(url => HadoopWriter.circeR[F](configuration, url))

    (ss: Stream[F, Json]) => persist(ss.chunks, get_writer).stream
  }

  // kantan csv
  override def kantan(csvConfiguration: CsvConfiguration): Sink[Seq[String]] = {
    val get_writer: GetWriter[Seq[String]] =
      Reader(url => HadoopWriter.kantanR[F](configuration, url, csvConfiguration))

    (ss: Stream[F, Seq[String]]) => persist(ss.chunks, get_writer).stream
  }

  // text
  override val text: Sink[String] = {
    val get_writer: GetWriter[String] =
      Reader(url => HadoopWriter.stringR(configuration, url))

    (ss: Stream[F, String]) => persist(ss.chunks, get_writer).stream
  }

  // protobuf
  override val protobuf: Sink[GeneratedMessage] = {
    val get_writer: GetWriter[GeneratedMessage] =
      Reader(url => HadoopWriter.protobufR(configuration, url))

    (ss: Stream[F, GeneratedMessage]) => persist(ss.chunks, get_writer).stream
  }

  // json node
  override def jsonNode(objectWriter: ObjectWriter): Pipe[F, JsonNode, RotateFile] = {
    val get_writer: GetWriter[JsonNode] =
      Reader(url => HadoopWriter.jsonNodeR(configuration, url, objectWriter))

    (ss: Stream[F, JsonNode]) => persist(ss.chunks, get_writer).stream
  }
}
