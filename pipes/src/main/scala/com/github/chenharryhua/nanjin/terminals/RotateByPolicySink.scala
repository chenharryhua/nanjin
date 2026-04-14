package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.NonEmptyHotswap
import cats.syntax.monadError.given
import com.fasterxml.jackson.databind.JsonNode
import fs2.{Chunk, Pipe, Pull, Stream}
import io.circe.Json
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter.Builder
import scalapb.GeneratedMessage

final private class RotateByPolicySink[F[_]: Async](
  configuration: Configuration,
  pathBuilder: CreateRotateFile => Url,
  rotateSequence: Stream[F, CreateRotateFile])
    extends RotateByPolicy[F] {

  private type GetWriter[A] = Reader[Url, Resource[F, HadoopWriter[F, A]]]

  private def doWork[A](
    getWriter: GetWriter[A],
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
              doWork(getWriter, hotswap, tail, current, count + data.size)
          case Right(next) =>
            for {
              _ <- Pull.eval(hotswap.swap(getWriter(pathBuilder(next))))
              _ <- Pull.output1[F, RotateFile](
                RotateFile(
                  create = current,
                  closed = next.time.toInstant,
                  url = pathBuilder(current),
                  recordCount = count
                ))
              _ <- doWork(getWriter, hotswap, tail, next, 0L)
            } yield ()
        }
    }

  private def persist[A](data: Stream[F, Chunk[A]], getWriter: GetWriter[A]): Pull[F, RotateFile, Unit] =
    rotateSequence.pull.uncons1.flatMap {
      case None               => Pull.done
      case Some((head, tail)) => // use the very first tick to build writer and hotswap
        Stream
          .resource(NonEmptyHotswap(getWriter(pathBuilder(head))))
          .flatMap { hotswap =>
            doWork(
              getWriter = getWriter,
              hotswap = hotswap,
              merged = data.map(Left(_)).mergeHaltBoth(tail.map(Right(_))),
              current = head,
              count = 0L).stream
          }
          .pull
          .echo
    }

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
      HadoopWriter.parquetR[F](default_parquet_write_builder(configuration, schema, f).map(f), url)
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
    val get_writer: GetWriter[String] =
      Reader(url =>
        HadoopWriter.csvStringR[F](configuration, url).evalTap(_.write(headerWithCrlf(csvConfiguration))))

    (ss: Stream[F, Seq[String]]) => persist(ss.map(csvRow(csvConfiguration)).chunks, get_writer).stream
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
  override def jsonNode: Pipe[F, JsonNode, RotateFile] = {
    val get_writer: GetWriter[JsonNode] =
      Reader(url => HadoopWriter.jsonNodeR(configuration, url))

    (ss: Stream[F, JsonNode]) => persist(ss.chunks, get_writer).stream
  }
}
