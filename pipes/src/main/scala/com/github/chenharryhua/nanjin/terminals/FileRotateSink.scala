package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.github.chenharryhua.nanjin.common.chrono.TickedValue
import fs2.{Chunk, Pipe, Pull, Stream}
import io.circe.Json
import io.circe.jackson.circeToJackson
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.hadoop.conf.Configuration

final class FileRotateSink[F[_]: Async] private (
  configuration: Configuration,
  paths: Stream[F, TickedValue[Url]]) {

  val bytes: Pipe[F, Chunk[Byte], TickedValue[Int]] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, Byte]] =
      HadoopWriter.byteR[F](configuration, toHadoopPath(url))

    (ss: Stream[F, Chunk[Byte]]) => periodically.persist(ss, paths, get_writer)
  }

  val circe: Pipe[F, Chunk[Json], TickedValue[Int]] = {
    val mapper: ObjectMapper = new ObjectMapper() // create ObjectMapper is expensive

    def get_writer(url: Url): Resource[F, HadoopWriter[F, JsonNode]] =
      HadoopWriter.jsonNodeR[F](configuration, toHadoopPath(url), mapper)

    (ss: Stream[F, Chunk[Json]]) => periodically.persist(ss.map(_.map(circeToJackson)), paths, get_writer)
  }

  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Chunk[Seq[String]], TickedValue[Int]] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.csvStringR[F](configuration, toHadoopPath(url))

    (ss: Stream[F, Chunk[Seq[String]]]) =>
      val encodedSrc: Stream[F, Chunk[String]] = ss.map(_.map(csvRow(csvConfiguration)))
      if (csvConfiguration.hasHeader) {
        val header: Chunk[String] = csvHeader(csvConfiguration)
        paths.pull.uncons1.flatMap {
          case Some((head, tail)) =>
            Stream
              .resource(Hotswap(get_writer(head.value)))
              .flatMap { case (hotswap, writer) =>
                Stream.eval(writer.write(header)) >>
                  periodically
                    .persistCsvWithHeader[F](
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
      } else periodically.persist(encodedSrc, paths, get_writer)
  }

  val text: Pipe[F, Chunk[String], TickedValue[Int]] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, toHadoopPath(url))

    (ss: Stream[F, Chunk[String]]) => periodically.persist(ss, paths, get_writer)
  }
}

object FileRotateSink {
  def apply[F[_]: Async](configuration: Configuration, paths: Stream[F, TickedValue[Url]]) =
    new FileRotateSink(configuration, paths)
}
