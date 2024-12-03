package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import cats.implicits.toFunctorOps
import com.fasterxml.jackson.databind.ObjectMapper
import fs2.{Chunk, Pipe, Stream}
import io.circe.Json
import io.circe.jackson.circeToJackson
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.hadoop.conf.Configuration

final class FileSink[F[_]: Sync] private (configuration: Configuration, path: Url) {
  val bytes: Pipe[F, Chunk[Byte], Int] = { (ss: Stream[F, Chunk[Byte]]) =>
    Stream
      .resource(HadoopWriter.byteR[F](configuration, toHadoopPath(path)))
      .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  val circe: Pipe[F, Chunk[Json], Int] = { (ss: Stream[F, Chunk[Json]]) =>
    Stream
      .resource(HadoopWriter.jsonNodeR[F](configuration, toHadoopPath(path), new ObjectMapper()))
      .flatMap(w => ss.evalMap(c => w.write(c.map(circeToJackson)).as(c.size)))
  }

  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Chunk[Seq[String]], Int] = {
    (ss: Stream[F, Chunk[Seq[String]]]) =>
      Stream.resource(HadoopWriter.csvStringR[F](configuration, toHadoopPath(path))).flatMap { w =>
        val process: Stream[F, Int] =
          ss.evalMap(c => w.write(c.map(csvRow(csvConfiguration))).as(c.size))

        val header: Stream[F, Unit] = Stream.eval(w.write(csvHeader(csvConfiguration)))

        if (csvConfiguration.hasHeader) header >> process else process
      }
  }

  val text: Pipe[F, Chunk[String], Int] = { (ss: Stream[F, Chunk[String]]) =>
    Stream
      .resource(HadoopWriter.stringR[F](configuration, toHadoopPath(path)))
      .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }
}

object FileSink {
  def apply[F[_]: Sync](configuration: Configuration, path: Url): FileSink[F] =
    new FileSink[F](configuration, path)
}
