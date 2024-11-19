package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.implicits.toFunctorOps
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import fs2.{Chunk, Pipe, Stream}
import io.circe.Json
import io.circe.jackson.circeToJackson
import io.lemonlabs.uri.Url
import org.apache.hadoop.conf.Configuration
import squants.information.{Bytes, Information}

import java.time.ZoneId

final class HadoopCirce[F[_]] private (configuration: Configuration) extends HadoopSink[F, Json] {

  def source(path: Url, bufferSize: Information)(implicit F: Sync[F]): Stream[F, Json] =
    HadoopReader.jawnS[F](configuration, toHadoopPath(path), bufferSize)

  def source(path: Url)(implicit F: Sync[F]): Stream[F, Json] =
    source(path, Bytes(1024 * 512))

  def sink(path: Url)(implicit F: Sync[F]): Pipe[F, Chunk[Json], Int] = { (ss: Stream[F, Chunk[Json]]) =>
    Stream
      .resource(HadoopWriter.jsonNodeR[F](configuration, toHadoopPath(path), new ObjectMapper()))
      .flatMap { w =>
        ss.evalMap(c => w.write(c.map(circeToJackson)).as(c.size))
      }
  }

  def sink(paths: Stream[F, Url])(implicit F: Async[F]): Pipe[F, Chunk[Json], Int] = {
    val mapper: ObjectMapper = new ObjectMapper() // create ObjectMapper is expensive

    def get_writer(url: Url): Resource[F, HadoopWriter[F, JsonNode]] =
      HadoopWriter.jsonNodeR[F](configuration, toHadoopPath(url), mapper)

    (ss: Stream[F, Chunk[Json]]) => periodically.persist(ss.map(_.map(circeToJackson)), paths, get_writer)
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => Url)(implicit
    F: Async[F]): Pipe[F, Chunk[Json], Int] =
    sink(tickStream.fromZero(policy, zoneId).map(pathBuilder))
}

object HadoopCirce {
  def apply[F[_]](cfg: Configuration): HadoopCirce[F] = new HadoopCirce[F](cfg)
}
