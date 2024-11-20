package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.TickedValue
import fs2.{Chunk, Pipe, Stream}
import io.lemonlabs.uri.Url
import org.apache.hadoop.conf.Configuration

final class HadoopText[F[_]] private (configuration: Configuration) extends HadoopSink[F, String] {

  // read

  def source(path: Url, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, String] =
    HadoopReader.stringS[F](configuration, toHadoopPath(path), chunkSize)

  // write

  override def sink(path: Url)(implicit F: Sync[F]): Pipe[F, Chunk[String], Int] = {
    (ss: Stream[F, Chunk[String]]) =>
      Stream
        .resource(HadoopWriter.stringR[F](configuration, toHadoopPath(path)))
        .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  override def rotateSink(paths: Stream[F, TickedValue[Url]])(implicit
    F: Async[F]): Pipe[F, Chunk[String], TickedValue[Int]] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, toHadoopPath(url))

    (ss: Stream[F, Chunk[String]]) => periodically.persist(ss, paths, get_writer)
  }
}

object HadoopText {
  def apply[F[_]](configuration: Configuration): HadoopText[F] = new HadoopText[F](configuration)
}
