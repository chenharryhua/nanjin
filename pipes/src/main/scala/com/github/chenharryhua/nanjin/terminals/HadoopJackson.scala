package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.TickedValue
import fs2.{Chunk, Pipe, Stream}
import io.lemonlabs.uri.Url
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration

final class HadoopJackson[F[_]] private (configuration: Configuration, schema: Schema)
    extends HadoopSink[F, GenericRecord] {

  // read

  def source(path: Url, chunkSize: ChunkSize)(implicit F: Async[F]): Stream[F, GenericData.Record] =
    HadoopReader.jacksonS[F](configuration, schema, toHadoopPath(path), chunkSize)

  // write

  override def sink(path: Url)(implicit F: Sync[F]): Pipe[F, Chunk[GenericRecord], Int] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream
        .resource(HadoopWriter.jacksonR[F](configuration, schema, toHadoopPath(path)))
        .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  override def rotateSink(paths: Stream[F, TickedValue[Url]])(implicit
    F: Async[F]): Pipe[F, Chunk[GenericRecord], TickedValue[Int]] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, toHadoopPath(url))

    (ss: Stream[F, Chunk[GenericRecord]]) => periodically.persist(ss, paths, get_writer)
  }
}

object HadoopJackson {
  def apply[F[_]](configuration: Configuration, schema: Schema): HadoopJackson[F] =
    new HadoopJackson[F](configuration, schema)
}
