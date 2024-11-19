package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import fs2.{Chunk, Pipe, Stream}
import io.lemonlabs.uri.Url
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration

import java.time.ZoneId

final class HadoopAvro[F[_]] private (
  configuration: Configuration,
  schema: Schema,
  compression: AvroCompression)
    extends HadoopSink[F, GenericRecord] {
  // config

  def withCompression(compression: AvroCompression): HadoopAvro[F] =
    new HadoopAvro[F](configuration, schema, compression)
  def withCompression(f: AvroCompression.type => AvroCompression): HadoopAvro[F] =
    withCompression(f(AvroCompression))

  // read

  def source(path: Url, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    HadoopReader.avroS(configuration, schema, toHadoopPath(path), chunkSize)

  // write

  def sink(path: Url)(implicit F: Sync[F]): Pipe[F, Chunk[GenericRecord], Int] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream
        .resource(HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, toHadoopPath(path)))
        .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  def sink(paths: Stream[F, Url])(implicit F: Async[F]): Pipe[F, Chunk[GenericRecord], Int] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, toHadoopPath(url))

    (ss: Stream[F, Chunk[GenericRecord]]) => periodically.persist(ss, paths, get_writer)
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => Url)(implicit
    F: Async[F]): Pipe[F, Chunk[GenericRecord], Int] =
    sink(tickStream.fromZero(policy, zoneId).map(pathBuilder))
}

object HadoopAvro {
  def apply[F[_]](cfg: Configuration, schema: Schema): HadoopAvro[F] =
    new HadoopAvro[F](cfg, schema, NJCompression.Uncompressed)
}
