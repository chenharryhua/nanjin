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

final class HadoopBinAvro[F[_]] private (
  configuration: Configuration,
  schema: Schema
) extends HadoopSink[F, GenericRecord] {

  // read

  def source(path: Url, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    HadoopReader.binAvroS[F](configuration, schema, toHadoopPath(path), chunkSize)

  // write

  override def sink(path: Url)(implicit F: Sync[F]): Pipe[F, Chunk[GenericRecord], Int] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream
        .resource(HadoopWriter.binAvroR[F](configuration, schema, toHadoopPath(path)))
        .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  override def sink(paths: Stream[F, TickedValue[Url]])(implicit
    F: Async[F]): Pipe[F, Chunk[GenericRecord], TickedValue[Int]] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.binAvroR[F](configuration, schema, toHadoopPath(url))

    (ss: Stream[F, Chunk[GenericRecord]]) => periodically.persist(ss, paths, get_writer)
  }
}

object HadoopBinAvro {
  def apply[F[_]](configuration: Configuration, schema: Schema): HadoopBinAvro[F] =
    new HadoopBinAvro[F](configuration, schema)
}
