package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.time.{awakeEvery, Tick}
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetReader}
import retry.RetryPolicy

final class HadoopParquet[F[_]] private (
  readBuilder: Reader[NJPath, ParquetReader.Builder[GenericRecord]],
  writeBuilder: Reader[NJPath, AvroParquetWriter.Builder[GenericRecord]]) {
  def updateReader(f: Endo[ParquetReader.Builder[GenericRecord]]): HadoopParquet[F] =
    new HadoopParquet(readBuilder.map(f), writeBuilder)

  def updateWriter(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): HadoopParquet[F] =
    new HadoopParquet(readBuilder, writeBuilder.map(f))

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, GenericRecord] =
    for {
      rd <- Stream.resource(HadoopReader.parquet(readBuilder, path))
      gr <- Stream.repeatEval(F.blocking(Option(rd.read()))).unNoneTerminate
    } yield gr

  def source(paths: List[NJPath])(implicit F: Sync[F]): Stream[F, GenericRecord] =
    paths.foldLeft(Stream.empty.covaryAll[F, GenericRecord]) { case (s, p) =>
      s ++ source(p)
    }

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, GenericRecord, Nothing] = {
    (ss: Stream[F, GenericRecord]) =>
      Stream
        .resource(HadoopWriter.parquet[F](writeBuilder, path))
        .flatMap(pw => persist[F, GenericRecord](pw, ss).stream)
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, GenericRecord, Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.parquet[F](writeBuilder, pathBuilder(tick))

    val init: Resource[F, (Hotswap[F, HadoopWriter[F, GenericRecord]], HadoopWriter[F, GenericRecord])] =
      Hotswap(HadoopWriter.parquet[F](writeBuilder, pathBuilder(Tick.Zero)))

    (ss: Stream[F, GenericRecord]) =>
      Stream.resource(init).flatMap { case (hotswap, writer) =>
        rotatePersist[F, GenericRecord](
          getWriter,
          hotswap,
          writer,
          ss.map(Left(_)).mergeHaltL(awakeEvery[F](policy).map(Right(_)))
        ).stream
      }
  }
}

object HadoopParquet {
  def apply[F[_]](cfg: Configuration, schema: Schema): HadoopParquet[F] =
    new HadoopParquet[F](
      readBuilder = Reader((path: NJPath) =>
        AvroParquetReader
          .builder[GenericRecord](HadoopInputFile.fromPath(path.hadoopPath, cfg))
          .withDataModel(GenericData.get())
          .withConf(cfg)),
      writeBuilder = Reader((path: NJPath) =>
        AvroParquetWriter
          .builder[GenericRecord](HadoopOutputFile.fromPath(path.hadoopPath, cfg))
          .withDataModel(GenericData.get())
          .withConf(cfg)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE))
    )
}
