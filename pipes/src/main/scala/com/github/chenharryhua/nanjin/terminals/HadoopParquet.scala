package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.{Chunk, Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetReader}

import java.time.ZoneId

final class HadoopParquet[F[_]] private (
  readBuilder: Reader[Path, ParquetReader.Builder[GenericData.Record]],
  writeBuilder: Reader[Path, AvroParquetWriter.Builder[GenericRecord]])
    extends GenericRecordSink[F] {

  // config

  def updateReader(f: Endo[ParquetReader.Builder[GenericData.Record]]): HadoopParquet[F] =
    new HadoopParquet(readBuilder.map(f), writeBuilder)

  def updateWriter(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): HadoopParquet[F] =
    new HadoopParquet(readBuilder, writeBuilder.map(f))

  // read

  def source(path: NJPath, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    Stream.resource(HadoopReader.parquetR(readBuilder, path.hadoopPath)).flatMap { pr =>
      val iterator = Iterator.continually(Option(pr.read())).takeWhile(_.nonEmpty).map(_.get)
      Stream.fromBlockingIterator[F](iterator, chunkSize.value)
    }

  def source(paths: List[NJPath], chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    paths.foldLeft(Stream.empty.covaryAll[F, GenericData.Record]) { case (s, p) => s ++ source(p, chunkSize) }

  // write

  private def get_writerR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    HadoopWriter.parquetR[F](writeBuilder, path)

  override def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, GenericRecord, Int] = {
    (ss: Stream[F, GenericRecord]) =>
      Stream
        .resource(get_writerR(path.hadoopPath))
        .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  override def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, GenericRecord, Int] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      get_writerR(pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, GenericRecord]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        val ticks: Stream[F, Either[Chunk[GenericRecord], Tick]] = tickStream[F](zero).map(Right(_))

        Stream.resource(Hotswap(getWriter(zero.tick))).flatMap { case (hotswap, writer) =>
          persist[F, GenericRecord](
            getWriter,
            hotswap,
            writer,
            ss.chunks.map(Left(_)).mergeHaltBoth(ticks)
          ).stream
        }
      }
  }
}

object HadoopParquet {
  def apply[F[_]](cfg: Configuration, schema: Schema): HadoopParquet[F] =
    new HadoopParquet[F](
      readBuilder = Reader((path: Path) =>
        AvroParquetReader
          .builder[GenericData.Record](HadoopInputFile.fromPath(path, cfg))
          .withDataModel(GenericData.get())
          .withConf(cfg)),
      writeBuilder = Reader((path: Path) =>
        AvroParquetWriter
          .builder[GenericRecord](HadoopOutputFile.fromPath(path, cfg))
          .withDataModel(GenericData.get())
          .withConf(cfg)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE))
    )
}
