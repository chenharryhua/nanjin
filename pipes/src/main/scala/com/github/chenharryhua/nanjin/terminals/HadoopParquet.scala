package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.datetime.policies.Policy
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.{Chunk, Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetReader}

final class HadoopParquet[F[_]] private (
  readBuilder: Reader[Path, ParquetReader.Builder[GenericData.Record]],
  writeBuilder: Reader[Path, AvroParquetWriter.Builder[GenericRecord]]) {

  // config

  def updateReader(f: Endo[ParquetReader.Builder[GenericData.Record]]): HadoopParquet[F] =
    new HadoopParquet(readBuilder.map(f), writeBuilder)

  def updateWriter(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): HadoopParquet[F] =
    new HadoopParquet(readBuilder, writeBuilder.map(f))

  // read

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    for {
      rd <- Stream.resource(HadoopReader.parquetR(readBuilder, path.hadoopPath))
      gr <- Stream.repeatEval(F.blocking(Option(rd.read()))).unNoneTerminate
    } yield gr

  def source(paths: List[NJPath])(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    paths.foldLeft(Stream.empty.covaryAll[F, GenericData.Record]) { case (s, p) => s ++ source(p) }

  // write

  private def getWriterR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    HadoopWriter.parquetR[F](writeBuilder, path)

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[GenericRecord], Nothing] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream.resource(getWriterR(path.hadoopPath)).flatMap(w => ss.foreach(w.write))
  }

  def sink(policy: Policy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[GenericRecord], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      getWriterR(pathBuilder(tick).hadoopPath)

    def init(
      tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, GenericRecord]], HadoopWriter[F, GenericRecord])] =
      Hotswap(getWriter(tick))

    // save
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          persist[F, GenericRecord](
            getWriter,
            hotswap,
            writer,
            ss.map(Left(_)).mergeHaltBoth(tickStream[F](policy, zero).map(Right(_)))
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
