package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource, Sync}
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.TickedValue
import fs2.{Chunk, Pipe, Stream}
import io.lemonlabs.uri.Url
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
  writeBuilder: Reader[Path, AvroParquetWriter.Builder[GenericRecord]])
    extends HadoopSink[F, GenericRecord] {

  // config

  def updateReader(f: Endo[ParquetReader.Builder[GenericData.Record]]): HadoopParquet[F] =
    new HadoopParquet(readBuilder.map(f), writeBuilder)

  def updateWriter(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): HadoopParquet[F] =
    new HadoopParquet(readBuilder, writeBuilder.map(f))

  // read

  def source(path: Url, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    HadoopReader.parquetS(readBuilder, toHadoopPath(path), chunkSize)

  // write

  override def sink(path: Url)(implicit F: Sync[F]): Pipe[F, Chunk[GenericRecord], Int] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream
        .resource(HadoopWriter.parquetR[F](writeBuilder, toHadoopPath(path)))
        .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  override def rotateSink(paths: Stream[F, TickedValue[Url]])(implicit
    F: Async[F]): Pipe[F, Chunk[GenericRecord], TickedValue[Int]] = {
    def get_writer(url: Url): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.parquetR[F](writeBuilder, toHadoopPath(url))

    (ss: Stream[F, Chunk[GenericRecord]]) => periodically.persist(ss, paths, get_writer)
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
