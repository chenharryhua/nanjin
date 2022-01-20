package com.github.chenharryhua.nanjin.spark.persist

import cats.data.Kleisli
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.{Pipe, Stream}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.spark.sql.Dataset

final class SaveParquet[F[_], A](ds: Dataset[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  def file: SaveSingleParquet[F, A] = {
    val params: HoarderParams    = cfg.evalConfig
    val hadoopCfg: Configuration = ds.sparkSession.sparkContext.hadoopConfiguration
    val builder: AvroParquetWriter.Builder[GenericRecord] = AvroParquetWriter
      .builder[GenericRecord](HadoopOutputFile.fromPath(new Path(params.outPath), hadoopCfg))
      .withDataModel(GenericData.get())
      .withSchema(encoder.schema)
      .withConf(hadoopCfg)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
    new SaveSingleParquet[F, A](ds, cfg, encoder, builder, None)
  }
  def folder: SaveMultiParquet[F, A] = new SaveMultiParquet[F, A](ds, cfg)
}

final class SaveSingleParquet[F[_], A](
  ds: Dataset[A],
  cfg: HoarderConfig,
  encoder: AvroEncoder[A],
  builder: AvroParquetWriter.Builder[GenericRecord],
  listener: Option[Kleisli[F, A, Unit]])
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleParquet[F, A] =
    new SaveSingleParquet[F, A](ds, cfg, encoder, builder, listener)

  def updateBuilder(
    f: AvroParquetWriter.Builder[GenericRecord] => AvroParquetWriter.Builder[GenericRecord]): SaveSingleParquet[F, A] =
    new SaveSingleParquet[F, A](ds, cfg, encoder, f(builder), listener)

  def withChunkSize(cs: ChunkSize): SaveSingleParquet[F, A] = updateConfig(cfg.chunkSize(cs))
  def withListener(f: A => F[Unit]): SaveSingleParquet[F, A] =
    new SaveSingleParquet[F, A](ds, cfg, encoder, builder, Some(Kleisli(f)))

  def sink(implicit F: Sync[F]): Stream[F, Unit] = {
    val hc: Configuration     = ds.sparkSession.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)
    val src: Stream[F, A]     = ds.rdd.stream[F](params.chunkSize)
    val tgt: Pipe[F, A, Unit] = sinks.parquet(builder, encoder)
    val ss: Stream[F, Unit]   = listener.fold(src.through(tgt))(k => src.evalTap(k.run).through(tgt))

    sma.checkAndRun(ss)
  }
}

final class SaveMultiParquet[F[_], A](ds: Dataset[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiParquet[F, A] =
    new SaveMultiParquet[F, A](ds, cfg)

  def append: SaveMultiParquet[F, A]         = updateConfig(cfg.appendMode)
  def overwrite: SaveMultiParquet[F, A]      = updateConfig(cfg.overwriteMode)
  def errorIfExists: SaveMultiParquet[F, A]  = updateConfig(cfg.errorMode)
  def ignoreIfExists: SaveMultiParquet[F, A] = updateConfig(cfg.ignoreMode)

  def zstd(level: Int): SaveMultiParquet[F, A] = updateConfig(cfg.outputCompression(Compression.Zstandard(level)))
  def lz4: SaveMultiParquet[F, A]              = updateConfig(cfg.outputCompression(Compression.Lz4))
  def snappy: SaveMultiParquet[F, A]           = updateConfig(cfg.outputCompression(Compression.Snappy))
  def gzip: SaveMultiParquet[F, A]             = updateConfig(cfg.outputCompression(Compression.Gzip))
  def uncompress: SaveMultiParquet[F, A]       = updateConfig(cfg.outputCompression(Compression.Uncompressed))

  def run(implicit F: Sync[F]): F[Unit] =
    new SaveModeAware[F](params.saveMode, params.outPath, ds.sparkSession.sparkContext.hadoopConfiguration)
      .checkAndRun(F.interruptibleMany {
        ds.write.option("compression", params.compression.name).mode(params.saveMode).parquet(params.outPath)
      })
}
