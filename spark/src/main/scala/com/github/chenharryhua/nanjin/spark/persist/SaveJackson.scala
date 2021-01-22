package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.CompressionCodecs

final class SaveJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  def file: SaveSingleJackson[F, A]  = new SaveSingleJackson[F, A](rdd, encoder, cfg)
  def folder: SaveMultiJackson[F, A] = new SaveMultiJackson[F, A](rdd, encoder, cfg)
}

final class SaveSingleJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleJackson[F, A] =
    new SaveSingleJackson[F, A](rdd, encoder, cfg)

  def overwrite: SaveSingleJackson[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveSingleJackson[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveSingleJackson[F, A] = updateConfig(cfg.withIgnore)

  def gzip: SaveSingleJackson[F, A]                = updateConfig(cfg.withCompression(Compression.Gzip))
  def deflate(level: Int): SaveSingleJackson[F, A] = updateConfig(cfg.withCompression(Compression.Deflate(level)))
  def uncompress: SaveSingleJackson[F, A]          = updateConfig(cfg.withCompression(Compression.Uncompressed))

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val hc: Configuration     = rdd.sparkContext.hadoopConfiguration
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hc)

    sma.checkAndRun(blocker)(
      rdd
        .stream[F]
        .through(sinks.jackson(params.outPath, hc, encoder, params.compression.fs2Compression, blocker))
        .compile
        .drain)
  }
}

final class SaveMultiJackson[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiJackson[F, A] =
    new SaveMultiJackson[F, A](rdd, encoder, cfg)

  def append: SaveMultiJackson[F, A]         = updateConfig(cfg.withAppend)
  def overwrite: SaveMultiJackson[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveMultiJackson[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveMultiJackson[F, A] = updateConfig(cfg.withIgnore)

  def bzip2: SaveMultiJackson[F, A]               = updateConfig(cfg.withCompression(Compression.Bzip2))
  def gzip: SaveMultiJackson[F, A]                = updateConfig(cfg.withCompression(Compression.Gzip))
  def deflate(level: Int): SaveMultiJackson[F, A] = updateConfig(cfg.withCompression(Compression.Deflate(level)))
  def uncompress: SaveMultiJackson[F, A]          = updateConfig(cfg.withCompression(Compression.Uncompressed))

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, config)

    sma.checkAndRun(blocker)(F.delay {
      CompressionCodecs.setCodecConfiguration(config, CompressionCodecs.getCodecClassName(params.compression.name))
      val job = Job.getInstance(config)
      AvroJob.setOutputKeySchema(job, encoder.schema)
      utils
        .genericRecordPair(rdd, encoder)
        .saveAsNewAPIHadoopFile(
          params.outPath,
          classOf[AvroKey[GenericRecord]],
          classOf[NullWritable],
          classOf[NJJacksonKeyOutputFormat],
          job.getConfiguration)
    })
  }
}
