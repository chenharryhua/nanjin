package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.{Dataset, SaveMode}

final class SaveParquet[F[_], A](ds: Dataset[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveParquet[F, A] =
    new SaveParquet[F, A](ds, encoder, cfg)

  def overwrite: SaveParquet[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveParquet[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveParquet[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveParquet[F, A] = updateConfig(cfg.withOutPutPath(path))

  def snappy: SaveParquet[F, A]     = updateConfig(cfg.withCompression(Compression.Snappy))
  def gzip: SaveParquet[F, A]       = updateConfig(cfg.withCompression(Compression.Gzip))
  def uncompress: SaveParquet[F, A] = updateConfig(cfg.withCompression(Compression.Uncompressed))

  def file: SaveParquet[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveParquet[F, A] = updateConfig(cfg.withFolder)

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val hadoopConfiguration = new Configuration(ds.sparkSession.sparkContext.hadoopConfiguration)

    val sma: SaveModeAware[F] =
      new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)

    val ccn: CompressionCodecName = params.compression.parquet

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        sma.checkAndRun(blocker)(
          ds.rdd
            .stream[F]
            .through(sinks.parquet(params.outPath, hadoopConfiguration, encoder, ccn, blocker))
            .compile
            .drain)
      case FolderOrFile.Folder =>
        sma.checkAndRun(blocker)(F.delay {
          ds.sparkSession.sparkContext.hadoopConfiguration.addResource(hadoopConfiguration)
          ds.write.option("compression", ccn.name()).mode(SaveMode.Overwrite).parquet(params.outPath)
        })
    }
  }
}
