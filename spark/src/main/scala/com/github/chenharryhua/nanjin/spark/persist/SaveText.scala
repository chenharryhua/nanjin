package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.TextSerialization
import com.github.chenharryhua.nanjin.spark.RddExt
import fs2.Pipe
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.rdd.RDD

final class SaveText[F[_], A](rdd: RDD[A], cfg: HoarderConfig, suffix: String) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveText[F, A] =
    new SaveText[F, A](rdd, cfg, suffix)

  def overwrite: SaveText[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveText[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveText[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveText[F, A] = updateConfig(cfg.withOutPutPath(path))

  def withSuffix(suffix: String): SaveText[F, A] =
    new SaveText[F, A](rdd, cfg, suffix)

  def file: SaveText[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveText[F, A] = updateConfig(cfg.withFolder)

  def gzip: SaveText[F, A] = updateConfig(cfg.withCompression(Compression.Gzip))

  def deflate(level: Int): SaveText[F, A] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F], show: Show[A]): F[Unit] = {
    val hadoopConfiguration = new Configuration(rdd.sparkContext.hadoopConfiguration)

    val sma: SaveModeAware[F] =
      new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)

    val ccg: CompressionCodecGroup[F] =
      params.compression.ccg[F](hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop: NJHadoop[F]         = NJHadoop[F](hadoopConfiguration, blocker)
        val pipe: Pipe[F, String, Byte] = new TextSerialization[F].serialize
        val sink: Pipe[F, Byte, Unit]   = hadoop.byteSink(params.outPath)
        sma.checkAndRun(blocker)(
          rdd.stream[F].map(show.show).through(pipe).through(ccg.compressionPipe).through(sink).compile.drain)

      case FolderOrFile.Folder =>
        hadoopConfiguration.set(NJTextOutputFormat.suffix, suffix)
        rdd.sparkContext.hadoopConfiguration.addResource(hadoopConfiguration)
        sma.checkAndRun(blocker)(
          F.delay(
            rdd
              .map(a => (NullWritable.get(), new Text(show.show(a))))
              .saveAsNewAPIHadoopFile[NJTextOutputFormat](params.outPath)))
    }
  }
}
