package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.spark.RddExt
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

import java.io.ByteArrayOutputStream

final class SaveProtobuf[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveProtobuf[F, A] =
    new SaveProtobuf[F, A](rdd, cfg)

  def overwrite: SaveProtobuf[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveProtobuf[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveProtobuf[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveProtobuf[F, A] = updateConfig(cfg.withOutPutPath(path))

  def file: SaveProtobuf[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveProtobuf[F, A] = updateConfig(cfg.withFolder)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], enc: A <:< GeneratedMessage): F[Unit] = {
    val hadoopConfiguration = new Configuration(rdd.sparkContext.hadoopConfiguration)

    def bytesWritable(a: A): BytesWritable = {
      val os: ByteArrayOutputStream = new ByteArrayOutputStream()
      enc(a).writeDelimitedTo(os)
      os.close()
      new BytesWritable(os.toByteArray)
    }

    val sma: SaveModeAware[F] =
      new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)

    params.compression.ccg[F](hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        sma.checkAndRun(blocker)(
          rdd.stream[F].through(sinks.protobuf(params.outPath, hadoopConfiguration, blocker)).compile.drain)

      case FolderOrFile.Folder =>
        rdd.sparkContext.hadoopConfiguration.set(NJBinaryOutputFormat.suffix, params.format.suffix)
        sma.checkAndRun(blocker)(F.delay {
          rdd.sparkContext.hadoopConfiguration.addResource(hadoopConfiguration)
          rdd
            .map(x => (NullWritable.get(), bytesWritable(x)))
            .saveAsNewAPIHadoopFile[NJBinaryOutputFormat](params.outPath)
        })

    }
  }
}
