package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.spark.RddExt
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import scalapb.GeneratedMessage

import java.io.ByteArrayOutputStream

final class SaveProtobuf[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {
  def file: SaveSingleProtobuf[F, A]  = new SaveSingleProtobuf[F, A](rdd, cfg)
  def folder: SaveMultiProtobuf[F, A] = new SaveMultiProtobuf[F, A](rdd, cfg)
}

final class SaveSingleProtobuf[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveSingleProtobuf[F, A] =
    new SaveSingleProtobuf[F, A](rdd, cfg)

  def overwrite: SaveSingleProtobuf[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveSingleProtobuf[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveSingleProtobuf[F, A] = updateConfig(cfg.withIgnore)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], enc: A <:< GeneratedMessage): F[Unit] = {
    val hadoopConfiguration   = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)
    sma.checkAndRun(blocker)(
      rdd.stream[F].through(sinks.protobuf(params.outPath, hadoopConfiguration, blocker)).compile.drain)
  }
}

final class SaveMultiProtobuf[F[_], A](rdd: RDD[A], cfg: HoarderConfig) extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiProtobuf[F, A] =
    new SaveMultiProtobuf[F, A](rdd, cfg)

  def append: SaveMultiProtobuf[F, A]         = updateConfig(cfg.withAppend)
  def overwrite: SaveMultiProtobuf[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveMultiProtobuf[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveMultiProtobuf[F, A] = updateConfig(cfg.withIgnore)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], enc: A <:< GeneratedMessage): F[Unit] = {

    def bytesWritable(a: A): BytesWritable = {
      val os: ByteArrayOutputStream = new ByteArrayOutputStream()
      enc(a).writeDelimitedTo(os)
      os.close()
      new BytesWritable(os.toByteArray)
    }

    val hadoopConfiguration   = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)

    rdd.sparkContext.hadoopConfiguration.set(NJBinaryOutputFormat.suffix, params.format.suffix)
    sma.checkAndRun(blocker)(F.delay {
      CompressionCodecs
        .setCodecConfiguration(hadoopConfiguration, CompressionCodecs.getCodecClassName(params.compression.name))
      rdd.sparkContext.hadoopConfiguration.addResource(hadoopConfiguration)
      rdd.map(x => (NullWritable.get(), bytesWritable(x))).saveAsNewAPIHadoopFile[NJBinaryOutputFormat](params.outPath)
    })
  }
}
