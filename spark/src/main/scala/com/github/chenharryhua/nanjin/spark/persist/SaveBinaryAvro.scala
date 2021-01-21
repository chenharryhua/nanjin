package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{AvroOutputStream, Encoder => AvroEncoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.CompressionCodecs

import java.io.ByteArrayOutputStream

final class SaveBinaryAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  def file: SaveSingleBinaryAvro[F, A]  = new SaveSingleBinaryAvro[F, A](rdd, encoder, cfg)
  def folder: SaveMultiBinaryAvro[F, A] = new SaveMultiBinaryAvro[F, A](rdd, encoder, cfg)
}

final class SaveSingleBinaryAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, encoder, cfg)

  def overwrite: SaveBinaryAvro[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveBinaryAvro[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveBinaryAvro[F, A] = updateConfig(cfg.withIgnore)

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val hadoopConfiguration   = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)
    sma.checkAndRun(blocker)(
      rdd.stream[F].through(sinks.binAvro(params.outPath, hadoopConfiguration, encoder, blocker)).compile.drain)
  }
}

final class SaveMultiBinaryAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveMultiBinaryAvro[F, A] =
    new SaveMultiBinaryAvro[F, A](rdd, encoder, cfg)

  def append: SaveMultiBinaryAvro[F, A]         = updateConfig(cfg.withAppend)
  def overwrite: SaveMultiBinaryAvro[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveMultiBinaryAvro[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveMultiBinaryAvro[F, A] = updateConfig(cfg.withIgnore)

  private def bytesWritable(a: A): BytesWritable = {
    val os  = new ByteArrayOutputStream()
    val aos = AvroOutputStream.binary(encoder).to(os).build()
    aos.write(a)
    aos.flush()
    aos.close()
    new BytesWritable(os.toByteArray)
  }

  def run(blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] = {
    val hadoopConfiguration   = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)
    sma.checkAndRun(blocker)(F.delay {
      CompressionCodecs
        .setCodecConfiguration(hadoopConfiguration, CompressionCodecs.getCodecClassName(params.compression.name))
      hadoopConfiguration.set(NJBinaryOutputFormat.suffix, params.format.suffix)
      rdd.sparkContext.hadoopConfiguration.addResource(hadoopConfiguration)
      rdd.map(x => (NullWritable.get(), bytesWritable(x))).saveAsNewAPIHadoopFile[NJBinaryOutputFormat](params.outPath)
    })
  }
}
