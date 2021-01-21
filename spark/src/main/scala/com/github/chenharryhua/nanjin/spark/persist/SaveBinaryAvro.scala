package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, ContextShift, Sync}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.{BinaryAvroSerialization, GenericRecordCodec}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{AvroOutputStream, Encoder => AvroEncoder}
import fs2.Pipe
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.CompressionCodecs

import java.io.ByteArrayOutputStream

final class SaveBinaryAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig) extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, encoder, cfg)

  def append: SaveBinaryAvro[F, A]         = updateConfig(cfg.withAppend)
  def overwrite: SaveBinaryAvro[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveBinaryAvro[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveBinaryAvro[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveBinaryAvro[F, A] = updateConfig(cfg.withOutPutPath(path))

  def file: SaveBinaryAvro[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveBinaryAvro[F, A] = updateConfig(cfg.withFolder)

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

    CompressionCodecs.setCodecConfiguration(
      hadoopConfiguration,
      CompressionCodecs.getCodecClassName(params.compression.name))

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        sma.checkAndRun(blocker)(
          rdd.stream[F].through(sinks.binAvro(params.outPath, hadoopConfiguration, encoder, blocker)).compile.drain)

      case FolderOrFile.Folder =>
        hadoopConfiguration.set(NJBinaryOutputFormat.suffix, params.format.suffix)
        rdd.sparkContext.hadoopConfiguration.addResource(hadoopConfiguration)
        sma.checkAndRun(blocker)(
          F.delay(
            rdd
              .map(x => (NullWritable.get(), bytesWritable(x)))
              .saveAsNewAPIHadoopFile[NJBinaryOutputFormat](params.outPath)))

    }
  }
}
