package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.pipes.{BinaryAvroSerialization, GenericRecordCodec}
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{AvroOutputStream, Encoder => AvroEncoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD

import java.io.ByteArrayOutputStream

final class SaveBinaryAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, encoder, cfg)

  def overwrite: SaveBinaryAvro[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveBinaryAvro[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveBinaryAvro[F, A] = updateConfig(cfg.withIgnore)

  def outPath(path: String): SaveBinaryAvro[F, A] = updateConfig(cfg.withOutPutPath(path))

  def file: SaveBinaryAvro[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveBinaryAvro[F, A] = updateConfig(cfg.withFolder)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {

    val hadoopConfiguration = new Configuration(rdd.sparkContext.hadoopConfiguration)

    def bytesWritable(a: A): BytesWritable = {
      val os  = new ByteArrayOutputStream()
      val aos = AvroOutputStream.binary(encoder).to(os).build()
      aos.write(a)
      aos.flush()
      aos.close()
      new BytesWritable(os.toByteArray)
    }

    val sma: SaveModeAware[F] =
      new SaveModeAware[F](params.saveMode, params.outPath, hadoopConfiguration)
    params.compression.ccg[F](hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop: NJHadoop[F]              = NJHadoop[F](hadoopConfiguration, blocker)
        val gr: GenericRecordCodec[F, A]     = new GenericRecordCodec[F, A]
        val pipe: BinaryAvroSerialization[F] = new BinaryAvroSerialization[F](encoder.schema)

        sma.checkAndRun(blocker)(
          rdd
            .stream[F]
            .through(gr.encode(encoder))
            .through(pipe.serialize)
            .through(hadoop.byteSink(params.outPath))
            .compile
            .drain)

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
