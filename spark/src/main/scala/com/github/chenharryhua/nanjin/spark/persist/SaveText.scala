package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.pipes.TextSerialization
import com.github.chenharryhua.nanjin.spark.RddExt
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveText[F[_], A](rdd: RDD[A], codec: AvroCodec[A], cfg: HoarderConfig, suffix: String)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveText[F, A] =
    new SaveText[F, A](rdd, codec, cfg, suffix)

  def withSuffix(suffix: String): SaveText[F, A] =
    new SaveText[F, A](rdd, codec, cfg, suffix)

  def file: SaveText[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveText[F, A] = updateConfig(cfg.withFolder)

  def gzip: SaveText[F, A] = updateConfig(cfg.withCompression(Compression.Gzip))

  def deflate(level: Int): SaveText[F, A] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    cs: ContextShift[F],
    ss: SparkSession,
    show: Show[A],
    tag: ClassTag[A]): F[Unit] = {

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    val ccg: CompressionCodecGroup[F] =
      params.compression.ccg[F](ss.sparkContext.hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop = NJHadoop[F](ss.sparkContext.hadoopConfiguration, blocker)
        val pipe   = new TextSerialization[F]
        sma.checkAndRun(blocker)(
          rdd
            .stream[F]
            .map(a => show.show(codec.idConversion(a)))
            .through(pipe.serialize)
            .through(ccg.compressionPipe)
            .through(hadoop.byteSink(params.outPath))
            .compile
            .drain)
      case FolderOrFile.Folder =>
        ss.sparkContext.hadoopConfiguration.set(NJTextOutputFormat.suffix, suffix)
        sma.checkAndRun(blocker)(
          F.delay(
            rdd
              .map(a => (NullWritable.get(), new Text(show.show(codec.idConversion(a)))))
              .saveAsNewAPIHadoopFile[NJTextOutputFormat](params.outPath)))
    }
  }
}
