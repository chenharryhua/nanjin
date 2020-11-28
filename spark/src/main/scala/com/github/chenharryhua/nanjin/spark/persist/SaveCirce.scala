package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.pipes.CirceSerialization
import com.github.chenharryhua.nanjin.spark.RddExt
import io.circe.{Json, Encoder => JsonEncoder}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveCirce[F[_], A](
  rdd: RDD[A],
  codec: AvroCodec[A],
  cfg: HoarderConfig,
  isKeepNull: Boolean)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, codec, cfg, isKeepNull)

  def keepNull: SaveCirce[F, A] = new SaveCirce[F, A](rdd, codec, cfg, true)
  def dropNull: SaveCirce[F, A] = new SaveCirce[F, A](rdd, codec, cfg, false)

  def file: SaveCirce[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveCirce[F, A] = updateConfig(cfg.withFolder)

  def gzip: SaveCirce[F, A] = updateConfig(cfg.withCompression(Compression.Gzip))

  def deflate(level: Int): SaveCirce[F, A] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    cs: ContextShift[F],
    ss: SparkSession,
    jsonEncoder: JsonEncoder[A],
    tag: ClassTag[A]): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)

    val ccg = params.compression.ccg[F](ss.sparkContext.hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop = NJHadoop[F](ss.sparkContext.hadoopConfiguration, blocker)
        val pipe   = new CirceSerialization[F, A]
        sma.checkAndRun(blocker)(
          rdd
            .map(codec.idConversion)
            .stream[F]
            .through(pipe.serialize(isKeepNull))
            .through(ccg.compressionPipe)
            .through(hadoop.byteSink(params.outPath))
            .compile
            .drain)
      case FolderOrFile.Folder =>
        def encode(a: A): Json =
          if (isKeepNull)
            jsonEncoder(codec.idConversion(a))
          else
            jsonEncoder(codec.idConversion(a)).deepDropNullValues
        ss.sparkContext.hadoopConfiguration.set(NJTextOutputFormat.suffix, ".circe.json")
        sma.checkAndRun(blocker)(
          F.delay(
            rdd
              .map(x => (NullWritable.get(), new Text(encode(x).noSpaces)))
              .saveAsNewAPIHadoopFile[NJTextOutputFormat](params.outPath)))
    }
  }
}
