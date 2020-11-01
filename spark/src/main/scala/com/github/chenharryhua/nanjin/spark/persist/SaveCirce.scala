package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.pipes.CirceSerialization
import com.github.chenharryhua.nanjin.spark.RddExt
import io.circe.{Json, Encoder => JsonEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveCirce[F[_], A](
  rdd: RDD[A],
  codec: AvroCodec[A],
  isKeepNull: Boolean,
  cfg: HoarderConfig)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, codec, isKeepNull, cfg)

  def keepNull: SaveCirce[F, A] = new SaveCirce[F, A](rdd, codec, true, cfg)
  def dropNull: SaveCirce[F, A] = new SaveCirce[F, A](rdd, codec, false, cfg)

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
        val hadoop = new NJHadoop[F](ss.sparkContext.hadoopConfiguration)
        val pipe   = new CirceSerialization[F, A]
        sma.checkAndRun(blocker)(
          rdd
            .map(codec.idConversion)
            .stream[F]
            .through(pipe.serialize(isKeepNull))
            .through(ccg.compressionPipe)
            .through(hadoop.byteSink(params.outPath, blocker))
            .compile
            .drain)
      case FolderOrFile.Folder =>
        def encode(a: A): Json =
          if (isKeepNull)
            jsonEncoder(codec.idConversion(a))
          else
            jsonEncoder(codec.idConversion(a)).deepDropNullValues

        sma.checkAndRun(blocker)(
          F.delay(rdd.map(encode(_).noSpaces).saveAsTextFile(params.outPath, ccg.klass)))
    }
  }
}
