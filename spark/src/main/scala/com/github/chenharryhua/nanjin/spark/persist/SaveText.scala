package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveText[F[_], A: ClassTag](rdd: RDD[A], cfg: HoarderConfig)(implicit
  show: Show[A],
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveText[F, A] =
    new SaveText[F, A](rdd, cfg)

  def file: SaveText[F, A]   = updateConfig(cfg.withFile)
  def folder: SaveText[F, A] = updateConfig(cfg.withFolder)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    params.singleOrMulti match {
      case FolderOrFile.SingleFile =>
        sma.checkAndRun(blocker)(
          rdd.stream[F].through(fileSink[F](blocker).text(params.outPath)).compile.drain)
      case FolderOrFile.Folder =>
        sma.checkAndRun(blocker)(
          F.delay(rdd.map(a => show.show(codec.idConversion(a))).saveAsTextFile(params.outPath)))
    }
  }
}
