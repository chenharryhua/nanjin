package com.github.chenharryhua.nanjin.spark.persist

import cats.{Eq, Parallel}
import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import io.circe.{Encoder => JsonEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveCirce[F[_], A: ClassTag](rdd: RDD[A], cfg: HoarderConfig)(implicit
  jsonEncoder: JsonEncoder[A],
  codec: AvroCodec[A],
  ss: SparkSession)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, cfg)

  def file: SaveCirce[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveCirce[F, A] = updateConfig(cfg.withFolder)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        sma.checkAndRun(blocker)(
          rdd
            .map(codec.idConversion)
            .stream[F]
            .through(fileSink[F](blocker).circe(params.outPath))
            .compile
            .drain)
      case FolderOrFile.Folder =>
        sma.checkAndRun(blocker)(F.delay(
          rdd.map(a => jsonEncoder(codec.idConversion(a)).noSpaces).saveAsTextFile(params.outPath)))
    }
  }
}

final class PartitionCirce[F[_], A: ClassTag, K: ClassTag: Eq](
  rdd: RDD[A],
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)(implicit
  jsonEncoder: JsonEncoder[A],
  codec: AvroCodec[A],
  ss: SparkSession)
    extends AbstractPartition[F, A, K] {

  val params: HoarderParams = cfg.evalConfig

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F], P: Parallel[F]): F[Unit] =
    savePartition(
      blocker,
      rdd,
      params.parallelism,
      params.format,
      bucketing,
      pathBuilder,
      (r, p) => new SaveCirce[F, A](r, cfg.withOutPutPath(p)).run(blocker))
}
