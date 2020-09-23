package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.{Eq, Parallel}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.pipes.CirceSerialization
import com.github.chenharryhua.nanjin.spark.RddExt
import io.circe.{Encoder => JsonEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveCirce[F[_], A](rdd: RDD[A], codec: AvroCodec[A], cfg: HoarderConfig)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, codec, cfg)

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

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop = new NJHadoop[F](ss.sparkContext.hadoopConfiguration)
        val pipe   = new CirceSerialization[F, A]
        sma.checkAndRun(blocker)(
          rdd
            .map(codec.idConversion)
            .stream[F]
            .through(pipe.serialize)
            .through(params.compression.ccg.pipe)
            .through(hadoop.byteSink(params.outPath, blocker))
            .compile
            .drain)
      case FolderOrFile.Folder =>
        sma.checkAndRun(blocker)(
          F.delay(
            rdd
              .map(a => jsonEncoder(codec.idConversion(a)).noSpaces)
              .saveAsTextFile(params.outPath, params.compression.ccg.klass)))
    }
  }
}

final class PartitionCirce[F[_], A, K](
  rdd: RDD[A],
  codec: AvroCodec[A],
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)
    extends AbstractPartition[F, A, K] {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): PartitionCirce[F, A, K] =
    new PartitionCirce[F, A, K](rdd, codec, cfg, bucketing, pathBuilder)

  def gzip: PartitionCirce[F, A, K] = updateConfig(cfg.withCompression(Compression.Gzip))

  def deflate(level: Int): PartitionCirce[F, A, K] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F],
    ss: SparkSession,
    jsonEncoder: JsonEncoder[A],
    tagA: ClassTag[A],
    tagK: ClassTag[K],
    eq: Eq[K]): F[Unit] =
    savePartition(
      blocker,
      rdd,
      params.parallelism,
      params.format,
      bucketing,
      pathBuilder,
      (r, p) => new SaveCirce[F, A](r, codec, cfg.withOutPutPath(p)).run(blocker))
}
