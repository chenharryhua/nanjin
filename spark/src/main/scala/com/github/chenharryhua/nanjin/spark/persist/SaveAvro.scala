package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.{Eq, Parallel}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, RddExt}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.TypedEncoder
import frameless.cats.implicits._
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveAvro[F[_], A: ClassTag](
  rdd: RDD[A],
  codec: AvroCodec[A],
  ote: Option[TypedEncoder[A]],
  cfg: HoarderConfig)(implicit ss: SparkSession)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, codec, ote, cfg)

  def spark(implicit te: TypedEncoder[A]): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, codec, Some(te), cfg)

  def file: SaveAvro[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveAvro[F, A] = updateConfig(cfg.withFolder)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)

    (params.folderOrFile, ote) match {
      case (FolderOrFile.SingleFile, _) =>
        sma.checkAndRun(blocker)(
          rdd.stream[F].through(fileSink[F](blocker).avro(params.outPath)).compile.drain)
      case (FolderOrFile.Folder, Some(te)) =>
        val ate = AvroTypedEncoder[A](te, codec)
        sma.checkAndRun(blocker)(
          F.delay(
            ate.normalize(rdd).write.mode(SaveMode.Overwrite).format("avro").save(params.outPath))
        )
      case (FolderOrFile.Folder, None) =>
        val sparkjob = F.delay {
          val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, codec.schema)
          ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils
            .genericRecordPair(rdd.map(codec.idConversion), codec.avroEncoder)
            .saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](params.outPath)
        }
        sma.checkAndRun(blocker)(sparkjob)
    }
  }
}

final class PartitionAvro[F[_], A: ClassTag, K: ClassTag: Eq](
  rdd: RDD[A],
  codec: AvroCodec[A],
  ote: Option[TypedEncoder[A]],
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)(implicit ss: SparkSession)
    extends AbstractPartition[F, A, K] {

  val params: HoarderParams = cfg.evalConfig

  def spark(implicit te: TypedEncoder[A]): PartitionAvro[F, A, K] =
    new PartitionAvro[F, A, K](rdd, codec, Some(te), cfg, bucketing, pathBuilder)

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F], P: Parallel[F]): F[Unit] =
    savePartition(
      blocker,
      rdd,
      params.parallelism,
      params.format,
      bucketing,
      pathBuilder,
      (r, p) => new SaveAvro[F, A](r, codec, ote, cfg.withOutPutPath(p)).run(blocker))
}
