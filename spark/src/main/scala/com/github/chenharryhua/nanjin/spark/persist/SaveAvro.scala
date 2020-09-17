package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.{Eq, Parallel}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.cats.implicits._
import org.apache.avro.file.CodecFactory
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveAvro[F[_], A: ClassTag](
  rdd: RDD[A],
  codec: AvroCodec[A],
  compression: Compression,
  cfg: HoarderConfig)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, codec, compression, cfg)

  def file: SaveAvro[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveAvro[F, A] = updateConfig(cfg.withFolder)

  private def updateCompression(compression: Compression): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, codec, compression, cfg)

  def deflate(level: Int): SaveAvro[F, A] = updateCompression(Compression.Deflate(level))
  def xz(level: Int): SaveAvro[F, A]      = updateCompression(Compression.Xz(level))
  def snappy: SaveAvro[F, A]              = updateCompression(Compression.Snappy)
  def bzip2: SaveAvro[F, A]               = updateCompression(Compression.Bzip2)

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F], ss: SparkSession): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    val cf: CodecFactory      = compression.avro(ss.sparkContext.hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        sma.checkAndRun(blocker)(
          rdd.stream[F].through(fileSink[F](blocker).avro(params.outPath, cf)).compile.drain)
      case FolderOrFile.Folder =>
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
  compression: Compression,
  cfg: HoarderConfig,
  bucketing: A => Option[K],
  pathBuilder: (NJFileFormat, K) => String)
    extends AbstractPartition[F, A, K] {

  val params: HoarderParams = cfg.evalConfig

  private def updateCompression(compression: Compression): PartitionAvro[F, A, K] =
    new PartitionAvro[F, A, K](rdd, codec, compression, cfg, bucketing, pathBuilder)

  def deflate(level: Int): PartitionAvro[F, A, K] = updateCompression(Compression.Deflate(level))
  def xz(level: Int): PartitionAvro[F, A, K]      = updateCompression(Compression.Xz(level))
  def snappy: PartitionAvro[F, A, K]              = updateCompression(Compression.Snappy)
  def bzip2: PartitionAvro[F, A, K]               = updateCompression(Compression.Bzip2)

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F],
    ss: SparkSession): F[Unit] =
    savePartition(
      blocker,
      rdd,
      params.parallelism,
      params.format,
      bucketing,
      pathBuilder,
      (r, p) => new SaveAvro[F, A](r, codec, compression, cfg.withOutPutPath(p)).run(blocker))
}
