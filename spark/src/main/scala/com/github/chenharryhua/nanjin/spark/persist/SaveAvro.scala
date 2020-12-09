package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.pipes.GenericRecordCodec
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.cats.implicits._
import org.apache.avro.file.CodecFactory
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveAvro[F[_], A](rdd: RDD[A], encoder: AvroEncoder[A], cfg: HoarderConfig)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveAvro[F, A] =
    new SaveAvro[F, A](rdd, encoder, cfg)

  def overwrite: SaveAvro[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveAvro[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveAvro[F, A] = updateConfig(cfg.withIgnore)

  def file: SaveAvro[F, A]   = updateConfig(cfg.withSingleFile)
  def folder: SaveAvro[F, A] = updateConfig(cfg.withFolder)

  def deflate(level: Int): SaveAvro[F, A] =
    updateConfig(cfg.withCompression(Compression.Deflate(level)))

  def xz(level: Int): SaveAvro[F, A] =
    updateConfig(cfg.withCompression(Compression.Xz(level)))

  def snappy: SaveAvro[F, A] =
    updateConfig(cfg.withCompression(Compression.Snappy))

  def bzip2: SaveAvro[F, A] =
    updateConfig(cfg.withCompression(Compression.Bzip2))

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    cs: ContextShift[F],
    ss: SparkSession,
    tag: ClassTag[A]): F[Unit] = {

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)
    val cf: CodecFactory      = params.compression.avro(ss.sparkContext.hadoopConfiguration)

    params.folderOrFile match {
      case FolderOrFile.SingleFile =>
        val hadoop: NJHadoop[F]            = NJHadoop[F](ss.sparkContext.hadoopConfiguration, blocker)
        val pipe: GenericRecordCodec[F, A] = new GenericRecordCodec[F, A]
        sma.checkAndRun(blocker)(
          rdd
            .stream[F]
            .through(pipe.encode(encoder))
            .through(hadoop.avroSink(params.outPath, encoder.schema, cf))
            .compile
            .drain)
      case FolderOrFile.Folder =>
        val sparkjob = F.delay {
          val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, encoder.schema)
          ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils
            .genericRecordPair(rdd, encoder)
            .saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](params.outPath)
        }
        sma.checkAndRun(blocker)(sparkjob)
    }
  }
}
