package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetOutputFormat, GenericDataSupplier}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveParquet[F[_], A: ClassTag](rdd: RDD[A], outPath: String, cfg: HoarderConfig)(
  implicit
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveParquet[F, A] =
    new SaveParquet[F, A](rdd, outPath, cfg)

  def spark: SaveParquet[F, A] = updateConfig(cfg.withSpark)
  def raw: SaveParquet[F, A]   = updateConfig(cfg.withRaw)

  def single: SaveParquet[F, A] = updateConfig(cfg.withSingle)
  def multi: SaveParquet[F, A]  = updateConfig(cfg.withMulti)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder
    val sma: SaveModeAware[F]            = new SaveModeAware[F](params.saveMode, outPath, ss)

    (params.singleOrMulti, params.sparkOrRaw) match {
      case (SingleOrMulti.Single, _) =>
        sma.checkAndRun(blocker)(
          rdd.stream[F].through(fileSink[F](blocker).parquet(outPath)).compile.drain)
      case (SingleOrMulti.Multi, SparkOrRaw.Spark) =>
        sma.checkAndRun(blocker)(
          F.delay(
            utils
              .normalizedDF(rdd, codec.avroEncoder)
              .write
              .mode(SaveMode.Overwrite)
              .parquet(outPath)))
      case (SingleOrMulti.Multi, SparkOrRaw.Raw) =>
        val sparkjob = F.delay {
          val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
          AvroParquetOutputFormat.setAvroDataSupplier(job, classOf[GenericDataSupplier])
          AvroParquetOutputFormat.setSchema(job, codec.schema)
          ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          rdd // null as java Void
            .map(a => (null, codec.avroEncoder.encode(a).asInstanceOf[GenericRecord]))
            .saveAsNewAPIHadoopFile(
              outPath,
              classOf[Void],
              classOf[GenericRecord],
              classOf[AvroParquetOutputFormat[GenericRecord]])
        }
        sma.checkAndRun(blocker)(sparkjob)
    }
  }
}
