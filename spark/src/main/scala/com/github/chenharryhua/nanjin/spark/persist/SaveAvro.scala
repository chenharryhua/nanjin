package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.mapreduce.NJAvroKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.cats.implicits._
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveAvro[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  sma: SaveModeAware[F],
  cfg: SaverConfig)(implicit codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {
  val params: SaverParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder
    (params.singleOrMulti, params.sparkOrRaw) match {
      case (SingleOrMulti.Single, _) =>
        sma.run(
          rdd.stream[F].through(fileSink[F](blocker).avro(outPath)).compile.drain,
          outPath,
          blocker)
      case (SingleOrMulti.Multi, SparkOrRaw.Spark) =>
        sma.run(
          F.delay(
            utils
              .toDF(rdd, codec.avroEncoder)
              .write
              .mode(SaveMode.Overwrite)
              .format("avro")
              .save(outPath)),
          outPath,
          blocker)
      case (SingleOrMulti.Multi, SparkOrRaw.Raw) =>
        val sparkjob = F.delay {
          val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, codec.schema)
          ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils
            .genericRecordPair(rdd.map(codec.idConversion), codec.avroEncoder)
            .saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](outPath)
        }
        sma.run(sparkjob, outPath, blocker)
    }
  }
}

final class SaveBinaryAvro[F[_], A: ClassTag](rdd: RDD[A], outPath: String, sma: SaveModeAware[F])(
  implicit
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder
    sma.run(
      rdd.stream[F].through(fileSink[F](blocker).binAvro(outPath)).compile.drain,
      outPath,
      blocker)
  }
}
