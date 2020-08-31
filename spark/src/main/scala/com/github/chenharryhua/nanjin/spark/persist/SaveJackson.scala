package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.mapreduce.NJJacksonKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.{fileSink, utils, RddExt}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveJackson[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  sma: SaveModeAware[F],
  cfg: SaverConfig)(implicit codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {
  val params: SaverParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: AvroEncoder[A] = codec.avroEncoder
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        sma.run(
          rdd.stream[F].through(fileSink[F](blocker).jackson(outPath)).compile.drain,
          outPath,
          blocker)
      case SingleOrMulti.Multi =>
        val sparkjob = F.delay {
          val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
          AvroJob.setOutputKeySchema(job, codec.schema)
          ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
          utils
            .genericRecordPair(rdd.map(codec.idConversion), codec.avroEncoder)
            .saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](outPath)
        }
        sma.run(sparkjob, outPath, blocker)
    }
  }
}
