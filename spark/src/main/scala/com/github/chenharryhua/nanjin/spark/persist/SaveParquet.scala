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

final class SaveParquet[F[_], A: ClassTag](
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
          rdd.stream[F].through(fileSink[F](blocker).parquet(outPath)).compile.drain,
          outPath,
          blocker)
      case (SingleOrMulti.Multi, SparkOrRaw.Spark) =>
        sma.run(
          F.delay(
            utils.toDF(rdd, codec.avroEncoder).write.mode(SaveMode.Overwrite).parquet(outPath)),
          outPath,
          blocker)
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
        sma.run(sparkjob, outPath, blocker)
    }
  }
}
