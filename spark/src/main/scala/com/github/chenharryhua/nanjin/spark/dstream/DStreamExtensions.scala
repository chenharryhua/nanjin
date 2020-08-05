package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Reader
import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.spark.mapreduce.{
  NJAvroKeyOutputFormat,
  NJJacksonKeyOutputFormat
}
import com.github.chenharryhua.nanjin.spark.utils
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

/**
  * use spark structured stream whenever possible, but in case
  */

private[dstream] trait DStreamExtensions {

  implicit class DStreamExt[A](private val ds: DStream[A]) {

    def jackson[F[_]](pathStr: String)(implicit
      enc: AvroEncoder[A],
      ss: SparkSession,
      F: ConcurrentEffect[F],
      cs: ContextShift[F]): Reader[Blocker, Unit] = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, enc.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

      Reader((blocker: Blocker) =>
        ds.foreachRDD { (rdd, time) =>
          if (!rdd.isEmpty())
            utils
              .genericRecordPair(rdd, enc)
              .saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](pathStr)
        })
    }

    def avro[F[_]](pathStr: String)(implicit
      enc: AvroEncoder[A],
      ss: SparkSession,
      F: ConcurrentEffect[F],
      cs: ContextShift[F]): Reader[Blocker, Unit] = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, enc.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

      Reader((blocker: Blocker) =>
        ds.foreachRDD { (rdd, time) =>
          if (!rdd.isEmpty())
            utils.genericRecordPair(rdd, enc).saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](pathStr)
        })
    }
  }
}
