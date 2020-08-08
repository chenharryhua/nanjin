package com.github.chenharryhua.nanjin.spark.dstream

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

    def jackson(pathStr: String)(implicit enc: AvroEncoder[A], ss: SparkSession): Unit = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, enc.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

      ds.foreachRDD { (rdd, _) =>
        if (!rdd.isEmpty())
          utils
            .genericRecordPair(rdd, enc)
            .saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](pathStr)
      }
    }

    def avro(pathStr: String)(implicit enc: AvroEncoder[A], ss: SparkSession): Unit = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, enc.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

      ds.foreachRDD { (rdd, _) =>
        if (!rdd.isEmpty())
          utils.genericRecordPair(rdd, enc).saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](pathStr)
      }
    }
  }
}
