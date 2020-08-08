package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.spark.mapreduce.NJJacksonKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.utils
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

final class JacksonSaver[A](
  dstream: DStream[A],
  pathStr: String,
  enc: AvroEncoder[A],
  ss: SparkSession) {

  def run(): Unit = {
    val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job, enc.schema)
    ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)

    dstream.foreachRDD { (rdd, _) =>
      if (!rdd.isEmpty())
        utils.genericRecordPair(rdd, enc).saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](pathStr)
    }
  }
}
