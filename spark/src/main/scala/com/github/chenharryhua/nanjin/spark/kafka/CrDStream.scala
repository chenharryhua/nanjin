package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.mapreduce.NJAvroKeyOutputFormat
import com.github.chenharryhua.nanjin.spark.utils
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

final class CrDStream[F[_], K, V](val dstream: DStream[OptionalKV[K, V]], cfg: SKConfig)(implicit
  val sparkSession: SparkSession,
  val keyAvroEncoder: AvroEncoder[K],
  val valAvroEncoder: AvroEncoder[V]) {

  private val encoder: AvroEncoder[OptionalKV[K, V]] = AvroEncoder[OptionalKV[K, V]]

  val params: SKParams = cfg.evalConfig

  object save {

    def avro(pathStr: String): Unit = {
      val job = Job.getInstance(sparkSession.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, encoder.schema)
      sparkSession.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      dstream.foreachRDD { (rdd, _) =>
        if (!rdd.isEmpty())
          utils
            .genericRecordPair(rdd, encoder)
            .saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](pathStr)
      }
    }
  }
}
