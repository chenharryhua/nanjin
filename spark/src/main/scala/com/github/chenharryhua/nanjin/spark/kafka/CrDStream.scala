package com.github.chenharryhua.nanjin.spark.kafka

import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

final class CrDStream[F[_], K, V](val dstream: DStream[OptionalKV[K, V]], cfg: SKConfig)(implicit
  val sparkSession: SparkSession) {

  val params: SKParams = cfg.evalConfig

}
