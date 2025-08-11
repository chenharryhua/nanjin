package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, udf}

/** Notes: time range: from start time (inclusive) to end time (exclusive)
  *
  * offset range: from start offset (inclusive) to end offset (inclusive)
  */

private[kafka] object range {

  object cr {

    def timestamp[K, V](nd: DateTimeRange)(rdd: RDD[NJConsumerRecord[K, V]]): RDD[NJConsumerRecord[K, V]] =
      rdd.filter(o => nd.inBetween(o.timestamp))

    def offset[K, V](start: Long, end: Long)(rdd: RDD[NJConsumerRecord[K, V]]): RDD[NJConsumerRecord[K, V]] =
      rdd.filter(o => o.offset >= start && o.offset <= end)
  }

  object pr {

    def timestamp[K, V](nd: DateTimeRange)(rdd: RDD[NJProducerRecord[K, V]]): RDD[NJProducerRecord[K, V]] =
      rdd.filter(_.timestamp.exists(nd.inBetween))

    def offset[K, V](start: Long, end: Long)(rdd: RDD[NJProducerRecord[K, V]]): RDD[NJProducerRecord[K, V]] =
      rdd.filter(_.offset.exists(o => o >= start && o <= end))
  }

  def timestamp[K, V](nd: DateTimeRange)(
    ds: Dataset[NJConsumerRecord[K, V]]): Dataset[NJConsumerRecord[K, V]] = {
    val f = udf(nd.inBetween _)
    ds.filter(f(col("timestamp")))
  }

  def offset[K, V](start: Long, end: Long)(
    ds: Dataset[NJConsumerRecord[K, V]]): Dataset[NJConsumerRecord[K, V]] =
    ds.filter(col("offset").between(start, end))
}
