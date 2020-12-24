package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import frameless.TypedDataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, udf}

private[kafka] object range {

  object cr {

    def timestamp[K, V](nd: NJDateTimeRange)(rdd: RDD[OptionalKV[K, V]]): RDD[OptionalKV[K, V]] =
      rdd.filter(o => nd.isInBetween(o.timestamp))

    def offset[K, V](start: Long, end: Long)(rdd: RDD[OptionalKV[K, V]]): RDD[OptionalKV[K, V]] =
      rdd.filter(o => o.offset >= start && o.offset <= end)
  }

  object pr {

    def timestamp[K, V](nd: NJDateTimeRange)(
      rdd: RDD[NJProducerRecord[K, V]]): RDD[NJProducerRecord[K, V]] =
      rdd.filter(_.timestamp.exists(nd.isInBetween))

    def offset[K, V](start: Long, end: Long)(
      rdd: RDD[NJProducerRecord[K, V]]): RDD[NJProducerRecord[K, V]] =
      rdd.filter(_.offset.exists(o => o >= start && o <= end))
  }

  def timestamp[K, V](nd: NJDateTimeRange)(
    ds: Dataset[OptionalKV[K, V]]): Dataset[OptionalKV[K, V]] = {
    val f = udf(nd.isInBetween _)
    ds.filter(f(col("timestamp")))
  }

  def offset[K, V](start: Long, end: Long)(
    ds: Dataset[OptionalKV[K, V]]): Dataset[OptionalKV[K, V]] =
    ds.filter(col("offset").between(start, end))
}
