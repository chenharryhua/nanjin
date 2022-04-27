package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

private[kafka] object sort {

  object ascend {

    object cr {

      def timestamp[K, V](rdd: RDD[NJConsumerRecord[K, V]]): RDD[NJConsumerRecord[K, V]] =
        rdd.sortBy(x => (x.timestamp, x.offset, x.partition), ascending = true)

      def offset[K, V](rdd: RDD[NJConsumerRecord[K, V]]): RDD[NJConsumerRecord[K, V]] =
        rdd.sortBy(x => (x.offset, x.timestamp, x.partition), ascending = true)
    }

    object pr {

      def timestamp[K, V](rdd: RDD[NJProducerRecord[K, V]]): RDD[NJProducerRecord[K, V]] =
        rdd.sortBy(x => (x.timestamp, x.offset, x.partition), ascending = true)

      def offset[K, V](rdd: RDD[NJProducerRecord[K, V]]): RDD[NJProducerRecord[K, V]] =
        rdd.sortBy(x => (x.offset, x.timestamp, x.partition), ascending = true)
    }

    def timestamp[K, V](ds: Dataset[NJConsumerRecord[K, V]]): Dataset[NJConsumerRecord[K, V]] =
      ds.orderBy(col("timestamp").asc, col("offset").asc, col("partition").asc)

    def offset[K, V](ds: Dataset[NJConsumerRecord[K, V]]): Dataset[NJConsumerRecord[K, V]] =
      ds.orderBy(col("offset").asc, col("timestamp").asc, col("partition").asc)

  }

  object descend {

    object cr {

      def timestamp[K, V](rdd: RDD[NJConsumerRecord[K, V]]): RDD[NJConsumerRecord[K, V]] =
        rdd.sortBy(x => (x.timestamp, x.offset, x.partition), ascending = false)

      def offset[K, V](rdd: RDD[NJConsumerRecord[K, V]]): RDD[NJConsumerRecord[K, V]] =
        rdd.sortBy(x => (x.offset, x.timestamp, x.partition), ascending = false)
    }

    object pr {

      def timestamp[K, V](rdd: RDD[NJProducerRecord[K, V]]): RDD[NJProducerRecord[K, V]] =
        rdd.sortBy(x => (x.timestamp, x.offset, x.partition), ascending = false)

      def offset[K, V](rdd: RDD[NJProducerRecord[K, V]]): RDD[NJProducerRecord[K, V]] =
        rdd.sortBy(x => (x.offset, x.timestamp, x.partition), ascending = false)
    }

    def timestamp[K, V](ds: Dataset[NJConsumerRecord[K, V]]): Dataset[NJConsumerRecord[K, V]] =
      ds.orderBy(col("timestamp").desc, col("offset").desc, col("partition").desc)

    def offset[K, V](ds: Dataset[NJConsumerRecord[K, V]]): Dataset[NJConsumerRecord[K, V]] =
      ds.orderBy(col("offset").desc, col("timestamp").desc, col("partition").desc)

  }
}
