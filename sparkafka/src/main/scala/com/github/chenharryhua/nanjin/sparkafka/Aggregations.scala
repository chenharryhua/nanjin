package com.github.chenharryhua.nanjin.sparkafka

import frameless.functions.aggregate.collectSet
import frameless.functions.size
import frameless.{SQLTimestamp, TypedDataset, TypedEncoder}

final case class KeyPartition[K](key: K, partition: Int)
final case class KeysInPartitions[K](key: K, partitions: Vector[Int])

trait Aggregations {

  implicit class PredefinedAggregationFunction[K: TypedEncoder, V: TypedEncoder](
    tds: TypedDataset[SparkConsumerRecord[K, V]]) {

    val keysInPartitions: TypedDataset[KeysInPartitions[K]] = {
      val keyPartition = tds.project[KeyPartition[K]]
      val agged = keyPartition
        .groupBy(keyPartition('key))
        .agg(collectSet(keyPartition('partition)))
        .as[KeysInPartitions[K]]
      agged.filter(size(agged('partitions)) > 1)
    }
  }
}
