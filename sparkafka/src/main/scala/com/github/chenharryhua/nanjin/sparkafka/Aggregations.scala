package com.github.chenharryhua.nanjin.sparkafka

import com.github.chenharryhua.nanjin.codec.utils
import frameless.functions.aggregate.{collectSet, count}
import frameless.functions.size
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
final case class KeyPartition[K](key: K, partition: Int)
final case class KeysInPartitions[K](key: K, partitions: Vector[Int])

trait Aggregations {

  implicit class PredefinedAggregationFunction[K: TypedEncoder, V: TypedEncoder](
    tds: TypedDataset[SparkafkaRecord[K, V]]) {

    val keysInPartitions: TypedDataset[KeysInPartitions[K]] = {
      val keyPartition = tds.project[KeyPartition[K]]
      val agged = keyPartition
        .groupBy(keyPartition('key))
        .agg(collectSet(keyPartition('partition)))
        .as[KeysInPartitions[K]]
      agged.filter(size(agged('partitions)) > 1)
    }

    def hourly(implicit spark: SparkSession): TypedDataset[(Int, Long)] = {
      import org.apache.spark.sql.functions.{col, count}
      import spark.implicits._
      val hour = tds.deserialized.map { m =>
        utils.kafkaTimestamp(m.timestamp).getHour
      }.dataset.groupBy(col("_1")).agg(count("_1")).as[(Int, Long)]
      TypedDataset.create[(Int, Long)](hour)
    }

    def minutely: TypedDataset[(Int, Long)] = {
      val minute: TypedDataset[Int] = tds.deserialized.map { m =>
        utils.kafkaTimestamp(m.timestamp).getMinute
      }
      minute.groupBy(minute.asCol).agg(count(minute.asCol))
    }

    def daily: TypedDataset[(Int, Long)] = {
      val day: TypedDataset[Int] = tds.deserialized.map { m =>
        utils.kafkaTimestamp(m.timestamp).getDayOfYear
      }
      day.groupBy(day.asCol).agg(count(day.asCol))
    }
  }
}
