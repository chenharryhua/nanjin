package com.github.chenharryhua.nanjin.sparkafka

import com.github.chenharryhua.nanjin.codec.SparkafkaConsumerRecord
import com.github.chenharryhua.nanjin.kafka.KafkaTimestamp
import frameless.functions.aggregate.{collectSet, count}
import frameless.functions.size
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

final case class KeyPartition[K](key: K, partition: Int)
final case class KeysInPartitions[K](key: K, partitions: Vector[Int])

trait Aggregations {

  implicit class PredefinedAggregationFunction[K: TypedEncoder, V: TypedEncoder](
    tds: TypedDataset[SparkafkaConsumerRecord[K, V]]) {

    def hourly(implicit spark: SparkSession): TypedDataset[(Int, Long)] = {
      import org.apache.spark.sql.functions.{col, count}
      import spark.implicits._
      val hour = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local.getHour
      }.dataset.groupBy(col("_1")).agg(count("_1")).as[(Int, Long)]
      TypedDataset.create[(Int, Long)](hour)
    }

    def minutely: TypedDataset[(Int, Long)] = {
      val minute: TypedDataset[Int] = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local.getMinute
      }
      minute.groupBy(minute.asCol).agg(count(minute.asCol))
    }

    def daily: TypedDataset[(Int, Long)] = {
      val day: TypedDataset[Int] = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local.getDayOfYear
      }
      day.groupBy(day.asCol).agg(count(day.asCol))
    }
  }
}
