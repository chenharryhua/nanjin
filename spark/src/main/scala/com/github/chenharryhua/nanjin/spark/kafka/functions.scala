package com.github.chenharryhua.nanjin.spark.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import org.apache.spark.sql.functions.{col, countDistinct}
import org.apache.spark.sql.{Dataset, Encoder}

object functions {
  implicit final class NJConsumerRecordDatasetExt[K, V](dataset: Dataset[NJConsumerRecord[K, V]]) {
    def misorderedKey(implicit
      edk: Encoder[DisorderedKey[K]],
      eok: Encoder[Option[K]]): Dataset[DisorderedKey[K]] =
      dataset.groupByKey(_.key).flatMapGroups[DisorderedKey[K]] {
        (okey: Option[K], iter: Iterator[NJConsumerRecord[K, V]]) =>
          okey.traverse { key =>
            iter.toList.sortBy(_.offset).sliding(2).toList.flatMap {
              case List(c, n) =>
                if (n.timestamp >= c.timestamp) None
                else
                  Some(
                    DisorderedKey(
                      key,
                      c.partition,
                      c.offset,
                      c.timestamp,
                      c.timestamp - n.timestamp,
                      n.offset - c.offset,
                      n.partition,
                      n.offset,
                      n.timestamp))
              case _ => None // single item list
            }
          }.flatten
      }

    def misplacedKey(implicit tek: Encoder[MisplacedKey[K]]): Dataset[MisplacedKey[K]] =
      dataset
        .groupBy(col("key"))
        .agg(countDistinct(col("partition")).as("count"))
        .as[MisplacedKey[K]]
        .filter(col("count") > 1)
        .orderBy(col("count").desc)
  }
}
