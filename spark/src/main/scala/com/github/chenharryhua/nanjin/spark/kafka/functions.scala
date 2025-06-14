package com.github.chenharryhua.nanjin.spark.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, countDistinct}

import scala.annotation.unused

object functions {
  implicit final class NJConsumerRecordDatasetExt[K, V](dataset: Dataset[NJConsumerRecord[K, V]]) {

    def misorderedKey(implicit @unused tek: TypedEncoder[K]): Dataset[DisorderedKey[K]] = {
      val teok: TypedEncoder[Option[K]] = shapeless.cachedImplicit
      val temk: TypedEncoder[DisorderedKey[K]] = shapeless.cachedImplicit

      dataset
        .groupByKey(_.key)(TypedExpressionEncoder(teok))
        .flatMapGroups[DisorderedKey[K]] { (okey: Option[K], iter: Iterator[NJConsumerRecord[K, V]]) =>
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
        }(TypedExpressionEncoder(temk))
    }

    def misplacedKey(implicit @unused tek: TypedEncoder[K]): Dataset[MisplacedKey[K]] = {
      val te: TypedEncoder[MisplacedKey[K]] = shapeless.cachedImplicit

      dataset
        .groupBy(col("key"))
        .agg(countDistinct(col("partition")).as("count"))
        .as[MisplacedKey[K]](TypedExpressionEncoder(te))
        .filter(col("count") > 1)
        .orderBy(col("count").desc)
    }
  }
}
