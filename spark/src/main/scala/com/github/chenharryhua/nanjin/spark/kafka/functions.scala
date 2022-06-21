package com.github.chenharryhua.nanjin.spark.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, countDistinct}

import java.time.{Instant, LocalDateTime, ZoneId}
import scala.annotation.nowarn

final case class CRMetaInfo(
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: Int) {
  def localDateTime(zoneId: ZoneId): LocalDateTime =
    Instant.ofEpochMilli(timestamp).atZone(zoneId).toLocalDateTime
}

object CRMetaInfo {
  implicit val typedEncoder: TypedEncoder[CRMetaInfo] = shapeless.cachedImplicit

  def apply[K, V](cr: NJConsumerRecord[K, V]): CRMetaInfo =
    CRMetaInfo(cr.topic, cr.partition, cr.offset, cr.timestamp, cr.timestampType)
}

final case class MisorderedKey[K](
  key: K,
  partition: Int,
  offset: Long,
  ts: Long,
  msGap: Long,
  offsetDistance: Long,
  nextPartition: Int,
  nextOffset: Long,
  nextTS: Long)

final case class MisplacedKey[K](key: Option[K], count: Long)

object functions {
  implicit final class NJConsumerRecordDatasetExt[K, V](dataset: Dataset[NJConsumerRecord[K, V]]) {

    def misorderedKey(implicit @nowarn tek: TypedEncoder[K]): Dataset[MisorderedKey[K]] = {
      val teok: TypedEncoder[Option[K]]        = shapeless.cachedImplicit
      val temk: TypedEncoder[MisorderedKey[K]] = shapeless.cachedImplicit
      dataset
        .groupByKey(_.key)(TypedExpressionEncoder(teok))
        .flatMapGroups[MisorderedKey[K]] { (okey: Option[K], iter: Iterator[NJConsumerRecord[K, V]]) =>
          okey.traverse { key =>
            iter.toList.sortBy(_.offset).sliding(2).toList.flatMap {
              case List(c, n) =>
                if (n.timestamp >= c.timestamp) None
                else
                  Some(
                    MisorderedKey(
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

    def misplacedKey(implicit @nowarn tek: TypedEncoder[K]): Dataset[MisplacedKey[K]] = {
      val te: TypedEncoder[MisplacedKey[K]] = shapeless.cachedImplicit
      dataset
        .groupBy(col("key"))
        .agg(countDistinct(col("partition")).as("count"))
        .as[MisplacedKey[K]](TypedExpressionEncoder(te))
        .filter(col("count") > 1)
        .orderBy(col("count").desc)
    }

    def stats: Statistics = {
      val te: TypedEncoder[CRMetaInfo] = shapeless.cachedImplicit
      new Statistics(
        dataset.map(CRMetaInfo(_))(TypedExpressionEncoder(te)),
        ZoneId.of(dataset.sparkSession.conf.get("spark.sql.session.timeZone")))
    }
  }
}
