package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.functions.{col, countDistinct}

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import scala.annotation.unused
import io.scalaland.chimney.dsl.*
final case class CRMetaInfo(
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  timestampType: Int) {
  def localDateTime(zoneId: ZoneId): LocalDateTime =
    Instant.ofEpochMilli(timestamp).atZone(zoneId).toLocalDateTime

  def zoned(zoneId: ZoneId): ZonedCRMetaInfo =
    this
      .into[ZonedCRMetaInfo]
      .withFieldComputed(_.timestamp, cr => Instant.ofEpochMilli(cr.timestamp).atZone(zoneId))
      .transform
}

final case class ZonedCRMetaInfo(topic: String, partition: Int, offset: Long, timestamp: ZonedDateTime)

object CRMetaInfo {
  implicit val typedEncoderCRMetaInfo: TypedEncoder[CRMetaInfo] = shapeless.cachedImplicit
  implicit val encoderCRMetaInfo: Encoder[CRMetaInfo] = TypedExpressionEncoder(typedEncoderCRMetaInfo)

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
  implicit final class NJConsumerRecordDatasetExt[F[_], K, V](fdataset: F[Dataset[NJConsumerRecord[K, V]]])(
    implicit F: Sync[F]) {

    def misorderedKey(implicit @unused tek: TypedEncoder[K]): F[Dataset[MisorderedKey[K]]] = {
      val teok: TypedEncoder[Option[K]]        = shapeless.cachedImplicit
      val temk: TypedEncoder[MisorderedKey[K]] = shapeless.cachedImplicit
      F.flatMap(fdataset)(dataset =>
        F.interruptible(
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
            }(TypedExpressionEncoder(temk))))
    }

    def misplacedKey(implicit @unused tek: TypedEncoder[K]): F[Dataset[MisplacedKey[K]]] = {
      val te: TypedEncoder[MisplacedKey[K]] = shapeless.cachedImplicit
      F.flatMap(fdataset)(dataset =>
        F.interruptible(
          dataset
            .groupBy(col("key"))
            .agg(countDistinct(col("partition")).as("count"))
            .as[MisplacedKey[K]](TypedExpressionEncoder(te))
            .filter(col("count") > 1)
            .orderBy(col("count").desc)))
    }

    def stats: Statistics[F] =
      new Statistics[F](F.flatMap(fdataset)(ds => F.interruptible(ds.map(CRMetaInfo(_)))))
  }
}
