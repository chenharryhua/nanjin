package com.github.chenharryhua.nanjin.spark.kafka

import cats.Eq
import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, OptionalKV}
import frameless.cats.implicits._
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD

final case class CRMetaInfo(topic: String, partition: Int, offset: Long, timestamp: Long)

object CRMetaInfo {
  implicit val typedEncoder: TypedEncoder[CRMetaInfo] = shapeless.cachedImplicit

  def apply[K, V](cr: NJConsumerRecord[K, V]): CRMetaInfo =
    CRMetaInfo(
      cr.topic,
      cr.partition,
      cr.offset,
      cr.timestamp
    )
}

final case class KvDiffResult[K, V](key: Option[K], value: Option[V])
final case class DiffResult[K, V](left: OptionalKV[K, V], right: Option[OptionalKV[K, V]])
final case class DupResult(partition: Int, offset: Long, num: Long)

object inv {

  /**
    * offset increased exactly 1 in each partition
    */
  def missingData(tds: TypedDataset[CRMetaInfo]): TypedDataset[CRMetaInfo] =
    tds.groupBy(tds('partition)).deserialized.flatMapGroups {
      case (_, iter) =>
        iter.sliding(2).flatMap {
          case List(a, b) => if (a.offset + 1 === b.offset) None else Some(a)
        }
    }

  /**
    * kafka dataset could be saved in different format, avro, parquet,json, etc
    * are they identical?
    */
  def diffDataset[K: Eq: TypedEncoder, V: Eq: TypedEncoder](
    left: TypedDataset[OptionalKV[K, V]],
    right: TypedDataset[OptionalKV[K, V]]): TypedDataset[DiffResult[K, V]] =
    left
      .joinLeft(right)(
        (left('partition) === right('partition)) && (left('offset) === right('offset)))
      .deserialized
      .flatMap {
        case (m, om) =>
          if (om.exists(o => (o.key === m.key) && (o.value === m.value)))
            None
          else
            Some(DiffResult(m, om))
      }

  def diffRdd[K: Eq, V: Eq](
    left: RDD[OptionalKV[K, V]],
    right: RDD[OptionalKV[K, V]]): RDD[DiffResult[K, V]] = {

    val mine  = left.groupBy(okv => (okv.partition, okv.offset))
    val yours = right.groupBy(okv => (okv.partition, okv.offset))
    mine.leftOuterJoin(yours).flatMap {
      case ((_, _), (iterL, oIterR)) =>
        for {
          l <- iterL
          r <- oIterR.traverse(_.toList)
          dr <-
            if (r.exists(o => (o.key === l.key) && (o.value === l.value)))
              None
            else
              Some(DiffResult(l, r))
        } yield dr
    }
  }

  def kvDiffDataset[K: TypedEncoder, V: TypedEncoder](
    left: TypedDataset[OptionalKV[K, V]],
    right: TypedDataset[OptionalKV[K, V]]): TypedDataset[KvDiffResult[K, V]] = {
    val mine: TypedDataset[KvDiffResult[K, V]] =
      left.select(left('key), left('value)).distinct.as[KvDiffResult[K, V]]
    val yours: TypedDataset[KvDiffResult[K, V]] =
      right.select(right('key), right('value)).distinct.as[KvDiffResult[K, V]]
    mine.except(yours)
  }

  def kvDiffRdd[K, V](
    left: RDD[OptionalKV[K, V]],
    right: RDD[OptionalKV[K, V]]): RDD[KvDiffResult[K, V]] = {
    val mine: RDD[KvDiffResult[K, V]]  = left.map(x => KvDiffResult(x.key, x.value)).distinct()
    val yours: RDD[KvDiffResult[K, V]] = right.map(x => KvDiffResult(x.key, x.value)).distinct()
    mine.subtract(yours)
  }

  /**
    * (partition, offset) should be unique
    */
  def dupRecords(tds: TypedDataset[CRMetaInfo]): TypedDataset[DupResult] =
    tds
      .groupBy(tds('partition), tds('offset))
      .agg(count())
      .deserialized
      .flatMap(x => if (x._3 > 1) Some(DupResult(x._1, x._2, x._3)) else None)

}
