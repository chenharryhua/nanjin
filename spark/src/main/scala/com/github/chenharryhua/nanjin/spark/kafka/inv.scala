package com.github.chenharryhua.nanjin.spark.kafka

import cats.Eq
import cats.syntax.all.*
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD

final case class CRMetaInfo(topic: String, partition: Int, offset: Long, timestamp: Long, timestampType: Int)

object CRMetaInfo {
  implicit val typedEncoder: TypedEncoder[CRMetaInfo] = shapeless.cachedImplicit

  def apply[K, V](cr: NJConsumerRecord[K, V]): CRMetaInfo =
    CRMetaInfo(cr.topic, cr.partition, cr.offset, cr.timestamp, cr.timestampType)
}

final case class KvDiffResult[K, V](key: Option[K], value: Option[V])
final case class DiffResult[K, V](left: NJConsumerRecord[K, V], right: Option[NJConsumerRecord[K, V]])

object inv {

  def diffDataset[K: Eq: TypedEncoder, V: Eq: TypedEncoder](
    left: TypedDataset[NJConsumerRecord[K, V]],
    right: TypedDataset[NJConsumerRecord[K, V]]): TypedDataset[DiffResult[K, V]] =
    left
      .joinLeft(right)(
        (left(Symbol("partition")) === right(Symbol("partition"))) && (left(Symbol("offset")) === right(
          Symbol("offset"))))
      .deserialized
      .flatMap { case (m, om) =>
        if (om.exists(o => (o.key === m.key) && (o.value === m.value)))
          None
        else
          Some(DiffResult(m, om))
      }

  def diffRdd[K: Eq, V: Eq](
    left: RDD[NJConsumerRecord[K, V]],
    right: RDD[NJConsumerRecord[K, V]]): RDD[DiffResult[K, V]] = {

    val mine  = left.groupBy(okv => (okv.partition, okv.offset))
    val yours = right.groupBy(okv => (okv.partition, okv.offset))
    mine.leftOuterJoin(yours).flatMap { case ((_, _), (iterL, oIterR)) =>
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
    left: TypedDataset[NJConsumerRecord[K, V]],
    right: TypedDataset[NJConsumerRecord[K, V]]): TypedDataset[KvDiffResult[K, V]] = {
    val mine: TypedDataset[KvDiffResult[K, V]] =
      left.select(left(Symbol("key")), left(Symbol("value"))).distinct.as[KvDiffResult[K, V]]()
    val yours: TypedDataset[KvDiffResult[K, V]] =
      right.select(right(Symbol("key")), right(Symbol("value"))).distinct.as[KvDiffResult[K, V]]()
    mine.except(yours)
  }

  def kvDiffRdd[K, V](
    left: RDD[NJConsumerRecord[K, V]],
    right: RDD[NJConsumerRecord[K, V]]): RDD[KvDiffResult[K, V]] = {
    val mine: RDD[KvDiffResult[K, V]]  = left.map(x => KvDiffResult(x.key, x.value)).distinct()
    val yours: RDD[KvDiffResult[K, V]] = right.map(x => KvDiffResult(x.key, x.value)).distinct()
    mine.subtract(yours)
  }
}
