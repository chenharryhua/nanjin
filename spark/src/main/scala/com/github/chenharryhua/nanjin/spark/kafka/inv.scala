package com.github.chenharryhua.nanjin.spark.kafka

import cats.derived.auto.eq.kittensMkEq
import cats.implicits._
import frameless.TypedDataset
import frameless.cats.implicits._
import frameless.functions.aggregate.count

// inputs
final case class KafkaMsgDigest(partition: Int, offset: Long, keyHash: Int, valHash: Int)

// outputs
final case class DiffResult(left: KafkaMsgDigest, right: Option[KafkaMsgDigest])
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
    * find mismatch between left set and right set using hash code
    */
  def diffDataset(
    left: TypedDataset[KafkaMsgDigest],
    right: TypedDataset[KafkaMsgDigest]): TypedDataset[DiffResult] =
    left
      .joinLeft(right)(
        (left('partition) === right('partition)) && (left('offset) === right('offset)))
      .deserialized
      .flatMap { case (m, om) => if (om.exists(_ === m)) None else Some(DiffResult(m, om)) }

  /**
    * (partition, offset) should be unique but if not
    */
  def dupRecords(tds: TypedDataset[CRMetaInfo]): TypedDataset[DupResult] =
    tds
      .groupBy(tds('partition), tds('offset))
      .agg(count())
      .deserialized
      .flatMap(x => if (x._3 > 1) Some(DupResult(x._1, x._2, x._3)) else None)

}
