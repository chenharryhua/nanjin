package com.github.chenharryhua.nanjin.spark.kafka

import cats.Eq
import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import frameless.cats.implicits._
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}

// outputs
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
    * same kafka dataset saved in different format, avro, parquet,json, etc
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
