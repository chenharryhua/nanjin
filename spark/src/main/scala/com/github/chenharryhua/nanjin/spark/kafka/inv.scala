package com.github.chenharryhua.nanjin.spark.kafka

import cats.implicits._
import frameless.TypedDataset
import frameless.cats.implicits._
import frameless.functions.aggregate.count

final case class KafkaMsgDigest(partition: Int, offset: Long, keyHash: Int, valHash: Int)

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
  def compareDataset(
    left: TypedDataset[KafkaMsgDigest],
    right: TypedDataset[KafkaMsgDigest]): TypedDataset[KafkaMsgDigest] =
    left
      .joinLeft(right)(
        (left('partition) === right('partition)) && (left('offset) === right('offset)))
      .deserialized
      .flatMap {
        case (m, om) =>
          if (om.forall(x => (x.keyHash =!= m.keyHash) || (x.valHash =!= m.valHash))) Some(m)
          else None
      }

  /**
    * (partition, offset) should be unique but if not
    */
  def duplicatedRecords(tds: TypedDataset[CRMetaInfo]): TypedDataset[(CRMetaInfo, Long)] =
    tds.groupBy(tds.asCol).agg(count()).deserialized.filter(_._2 > 1)

}
