package com.github.chenharryhua.nanjin.spark.kafka

import cats.implicits._
import frameless.TypedDataset
import frameless.cats.implicits._
import org.apache.spark.sql.Dataset // e.g., count, sum, avg
final class MissingData(ds: Dataset[CRMetaInfo]) extends Serializable {
  @transient private val typedDataset: TypedDataset[CRMetaInfo] = TypedDataset.create(ds)

  def run: TypedDataset[CRMetaInfo] =
    typedDataset.groupBy(typedDataset('partition)).deserialized.flatMapGroups {
      case (_, iter) =>
        iter.sliding(2).flatMap {
          case List(a, b) => if (a.offset + 1 === b.offset) None else Some(a)
        }
    }
}

final case class KafkaMsgDigest(partition: Int, offset: Long, digest: String)

final class CompareDataset(left: Dataset[KafkaMsgDigest], right: Dataset[KafkaMsgDigest]) {

  @transient private val l: TypedDataset[KafkaMsgDigest] = TypedDataset.create(left)
  @transient private val r: TypedDataset[KafkaMsgDigest] = TypedDataset.create(right)

  def run: TypedDataset[KafkaMsgDigest] =
    l.joinLeft(r)((l('partition) === r('partition)) && (l('offset) === r('offset)))
      .deserialized
      .flatMap {
        case (m, om) => if (om.forall(_.digest =!= m.digest)) Some(m) else None
      }
}
