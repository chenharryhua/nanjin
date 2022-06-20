package com.github.chenharryhua.nanjin.spark.kafka

import cats.Eq
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

import scala.annotation.nowarn

final class CrDataset[F[_], K, V] private[kafka] (
  val dataset: Dataset[NJConsumerRecord[K, V]],
  ack: NJAvroCodec[K],
  acv: NJAvroCodec[V],
  tek: TypedEncoder[K],
  tev: TypedEncoder[V])
    extends Serializable {

  val ate: AvroTypedEncoder[NJConsumerRecord[K, V]] = AvroTypedEncoder(ack, acv)(tek, tev)

  lazy val typedDataset: TypedDataset[NJConsumerRecord[K, V]] = TypedDataset.create(dataset)(ate.typedEncoder)

  def diff(
    other: TypedDataset[NJConsumerRecord[K, V]])(implicit eqK: Eq[K], eqV: Eq[V]): Dataset[DiffResult[K, V]] =
    inv.diffDataset(typedDataset, other)(eqK, eqV, tek, tev).dataset

  def diff(other: CrDataset[F, K, V])(implicit eqK: Eq[K], eqV: Eq[V]): Dataset[DiffResult[K, V]] =
    diff(other.typedDataset)

  def diff(
    other: Dataset[NJConsumerRecord[K, V]])(implicit eqK: Eq[K], eqV: Eq[V]): Dataset[DiffResult[K, V]] =
    inv.diffDataset(typedDataset, TypedDataset.create(other)(ate.typedEncoder))(eqK, eqV, tek, tev).dataset

  def diffKV(other: TypedDataset[NJConsumerRecord[K, V]]): Dataset[KvDiffResult[K, V]] =
    inv.kvDiffDataset(typedDataset, other)(tek, tev).dataset

  def diffKV(other: Dataset[NJConsumerRecord[K, V]]): Dataset[KvDiffResult[K, V]] =
    inv.kvDiffDataset(typedDataset, TypedDataset.create(other)(ate.typedEncoder))(tek, tev).dataset

  def diffKV(other: CrDataset[F, K, V]): Dataset[KvDiffResult[K, V]] = diffKV(other.typedDataset)

  /** Notes: same key should be in same partition.
    */
  def misplacedKey: Dataset[MisplacedKey[K]] = {
    import frameless.functions.aggregate.countDistinct
    @nowarn implicit val enc: TypedEncoder[K]     = tek
    val tds: TypedDataset[NJConsumerRecord[K, V]] = typedDataset
    val res: TypedDataset[MisplacedKey[K]] =
      tds.groupBy(tds.col(_.key)).agg(countDistinct(tds.col(_.partition))).as[MisplacedKey[K]]()
    res.filter(res.col(_.count) > 1).orderBy(res.col(_.count).asc).dataset
  }

  /** Notes: timestamp order should follow offset order: the larger the offset is the larger of timestamp
    * should be, of the same key
    */
  def misorderedKey: Dataset[MisorderedKey[K]] = {
    @nowarn implicit val enc: TypedEncoder[K]     = tek
    val tds: TypedDataset[NJConsumerRecord[K, V]] = typedDataset
    tds
      .groupBy(tds.col(_.key))
      .deserialized
      .flatMapGroups { case (key, iter) =>
        key.traverse { key =>
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
      }
      .dataset
  }
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
