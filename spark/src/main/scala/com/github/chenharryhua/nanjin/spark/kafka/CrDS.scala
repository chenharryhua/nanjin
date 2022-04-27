package com.github.chenharryhua.nanjin.spark.kafka

import cats.Eq
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

final class CrDS[F[_], K, V] private[kafka] (
  val dataset: Dataset[NJConsumerRecord[K, V]],
  cfg: SKConfig,
  ack: NJAvroCodec[K],
  acv: NJAvroCodec[V],
  tek: TypedEncoder[K],
  tev: TypedEncoder[V])
    extends Serializable {

  val ate: AvroTypedEncoder[NJConsumerRecord[K, V]] = AvroTypedEncoder(ack, acv)(tek, tev)

  def typedDataset: TypedDataset[NJConsumerRecord[K, V]] = TypedDataset.create(dataset)(ate.typedEncoder)

  // transforms
  def transform(f: Dataset[NJConsumerRecord[K, V]] => Dataset[NJConsumerRecord[K, V]]): CrDS[F, K, V] =
    new CrDS[F, K, V](dataset.transform(f), cfg, ack, acv, tek, tev)

  def partitionOf(num: Int): CrDS[F, K, V] = transform(_.filter(col("partition") === num))

  def offsetRange(start: Long, end: Long): CrDS[F, K, V] = transform(range.offset(start, end))
  def timeRange(dr: NJDateTimeRange): CrDS[F, K, V]      = transform(range.timestamp(dr))
  def timeRange: CrDS[F, K, V]                           = timeRange(cfg.evalConfig.timeRange)

  def ascendOffset: CrDS[F, K, V]     = transform(sort.ascend.offset)
  def descendOffset: CrDS[F, K, V]    = transform(sort.descend.offset)
  def ascendTimestamp: CrDS[F, K, V]  = transform(sort.ascend.timestamp)
  def descendTimestamp: CrDS[F, K, V] = transform(sort.descend.timestamp)

  def union(other: CrDS[F, K, V]): CrDS[F, K, V] = transform(_.union(other.dataset))
  def repartition(num: Int): CrDS[F, K, V]       = transform(_.repartition(num))

  def normalize: CrDS[F, K, V] = transform(ate.normalize)

  def replicate(num: Int): CrDS[F, K, V] =
    transform(ds => (1 until num).foldLeft(ds) { case (r, _) => r.union(ds) })

  // maps
  def bimap[K2, V2](k: K => K2, v: V => V2)(ack2: NJAvroCodec[K2], acv2: NJAvroCodec[V2])(implicit
    k2: TypedEncoder[K2],
    v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[NJConsumerRecord[K2, V2]] = AvroTypedEncoder(ack2, acv2)
    new CrDS[F, K2, V2](dataset.map(_.bimap(k, v))(ate.sparkEncoder), cfg, ack2, acv2, k2, v2).normalize
  }

  def map[K2, V2](
    f: NJConsumerRecord[K, V] => NJConsumerRecord[K2, V2])(ack2: NJAvroCodec[K2], acv2: NJAvroCodec[V2])(implicit
    k2: TypedEncoder[K2],
    v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[NJConsumerRecord[K2, V2]] = AvroTypedEncoder(ack2, acv2)
    new CrDS[F, K2, V2](dataset.map(f)(ate.sparkEncoder), cfg, ack2, acv2, k2, v2).normalize
  }

  def flatMap[K2, V2](f: NJConsumerRecord[K, V] => IterableOnce[NJConsumerRecord[K2, V2]])(
    ack2: NJAvroCodec[K2],
    acv2: NJAvroCodec[V2])(implicit k2: TypedEncoder[K2], v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[NJConsumerRecord[K2, V2]] = AvroTypedEncoder(ack2, acv2)
    new CrDS[F, K2, V2](dataset.flatMap(f)(ate.sparkEncoder), cfg, ack2, acv2, k2, v2).normalize
  }

  val params: SKParams = cfg.evalConfig

  // transition

  def save: DatasetAvroFileHoarder[F, NJConsumerRecord[K, V]] =
    new DatasetAvroFileHoarder[F, NJConsumerRecord[K, V]](dataset, ate.avroCodec.avroEncoder)

  def crRdd: CrRdd[F, K, V] = new CrRdd[F, K, V](dataset.rdd, ack, acv, cfg, dataset.sparkSession)

  def prRdd: PrRdd[F, K, V] =
    new PrRdd[F, K, V](dataset.rdd.map(_.toNJProducerRecord), NJProducerRecord.avroCodec(ack, acv), cfg)

  // statistics
  def stats: Statistics[F] =
    new Statistics[F](dataset.map(CRMetaInfo(_))(TypedExpressionEncoder[CRMetaInfo]), cfg.evalConfig.timeRange.zoneId)

  def count(implicit F: Sync[F]): F[Long] = F.delay(dataset.count())

  def cherrypick(partition: Int, offset: Long): Option[NJConsumerRecord[K, V]] =
    partitionOf(partition).offsetRange(offset, offset).dataset.collect().headOption

  def diff(other: TypedDataset[NJConsumerRecord[K, V]])(implicit eqK: Eq[K], eqV: Eq[V]): Dataset[DiffResult[K, V]] =
    inv.diffDataset(typedDataset, other)(eqK, tek, eqV, tev).dataset

  def diff(other: CrDS[F, K, V])(implicit eqK: Eq[K], eqV: Eq[V]): Dataset[DiffResult[K, V]] =
    diff(other.typedDataset)

  def diffKV(other: TypedDataset[NJConsumerRecord[K, V]]): Dataset[KvDiffResult[K, V]] =
    inv.kvDiffDataset(typedDataset, other)(tek, tev).dataset

  def diffKV(other: CrDS[F, K, V]): Dataset[KvDiffResult[K, V]] = diffKV(other.typedDataset)

  /** Notes: same key should be in same partition.
    */
  def misplacedKey: Dataset[MisplacedKey[K]] = {
    import frameless.functions.aggregate.countDistinct
    implicit val enc: TypedEncoder[K]             = tek
    val tds: TypedDataset[NJConsumerRecord[K, V]] = typedDataset
    val res: TypedDataset[MisplacedKey[K]] =
      tds.groupBy(tds.col(_.key)).agg(countDistinct(tds.col(_.partition))).as[MisplacedKey[K]]()
    res.filter(res.col(_.count) > 1).orderBy(res.col(_.count).asc).dataset
  }

  /** Notes: timestamp order should follow offset order: the larger the offset is the larger of timestamp should be, of
    * the same key
    */
  def misorderedKey: Dataset[MisorderedKey[K]] = {
    implicit val enc: TypedEncoder[K]             = tek
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
