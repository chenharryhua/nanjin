package com.github.chenharryhua.nanjin.spark.kafka

import cats.Eq
import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.{
  CompulsoryK,
  CompulsoryKV,
  CompulsoryV,
  OptionalKV
}
import frameless.cats.implicits._
import frameless.{SparkDelay, TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class CrDataset[F[_], K: TypedEncoder, V: TypedEncoder](
  crs: Dataset[OptionalKV[K, V]],
  cfg: SKConfig)
    extends SparKafkaUpdateParams[CrDataset[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): CrDataset[F, K, V] =
    new CrDataset[F, K, V](crs, f(cfg))

  @transient lazy val typedDataset: TypedDataset[OptionalKV[K, V]] =
    TypedDataset.create(crs)

  override val params: SKParams = cfg.evalConfig

  // transformations
  def bimap[K2: TypedEncoder, V2: TypedEncoder](k: K => K2, v: V => V2): CrDataset[F, K2, V2] =
    new CrDataset[F, K2, V2](typedDataset.deserialized.map(_.bimap(k, v)).dataset, cfg)

  def flatMap[K2: TypedEncoder, V2: TypedEncoder](
    f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]]): CrDataset[F, K2, V2] =
    new CrDataset[F, K2, V2](typedDataset.deserialized.flatMap(f).dataset, cfg)

  def filter(f: OptionalKV[K, V] => Boolean): CrDataset[F, K, V] =
    new CrDataset[F, K, V](crs.filter(f), cfg)

  def ascending: CrDataset[F, K, V] = {
    val sd = typedDataset.orderBy(
      typedDataset('timestamp).asc,
      typedDataset('offset).asc,
      typedDataset('partition).asc)
    new CrDataset[F, K, V](sd.dataset, cfg)
  }

  def descending: CrDataset[F, K, V] = {
    val sd = typedDataset.orderBy(
      typedDataset('timestamp).desc,
      typedDataset('offset).desc,
      typedDataset('partition).desc)
    new CrDataset[F, K, V](sd.dataset, cfg)
  }

  def persist: CrDataset[F, K, V] =
    new CrDataset[F, K, V](crs.persist(), cfg)

  def inRange(dr: NJDateTimeRange): CrDataset[F, K, V] =
    new CrDataset[F, K, V](
      crs.filter((m: OptionalKV[K, V]) => dr.isInBetween(m.timestamp)),
      cfg.withTimeRange(dr))

  def inRange: CrDataset[F, K, V] = inRange(params.timeRange)

  def inRange(start: String, end: String): CrDataset[F, K, V] =
    inRange(params.timeRange.withTimeRange(start, end))

  // dataset

  def values: TypedDataset[CompulsoryV[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryV)

  def keys: TypedDataset[CompulsoryK[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryK)

  def keyValues: TypedDataset[CompulsoryKV[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryKV)

  // investigations:
  def stats: Statistics[F] =
    new Statistics[F](typedDataset.deserialized.map(CRMetaInfo(_)).dataset, cfg)

  def missingData: TypedDataset[CRMetaInfo] =
    inv.missingData(values.deserialized.map(CRMetaInfo(_)))

  def dupRecords: TypedDataset[DupResult] =
    inv.dupRecords(typedDataset.deserialized.map(CRMetaInfo(_)))

  def diff(other: TypedDataset[OptionalKV[K, V]])(implicit
    ke: Eq[K],
    ve: Eq[V]): TypedDataset[DiffResult[K, V]] =
    inv.diffDataset(typedDataset, other)

  def diff(
    other: CrDataset[F, K, V])(implicit ke: Eq[K], ve: Eq[V]): TypedDataset[DiffResult[K, V]] =
    diff(other.typedDataset)

  def kvDiff(other: TypedDataset[OptionalKV[K, V]]): TypedDataset[KvDiffResult[K, V]] =
    inv.kvDiffDataset(typedDataset, other)

  def kvDiff(other: CrDataset[F, K, V]): TypedDataset[KvDiffResult[K, V]] =
    kvDiff(other.typedDataset)

  def find(f: OptionalKV[K, V] => Boolean)(implicit F: Sync[F]): F[List[OptionalKV[K, V]]] =
    filter(f).typedDataset.take[F](params.showDs.rowNum).map(_.toList)

  def count(implicit F: SparkDelay[F]): F[Long] =
    typedDataset.count[F]()

  def show(implicit F: SparkDelay[F]): F[Unit] =
    typedDataset.show[F](params.showDs.rowNum, params.showDs.isTruncate)

  def first(implicit F: Sync[F]): F[Option[OptionalKV[K, V]]] = F.delay(crs.rdd.cminOption)
  def last(implicit F: Sync[F]): F[Option[OptionalKV[K, V]]]  = F.delay(crs.rdd.cmaxOption)

  // state change
  def toProducerRecords: PrDataset[F, K, V] =
    new PrDataset((typedDataset.deserialized.map(_.toNJProducerRecord)).dataset, cfg)

}
