package com.github.chenharryhua.nanjin.spark.kafka

import cats.Eq
import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.messages.kafka.{
  CompulsoryK,
  CompulsoryKV,
  CompulsoryV,
  OptionalKV
}
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class FsmConsumerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
  crs: Dataset[OptionalKV[K, V]],
  cfg: SKConfig)
    extends SparKafkaUpdateParams[FsmConsumerRecords[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs, f(cfg))

  @transient lazy val typedDataset: TypedDataset[OptionalKV[K, V]] =
    TypedDataset.create(crs)

  override val params: SKParams = cfg.evalConfig

  // transformations
  def bimap[K2: TypedEncoder, V2: TypedEncoder](
    k: K => K2,
    v: V => V2): FsmConsumerRecords[F, K2, V2] =
    new FsmConsumerRecords[F, K2, V2](typedDataset.deserialized.map(_.bimap(k, v)).dataset, cfg)

  def flatMap[K2: TypedEncoder, V2: TypedEncoder](
    f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]]): FsmConsumerRecords[F, K2, V2] =
    new FsmConsumerRecords[F, K2, V2](typedDataset.deserialized.flatMap(f).dataset, cfg)

  def filter(f: OptionalKV[K, V] => Boolean): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs.filter(f), cfg)

  def ascending: FsmConsumerRecords[F, K, V] = {
    val sd = typedDataset.orderBy(typedDataset('timestamp).asc)
    new FsmConsumerRecords[F, K, V](sd.dataset, cfg)
  }

  def descending: FsmConsumerRecords[F, K, V] = {
    val sd = typedDataset.orderBy(typedDataset('timestamp).desc)
    new FsmConsumerRecords[F, K, V](sd.dataset, cfg)
  }

  def persist: FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs.persist(), cfg)

  // dataset

  def values: TypedDataset[CompulsoryV[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryV)

  def keys: TypedDataset[CompulsoryK[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryK)

  def keyValues: TypedDataset[CompulsoryKV[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryKV)

  // investigations:
  def missingData: TypedDataset[CRMetaInfo] = {
    crs.sparkSession.withGroupId("nj.cr.inv").withDescription(s"missing data")
    inv.missingData(values.deserialized.map(CRMetaInfo(_)))
  }

  def dupRecords: TypedDataset[DupResult] = {
    crs.sparkSession.withGroupId("nj.cr.inv").withDescription(s"find dup data")
    inv.dupRecords(typedDataset.deserialized.map(CRMetaInfo(_)))
  }

  def diff(other: TypedDataset[OptionalKV[K, V]])(implicit
    ke: Eq[K],
    ve: Eq[V]): TypedDataset[DiffResult[K, V]] = {
    crs.sparkSession.withGroupId("nj.cr.inv").withDescription(s"compare two datasets")
    inv.diffDataset(typedDataset, other)
  }

  def count(implicit F: Sync[F]): F[Long] = {
    crs.sparkSession.withGroupId("nj.cr.inv").withDescription(s"count datasets")
    typedDataset.count[F]()
  }

  def show(implicit F: Sync[F]): F[Unit] = {
    crs.sparkSession.withGroupId("nj.cr.inv").withDescription(s"show datasets")
    typedDataset.show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  // state change
  def toProducerRecords: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords((typedDataset.deserialized.map(_.toNJProducerRecord)).dataset, cfg)

}
