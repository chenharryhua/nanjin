package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.syntax.bifunctor._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Dataset

final class CrDS[F[_], K, V] private[kafka] (
  val topic: KafkaTopic[F, K, V],
  val dataset: Dataset[OptionalKV[K, V]],
  val ate: AvroTypedEncoder[OptionalKV[K, V]],
  val cfg: SKConfig)
    extends Serializable {

  def repartition(num: Int): CrDS[F, K, V] =
    new CrDS[F, K, V](topic, dataset.repartition(num), ate, cfg)

  def bimap[K2, V2](k: K => K2, v: V => V2)(other: KafkaTopic[F, K2, V2])(implicit
    k2: TypedEncoder[K2],
    v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[OptionalKV[K2, V2]] = OptionalKV.ate(other.topicDef)
    new CrDS[F, K2, V2](other, dataset.map(_.bimap(k, v))(ate.sparkEncoder), ate, cfg)
  }

  def map[K2, V2](f: OptionalKV[K, V] => OptionalKV[K2, V2])(other: KafkaTopic[F, K2, V2])(implicit
    k2: TypedEncoder[K2],
    v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[OptionalKV[K2, V2]] = OptionalKV.ate(other.topicDef)
    new CrDS[F, K2, V2](other, dataset.map(f)(ate.sparkEncoder), ate, cfg)
  }

  def flatMap[K2, V2](f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]])(
    other: KafkaTopic[F, K2, V2])(implicit
    k2: TypedEncoder[K2],
    v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[OptionalKV[K2, V2]] = OptionalKV.ate(other.topicDef)
    new CrDS[F, K2, V2](other, dataset.flatMap(f)(ate.sparkEncoder), ate, cfg)
  }

  def normalize: CrDS[F, K, V] = new CrDS[F, K, V](topic, ate.normalize(dataset).dataset, ate, cfg)

  def filter(f: OptionalKV[K, V] => Boolean): CrDS[F, K, V] =
    new CrDS[F, K, V](topic, dataset.filter(f), ate, cfg)

  def union(other: Dataset[OptionalKV[K, V]]): CrDS[F, K, V] =
    new CrDS[F, K, V](topic, dataset.union(other), ate, cfg)

  def union(other: CrDS[F, K, V]): CrDS[F, K, V] =
    union(other.dataset)

  def count(implicit F: Sync[F]): F[Long] = F.delay(dataset.count())

  def typedDataset: TypedDataset[OptionalKV[K, V]] = TypedDataset.create(dataset)(ate.typedEncoder)

  def stats: Statistics[F] = {
    val enc = TypedExpressionEncoder[CRMetaInfo]
    new Statistics[F](dataset.map(CRMetaInfo(_))(enc), cfg)
  }

  def crRdd: CrRdd[F, K, V] = new CrRdd[F, K, V](topic, dataset.rdd, cfg)(dataset.sparkSession)
  def prRdd: PrRdd[F, K, V] = new PrRdd[F, K, V](topic, dataset.rdd.map(_.toNJProducerRecord), cfg)

  def save: DatasetAvroFileHoarder[F, OptionalKV[K, V]] =
    new DatasetAvroFileHoarder[F, OptionalKV[K, V]](dataset, ate.avroCodec.avroEncoder)

}
