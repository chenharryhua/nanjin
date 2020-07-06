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
import com.github.chenharryhua.nanjin.pipes.{GenericRecordEncoder, JsonAvroSerialization}
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.utils
import com.sksamuel.avro4s.{AvroSchema, SchemaFor, Encoder => AvroEncoder}
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.sql.Dataset

final class FsmConsumerRecords[F[_], K: TypedEncoder: AvroEncoder, V: TypedEncoder: AvroEncoder](
  crs: Dataset[OptionalKV[K, V]],
  cfg: SKConfig)
    extends SparKafkaUpdateParams[FsmConsumerRecords[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](crs, f(cfg))

  @transient lazy val typedDataset: TypedDataset[OptionalKV[K, V]] =
    TypedDataset.create(crs)

  override val params: SKParams = cfg.evalConfig

  // transformations
  def bimap[K2: TypedEncoder: AvroEncoder, V2: TypedEncoder: AvroEncoder](
    k: K => K2,
    v: V => V2): FsmConsumerRecords[F, K2, V2] =
    new FsmConsumerRecords[F, K2, V2](typedDataset.deserialized.map(_.bimap(k, v)).dataset, cfg)

  def flatMap[K2: TypedEncoder: AvroEncoder, V2: TypedEncoder: AvroEncoder](
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

  def inRange(dr: NJDateTimeRange): FsmConsumerRecords[F, K, V] =
    filter(m => dr.isInBetween(m.timestamp))

  def inRange: FsmConsumerRecords[F, K, V] = inRange(params.timeRange)

  // dataset

  def values: TypedDataset[CompulsoryV[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryV)

  def keys: TypedDataset[CompulsoryK[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryK)

  def keyValues: TypedDataset[CompulsoryKV[K, V]] =
    typedDataset.deserialized.flatMap(_.toCompulsoryKV)

  // investigations:
  def missingData: TypedDataset[CRMetaInfo] = {
    val id = utils.random4d.value
    crs.sparkSession.withGroupId(s"nj.cr.miss.$id").withDescription(s"missing data")
    inv.missingData(values.deserialized.map(CRMetaInfo(_)))
  }

  def dupRecords: TypedDataset[DupResult] = {
    val id = utils.random4d.value
    crs.sparkSession.withGroupId(s"nj.cr.dup.$id").withDescription(s"find dup data")
    inv.dupRecords(typedDataset.deserialized.map(CRMetaInfo(_)))
  }

  def diff(other: TypedDataset[OptionalKV[K, V]])(implicit
    ke: Eq[K],
    ve: Eq[V]): TypedDataset[DiffResult[K, V]] = {
    val id = utils.random4d.value
    crs.sparkSession.withGroupId(s"nj.cr.diff.$id").withDescription(s"compare two datasets")
    inv.diffDataset(typedDataset, other)
  }

  def diff(other: FsmConsumerRecords[F, K, V])(implicit
    ke: Eq[K],
    ve: Eq[V]): TypedDataset[DiffResult[K, V]] =
    diff(other.typedDataset)

  def find(f: OptionalKV[K, V] => Boolean)(implicit F: Sync[F]): F[Unit] = {
    val id = utils.random4d.value
    crs.sparkSession.withGroupId(s"nj.cr.find.$id").withDescription(s"find records")
    implicit val ks: SchemaFor[K] = SchemaFor[K](AvroEncoder[K].schema)
    implicit val vs: SchemaFor[V] = SchemaFor[V](AvroEncoder[V].schema)

    val pipe = new JsonAvroSerialization[F](AvroSchema[OptionalKV[K, V]])
    val gr   = new GenericRecordEncoder[F, OptionalKV[K, V]]()

    Stream
      .force(filter(f).typedDataset.take[F](params.showDs.rowNum).map(Stream.emits(_).covary[F]))
      .through(gr.encode)
      .through(pipe.compactJson)
      .showLinesStdOut
      .compile
      .drain
  }

  def count(implicit F: Sync[F]): F[Long] = {
    val id = utils.random4d.value
    crs.sparkSession.withGroupId(s"nj.cr.count.$id").withDescription(s"count datasets")
    typedDataset.count[F]()
  }

  def show(implicit F: Sync[F]): F[Unit] = {
    val id = utils.random4d.value
    crs.sparkSession.withGroupId(s"nj.cr.show.$id").withDescription(s"show datasets")
    typedDataset.show[F](params.showDs.rowNum, params.showDs.isTruncate)
  }

  // state change
  def toProducerRecords: FsmProducerRecords[F, K, V] =
    new FsmProducerRecords((typedDataset.deserialized.map(_.toNJProducerRecord)).dataset, cfg)

}
