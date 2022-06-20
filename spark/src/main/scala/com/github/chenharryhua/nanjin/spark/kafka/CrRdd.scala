package com.github.chenharryhua.nanjin.spark.kafka

import cats.{Endo, Eq}
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.{RddAvroFileHoarder, RddStreamSource}
import com.github.chenharryhua.nanjin.spark.table.NJTable
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class CrRdd[F[_], K, V] private[kafka] (
  val rdd: RDD[NJConsumerRecord[K, V]],
  ack: NJAvroCodec[K],
  acv: NJAvroCodec[V],
  cfg: SKConfig,
  ss: SparkSession)
    extends Serializable {

  protected val codec: NJAvroCodec[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(ack, acv)

  // transforms
  def transform(f: Endo[RDD[NJConsumerRecord[K, V]]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](f(rdd), ack, acv, cfg, ss)

  def partitionOf(num: Int): CrRdd[F, K, V] = transform(_.filter(_.partition === num))

  def offsetRange(start: Long, end: Long): CrRdd[F, K, V] = transform(range.cr.offset(start, end))
  def timeRange(dr: NJDateTimeRange): CrRdd[F, K, V]      = transform(range.cr.timestamp(dr))
  def timeRange: CrRdd[F, K, V]                           = timeRange(cfg.evalConfig.timeRange)

  def ascendTimestamp: CrRdd[F, K, V]  = transform(sort.ascend.cr.timestamp)
  def descendTimestamp: CrRdd[F, K, V] = transform(sort.descend.cr.timestamp)
  def ascendOffset: CrRdd[F, K, V]     = transform(sort.ascend.cr.offset)
  def descendOffset: CrRdd[F, K, V]    = transform(sort.descend.cr.offset)

  def union(other: CrRdd[F, K, V]): CrRdd[F, K, V] = transform(_.union(other.rdd))
  def repartition(num: Int): CrRdd[F, K, V]        = transform(_.repartition(num))

  def normalize: CrRdd[F, K, V] = transform(_.map(codec.idConversion))

  def dismissNulls: CrRdd[F, K, V] = transform(_.dismissNulls)

  def replicate(num: Int): CrRdd[F, K, V] =
    transform(rdd => (1 until num).foldLeft(rdd) { case (r, _) => r.union(rdd) })

  // maps
  def bimap[K2, V2](k: K => K2, v: V => V2)(ack2: NJAvroCodec[K2], acv2: NJAvroCodec[V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.map(_.bimap(k, v)), ack2, acv2, cfg, ss).normalize

  def map[K2, V2](f: NJConsumerRecord[K, V] => NJConsumerRecord[K2, V2])(
    ack2: NJAvroCodec[K2],
    acv2: NJAvroCodec[V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.map(f), ack2, acv2, cfg, ss).normalize

  def flatMap[K2, V2](f: NJConsumerRecord[K, V] => IterableOnce[NJConsumerRecord[K2, V2]])(
    ack2: NJAvroCodec[K2],
    acv2: NJAvroCodec[V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.flatMap(f), ack2, acv2, cfg, ss).normalize

  // transition
  def crDataset(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDataset[F, K, V] = {
    val ate = AvroTypedEncoder(ack, acv)
    new CrDataset[F, K, V](ss.createDataset(rdd)(ate.sparkEncoder), ack, acv, tek, tev)
  }

  def toTable(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): NJTable[F, NJConsumerRecord[K, V]] = {
    val ate = AvroTypedEncoder(ack, acv)
    new NJTable[F, NJConsumerRecord[K, V]](ss.createDataset(rdd)(ate.sparkEncoder), ate)
  }

  def prRdd: PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd.map(_.toNJProducerRecord), NJProducerRecord.avroCodec(ack, acv), cfg)

  val params: SKParams = cfg.evalConfig

  def save: RddAvroFileHoarder[F, NJConsumerRecord[K, V]] =
    new RddAvroFileHoarder[F, NJConsumerRecord[K, V]](rdd, codec.avroEncoder)

  // statistics
  def stats: Statistics[F] =
    new Statistics[F](
      TypedDataset.create(rdd.map(CRMetaInfo(_)))(TypedEncoder[CRMetaInfo], ss).dataset,
      params.timeRange.zoneId)

  def asSource: RddStreamSource[F, NJConsumerRecord[K, V]] =
    new RddStreamSource[F, NJConsumerRecord[K, V]](rdd)

  def count(implicit F: Sync[F]): F[Long] = F.delay(rdd.count())

  def cherrypick(partition: Int, offset: Long): Option[NJConsumerRecord[K, V]] =
    partitionOf(partition).offsetRange(offset, offset).rdd.collect().headOption

  def diff(other: RDD[NJConsumerRecord[K, V]])(implicit eqK: Eq[K], eqV: Eq[V]): RDD[DiffResult[K, V]] =
    inv.diffRdd(rdd, other)

  def diff(other: CrRdd[F, K, V])(implicit eqK: Eq[K], eqV: Eq[V]): RDD[DiffResult[K, V]] =
    diff(other.rdd)

  def diffKV(other: RDD[NJConsumerRecord[K, V]]): RDD[KvDiffResult[K, V]] = inv.kvDiffRdd(rdd, other)
  def diffKV(other: CrRdd[F, K, V]): RDD[KvDiffResult[K, V]]              = diffKV(other.rdd)

}
