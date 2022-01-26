package com.github.chenharryhua.nanjin.spark.kafka

import cats.Eq
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.persist.{HoarderConfig, RddAvroFileHoarder}
import com.github.chenharryhua.nanjin.terminals.NJPath
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class CrRdd[F[_], K, V] private[kafka] (
  val rdd: RDD[NJConsumerRecord[K, V]],
  topic: KafkaTopic[F, K, V],
  cfg: SKConfig,
  ss: SparkSession)
    extends Serializable {

  protected val codec: AvroCodec[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(topic.topicDef)

  // transforms
  def transform(f: RDD[NJConsumerRecord[K, V]] => RDD[NJConsumerRecord[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](f(rdd), topic, cfg, ss)

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
  def bimap[K2, V2](k: K => K2, v: V => V2)(other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.map(_.bimap(k, v)), other, cfg, ss).normalize

  def map[K2, V2](f: NJConsumerRecord[K, V] => NJConsumerRecord[K2, V2])(
    other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.map(f), other, cfg, ss).normalize

  def flatMap[K2, V2](f: NJConsumerRecord[K, V] => IterableOnce[NJConsumerRecord[K2, V2]])(
    other: KafkaTopic[F, K2, V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](rdd.flatMap(f), other, cfg, ss).normalize

  // transition
  def crDS(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): CrDS[F, K, V] = {
    val ate = NJConsumerRecord.ate(topic.topicDef)
    new CrDS[F, K, V](ss.createDataset(rdd)(ate.sparkEncoder), topic, cfg, tek, tev)
  }

  def prRdd: PrRdd[F, K, V] = new PrRdd[F, K, V](rdd.map(_.toNJProducerRecord), topic, cfg)

  def save(path: NJPath): RddAvroFileHoarder[F, NJConsumerRecord[K, V]] = {
    val params: SKParams = cfg.evalConfig
    new RddAvroFileHoarder[F, NJConsumerRecord[K, V]](
      rdd,
      codec.avroEncoder,
      HoarderConfig(path).chunkSize(params.loadParams.chunkSize).byteBuffer(params.loadParams.byteBuffer))
  }

  // statistics
  def stats: Statistics[F] =
    new Statistics[F](
      TypedDataset.create(rdd.map(CRMetaInfo(_)))(TypedEncoder[CRMetaInfo], ss).dataset,
      cfg.evalConfig.timeRange.zoneId)

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
