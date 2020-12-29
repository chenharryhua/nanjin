package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

final class CrDS[F[_], K, V] private[kafka] (
  val topic: KafkaTopic[F, K, V],
  val dataset: Dataset[OptionalKV[K, V]],
  val cfg: SKConfig,
  tek: TypedEncoder[K],
  tev: TypedEncoder[V])
    extends Serializable {

  val ate: AvroTypedEncoder[OptionalKV[K, V]] = OptionalKV.ate(topic.topicDef)(tek, tev)

  val params: SKParams = cfg.evalConfig

  def typedDataset: TypedDataset[OptionalKV[K, V]] = TypedDataset.create(dataset)(ate.typedEncoder)

  def transform(f: Dataset[OptionalKV[K, V]] => Dataset[OptionalKV[K, V]]): CrDS[F, K, V] =
    new CrDS[F, K, V](topic, dataset.transform(f), cfg, tek, tev)

  def partitionOf(num: Int): CrDS[F, K, V] = transform(_.filter(col("partition") === num))

  def offsetRange(start: Long, end: Long): CrDS[F, K, V] = transform(range.offset(start, end))
  def timeRange(dr: NJDateTimeRange): CrDS[F, K, V]      = transform(range.timestamp(dr))
  def timeRange: CrDS[F, K, V]                           = timeRange(params.timeRange)

  def ascendOffset: CrDS[F, K, V]     = transform(sort.ascend.offset)
  def descendOffset: CrDS[F, K, V]    = transform(sort.descend.offset)
  def ascendTimestamp: CrDS[F, K, V]  = transform(sort.ascend.timestamp)
  def descendTimestamp: CrDS[F, K, V] = transform(sort.descend.timestamp)

  def union(other: CrDS[F, K, V]): CrDS[F, K, V] = transform(_.union(other.dataset))
  def repartition(num: Int): CrDS[F, K, V]       = transform(_.repartition(num))

  def normalize: CrDS[F, K, V] = transform(ate.normalize(_).dataset)

  // maps
  def bimap[K2, V2](k: K => K2, v: V => V2)(
    other: KafkaTopic[F, K2, V2])(implicit k2: TypedEncoder[K2], v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[OptionalKV[K2, V2]] = OptionalKV.ate(other.topicDef)
    new CrDS[F, K2, V2](other, dataset.map(_.bimap(k, v))(ate.sparkEncoder), cfg, k2, v2).normalize
  }

  def map[K2, V2](f: OptionalKV[K, V] => OptionalKV[K2, V2])(
    other: KafkaTopic[F, K2, V2])(implicit k2: TypedEncoder[K2], v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[OptionalKV[K2, V2]] = OptionalKV.ate(other.topicDef)
    new CrDS[F, K2, V2](other, dataset.map(f)(ate.sparkEncoder), cfg, k2, v2).normalize
  }

  def flatMap[K2, V2](f: OptionalKV[K, V] => TraversableOnce[OptionalKV[K2, V2]])(
    other: KafkaTopic[F, K2, V2])(implicit k2: TypedEncoder[K2], v2: TypedEncoder[V2]): CrDS[F, K2, V2] = {
    val ate: AvroTypedEncoder[OptionalKV[K2, V2]] = OptionalKV.ate(other.topicDef)
    new CrDS[F, K2, V2](other, dataset.flatMap(f)(ate.sparkEncoder), cfg, k2, v2).normalize
  }

  def stats: Statistics[F] = {
    val enc = TypedExpressionEncoder[CRMetaInfo]
    new Statistics[F](dataset.map(CRMetaInfo(_))(enc), params.timeRange.zoneId)
  }

  def crRdd: CrRdd[F, K, V] = new CrRdd[F, K, V](topic, dataset.rdd, cfg, dataset.sparkSession)
  def prRdd: PrRdd[F, K, V] = new PrRdd[F, K, V](topic, dataset.rdd.map(_.toNJProducerRecord), cfg)

  def save: DatasetAvroFileHoarder[F, OptionalKV[K, V]] =
    new DatasetAvroFileHoarder[F, OptionalKV[K, V]](dataset, ate.avroCodec.avroEncoder)

  def count(implicit F: Sync[F]): F[Long] = F.delay(dataset.count())

  /** Notes:
    *  same key should be in same partition.
    */
  def misplacedKey: TypedDataset[MissplacedKey[K]] = {
    import frameless.functions.aggregate.countDistinct
    implicit val enc: TypedEncoder[K]       = tek
    val tds: TypedDataset[OptionalKV[K, V]] = typedDataset
    val res: TypedDataset[MissplacedKey[K]] =
      tds.groupBy(tds('key)).agg(countDistinct(tds('partition))).as[MissplacedKey[K]]
    res.filter(res('count) > 1).orderBy(res('count).asc)
  }

  /** Notes:
    * timestamp order should follows offset order:
    * the larger the offset is the larger of timestamp should be, of the same key
    */
  def misorderedKey: TypedDataset[MisorderedKey[K]] = {
    implicit val enc: TypedEncoder[K]       = tek
    val tds: TypedDataset[OptionalKV[K, V]] = typedDataset
    tds.groupBy(tds('key)).deserialized.flatMapGroups { case (key, iter) =>
      key.traverse { key =>
        iter.toList.sortBy(_.offset).sliding(2).toList.flatMap {
          case List(c, n) =>
            if (n.timestamp >= c.timestamp) None
            else Some(MisorderedKey(key, c.partition, c.offset, c.timestamp, n.partition, n.offset, n.timestamp))
          case _ => None // single item list
        }
      }.flatten
    }
  }
}

final case class MisorderedKey[K](
  key: K,
  partition: Int,
  offset: Long,
  ts: Long,
  nextPartition: Int,
  nextOffset: Long,
  nextTs: Long)

final case class MissplacedKey[K](key: Option[K], count: Long)
