package com.github.chenharryhua.nanjin.spark.kafka

import cats.{Endo, Monad}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import com.github.chenharryhua.nanjin.spark.table.NJTable
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class CrRdd[F[_]: Monad, K, V] private[kafka] (
  val frdd: F[RDD[NJConsumerRecord[K, V]]],
  ack: NJAvroCodec[K],
  acv: NJAvroCodec[V],
  ss: SparkSession)
    extends Serializable {

  protected val codec: NJAvroCodec[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(ack, acv)

  // transforms
  def transform(f: Endo[RDD[NJConsumerRecord[K, V]]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](frdd.map(f), ack, acv, ss)

  def filter(f: NJConsumerRecord[K, V] => Boolean): CrRdd[F, K, V] = transform(_.filter(f))
  def partitionOf(num: Int): CrRdd[F, K, V]                        = filter(_.partition === num)

  def offsetRange(start: Long, end: Long): CrRdd[F, K, V] = transform(range.cr.offset(start, end))
  def timeRange(dr: NJDateTimeRange): CrRdd[F, K, V]      = transform(range.cr.timestamp(dr))

  def ascendTimestamp: CrRdd[F, K, V]  = transform(sort.ascend.cr.timestamp)
  def descendTimestamp: CrRdd[F, K, V] = transform(sort.descend.cr.timestamp)
  def ascendOffset: CrRdd[F, K, V]     = transform(sort.ascend.cr.offset)
  def descendOffset: CrRdd[F, K, V]    = transform(sort.descend.cr.offset)

  def union(other: CrRdd[F, K, V]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](frdd.flatMap(me => other.frdd.map(me.union)), ack, acv, ss)
  def repartition(num: Int): CrRdd[F, K, V] = transform(_.repartition(num))

  def normalize: CrRdd[F, K, V] = transform(_.map(codec.idConversion))

  def replicate(num: Int): CrRdd[F, K, V] =
    transform(rdd => (1 until num).foldLeft(rdd) { case (r, _) => r.union(rdd) })

  // maps
  def bimap[K2, V2](k: K => K2, v: V => V2)(ack2: NJAvroCodec[K2], acv2: NJAvroCodec[V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](frdd.map(_.map(_.bimap(k, v))), ack2, acv2, ss).normalize

  def map[K2, V2](f: NJConsumerRecord[K, V] => NJConsumerRecord[K2, V2])(
    ack2: NJAvroCodec[K2],
    acv2: NJAvroCodec[V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](frdd.map(_.map(f)), ack2, acv2, ss).normalize

  def flatMap[K2, V2](f: NJConsumerRecord[K, V] => IterableOnce[NJConsumerRecord[K2, V2]])(
    ack2: NJAvroCodec[K2],
    acv2: NJAvroCodec[V2]): CrRdd[F, K2, V2] =
    new CrRdd[F, K2, V2](frdd.map(_.flatMap(f)), ack2, acv2, ss).normalize

  // transition

  def asTable(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): NJTable[F, NJConsumerRecord[K, V]] = {
    val ate: AvroTypedEncoder[NJConsumerRecord[K, V]] = AvroTypedEncoder(ack, acv)
    new NJTable[F, NJConsumerRecord[K, V]](frdd.map(ss.createDataset(_)(ate.sparkEncoder)), ate)
  }

  def prRdd: PrRdd[F, K, V] =
    new PrRdd[F, K, V](frdd.map(_.map(_.toNJProducerRecord)), NJProducerRecord.avroCodec(ack, acv))

  def output: RddAvroFileHoarder[F, NJConsumerRecord[K, V]] =
    new RddAvroFileHoarder[F, NJConsumerRecord[K, V]](frdd, codec.avroEncoder)

  // statistics
  def stats: Statistics[F] = {
    val te = TypedEncoder[CRMetaInfo]
    new Statistics[F](frdd.map { rdd =>
      ss.createDataset(rdd.map(CRMetaInfo(_)))(TypedExpressionEncoder(te))
    })
  }

  def count: F[Long] = frdd.map(_.count())

  def cherrypick(partition: Int, offset: Long): F[Option[NJConsumerRecord[K, V]]] =
    partitionOf(partition).offsetRange(offset, offset).frdd.map(_.collect().headOption)

  def diff(other: RDD[NJConsumerRecord[K, V]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](frdd.map(_.subtract(other)), ack, acv, ss)

  def diff(other: CrRdd[F, K, V]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](frdd.flatMap(r => other.frdd.map(o => r.subtract(o))), ack, acv, ss)
}
