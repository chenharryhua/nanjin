package com.github.chenharryhua.nanjin.spark.kafka

import cats.Endo
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import com.github.chenharryhua.nanjin.spark.table.NJTable
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class CrRdd[F[_], K, V] private[kafka] (
  val frdd: F[RDD[NJConsumerRecord[K, V]]],
  ack: NJAvroCodec[K],
  acv: NJAvroCodec[V],
  ss: SparkSession)(implicit F: Sync[F])
    extends Serializable {

  protected val codec: NJAvroCodec[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(ack, acv)

  // transforms
  def transform(f: Endo[RDD[NJConsumerRecord[K, V]]]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](F.flatMap(frdd)(rdd => F.blocking(f(rdd))), ack, acv, ss)

  def filter(f: NJConsumerRecord[K, V] => Boolean): CrRdd[F, K, V] = transform(_.filter(f))
  def partitionOf(num: Int): CrRdd[F, K, V]                        = filter(_.partition === num)

  def offsetRange(start: Long, end: Long): CrRdd[F, K, V] = transform(range.cr.offset(start, end))
  def timeRange(dr: NJDateTimeRange): CrRdd[F, K, V]      = transform(range.cr.timestamp(dr))

  def ascendTimestamp: CrRdd[F, K, V]  = transform(sort.ascend.cr.timestamp)
  def descendTimestamp: CrRdd[F, K, V] = transform(sort.descend.cr.timestamp)
  def ascendOffset: CrRdd[F, K, V]     = transform(sort.ascend.cr.offset)
  def descendOffset: CrRdd[F, K, V]    = transform(sort.descend.cr.offset)

  def union(other: CrRdd[F, K, V]): CrRdd[F, K, V] = {
    val rdd = for {
      me <- frdd
      you <- other.frdd
      rdd <- F.blocking(me.union(you))
    } yield rdd
    new CrRdd[F, K, V](rdd, ack, acv, ss)
  }

  def repartition(num: Int): CrRdd[F, K, V] = transform(_.repartition(num))

  def normalize: CrRdd[F, K, V] = transform(_.map(codec.idConversion))

  def replicate(num: Int): CrRdd[F, K, V] =
    transform(rdd => (1 until num).foldLeft(rdd) { case (r, _) => r.union(rdd) })

  // maps
  def bimap[K2, V2](k: K => K2, v: V => V2)(
    ack2: NJAvroCodec[K2],
    acv2: NJAvroCodec[V2]): CrRdd[F, K2, V2] = {
    val rdd = F.flatMap(frdd)(rdd => F.blocking(rdd.map(_.bimap(k, v))))
    new CrRdd[F, K2, V2](rdd, ack2, acv2, ss).normalize
  }

  def map[K2, V2](f: NJConsumerRecord[K, V] => NJConsumerRecord[K2, V2])(
    ack2: NJAvroCodec[K2],
    acv2: NJAvroCodec[V2]): CrRdd[F, K2, V2] = {
    val rdd = F.flatMap(frdd)(rdd => F.blocking(rdd.map(f)))
    new CrRdd[F, K2, V2](rdd, ack2, acv2, ss).normalize
  }

  def flatMap[K2, V2](f: NJConsumerRecord[K, V] => IterableOnce[NJConsumerRecord[K2, V2]])(
    ack2: NJAvroCodec[K2],
    acv2: NJAvroCodec[V2]): CrRdd[F, K2, V2] = {
    val rdd = F.flatMap(frdd)(rdd => F.blocking(rdd.flatMap(f)))
    new CrRdd[F, K2, V2](rdd, ack2, acv2, ss).normalize
  }

  // transition

  def asTable(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): NJTable[F, NJConsumerRecord[K, V]] = {
    val ate: AvroTypedEncoder[NJConsumerRecord[K, V]] = AvroTypedEncoder(ack, acv)
    val ds = F.flatMap(frdd)(rdd => F.blocking(ss.createDataset(rdd)(ate.sparkEncoder)))
    new NJTable[F, NJConsumerRecord[K, V]](ds, ate)
  }

  def prRdd: PrRdd[F, K, V] = {
    val rdd = F.flatMap(frdd)(rdd => F.blocking(rdd.map(_.toNJProducerRecord)))
    new PrRdd[F, K, V](rdd, NJProducerRecord.avroCodec(ack, acv))
  }

  def output: RddAvroFileHoarder[F, NJConsumerRecord[K, V]] =
    new RddAvroFileHoarder[F, NJConsumerRecord[K, V]](frdd, codec.avroEncoder)

  // statistics
  def stats: Statistics[F] = {
    val te = TypedExpressionEncoder(TypedEncoder[CRMetaInfo])
    val ds = F.flatMap(frdd)(rdd => F.blocking(ss.createDataset(rdd.map(CRMetaInfo(_)))(te)))
    new Statistics[F](ds)
  }

  def count: F[Long] = F.flatMap(frdd)(rdd => F.blocking(rdd.count()))

  def cherrypick(partition: Int, offset: Long): F[Option[NJConsumerRecord[K, V]]] =
    F.flatMap(partitionOf(partition).offsetRange(offset, offset).frdd)(rdd =>
      F.blocking(rdd.collect().headOption))

  def diff(other: RDD[NJConsumerRecord[K, V]]): CrRdd[F, K, V] =
    transform(_.subtract(other))

  def diff(other: CrRdd[F, K, V]): CrRdd[F, K, V] = {
    val rdd = for {
      me <- frdd
      you <- other.frdd
      rdd <- F.blocking(me.subtract(you))
    } yield rdd
    new CrRdd[F, K, V](rdd, ack, acv, ss)
  }
}
