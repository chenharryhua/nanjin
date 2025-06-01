package com.github.chenharryhua.nanjin.spark.kafka

import cats.Endo
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.{CRMetaInfo, NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.SchematizedEncoder
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import com.github.chenharryhua.nanjin.spark.table.Table
import frameless.TypedEncoder
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

final class CrRdd[K, V] private[kafka] (
  val rdd: RDD[NJConsumerRecord[K, V]],
  ack: AvroCodec[K],
  acv: AvroCodec[V],
  ss: SparkSession)
    extends Serializable {

  protected val codec: AvroCodec[NJConsumerRecord[K, V]] = NJConsumerRecord.avroCodec(ack, acv)

  // transforms

  def transform(f: Endo[RDD[NJConsumerRecord[K, V]]]): CrRdd[K, V] =
    new CrRdd[K, V](f(rdd), ack, acv, ss)

  def filter(f: NJConsumerRecord[K, V] => Boolean): CrRdd[K, V] = transform(_.filter(f))
  def partitionOf(num: Int): CrRdd[K, V]                        = filter(_.partition === num)

  def offsetRange(start: Long, end: Long): CrRdd[K, V] = transform(range.cr.offset(start, end))
  def timeRange(dr: DateTimeRange): CrRdd[K, V]        = transform(range.cr.timestamp(dr))

  def ascendTimestamp: CrRdd[K, V]  = transform(sort.ascend.cr.timestamp)
  def descendTimestamp: CrRdd[K, V] = transform(sort.descend.cr.timestamp)
  def ascendOffset: CrRdd[K, V]     = transform(sort.ascend.cr.offset)
  def descendOffset: CrRdd[K, V]    = transform(sort.descend.cr.offset)

  def repartition(num: Int): CrRdd[K, V] = transform(_.repartition(num))
  def persist(f: StorageLevel.type => StorageLevel): CrRdd[K, V] =
    transform(_.persist(f(StorageLevel)))

  def normalize: CrRdd[K, V] = transform(_.map(codec.idConversion))

  def diff(other: RDD[NJConsumerRecord[K, V]]): CrRdd[K, V] = transform(_.subtract(other))
  def diff(other: CrRdd[K, V]): CrRdd[K, V]                 = diff(other.rdd)

  def diff(other: RDD[NJConsumerRecord[K, V]], numPartitions: Int): CrRdd[K, V] = transform(
    _.subtract(other, numPartitions))
  def diff(other: CrRdd[K, V], numPartitions: Int): CrRdd[K, V] =
    diff(other.rdd, numPartitions)

  def union(other: RDD[NJConsumerRecord[K, V]]): CrRdd[K, V] = transform(_.union(other))
  def union(other: CrRdd[K, V]): CrRdd[K, V]                 = union(other.rdd)

  // transition

  def toTable(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): Table[NJConsumerRecord[K, V]] = {
    val ate: SchematizedEncoder[NJConsumerRecord[K, V]] = SchematizedEncoder(ack, acv)
    new Table[NJConsumerRecord[K, V]](ss.createDataset(rdd)(ate.sparkEncoder), ate)
  }

  def prRdd: PrRdd[K, V] =
    new PrRdd[K, V](rdd.map(_.toNJProducerRecord), NJProducerRecord.avroCodec(ack, acv))

  def output: RddAvroFileHoarder[NJConsumerRecord[K, V]] =
    new RddAvroFileHoarder[NJConsumerRecord[K, V]](rdd, codec)

  def stats: Statistics =
    new Statistics(ss.createDataset(rdd.map(CRMetaInfo(_))))

  // IO

  def count[F[_]](implicit F: Sync[F]): F[Long] = F.interruptible(rdd.count())

  def cherryPick[F[_]](partition: Int, offset: Long)(implicit F: Sync[F]): F[Option[NJConsumerRecord[K, V]]] =
    F.interruptible(
      transform(_.filter(cr => cr.partition === partition && cr.offset === offset)).rdd.collect().headOption)

  def stream[F[_]: Sync](chunkSize: ChunkSize): Stream[F, NJConsumerRecord[K, V]] =
    Stream.fromBlockingIterator[F](rdd.toLocalIterator, chunkSize.value)
}
