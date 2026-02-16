package com.github.chenharryhua.nanjin.spark.kafka

import cats.Endo
import cats.effect.kernel.Sync
import cats.syntax.eq.catsSyntaxEq
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.{MetaInfo, NJConsumerRecord}
import com.github.chenharryhua.nanjin.spark.describeJob
import fs2.Stream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

final class CrRdd[K, V](val rdd: RDD[NJConsumerRecord[K, V]], ss: SparkSession) extends Serializable {

  // transforms

  def transform(f: Endo[RDD[NJConsumerRecord[K, V]]]): CrRdd[K, V] =
    new CrRdd[K, V](f(rdd), ss)

  def filter(f: NJConsumerRecord[K, V] => Boolean): CrRdd[K, V] = transform(_.filter(f))
  def partitionOf(num: Int): CrRdd[K, V] = filter(_.partition === num)

  def offsetRange(start: Long, end: Long): CrRdd[K, V] = transform(range.cr.offset(start, end))
  def timeRange(dr: DateTimeRange): CrRdd[K, V] = transform(range.cr.timestamp(dr))

  def ascendTimestamp: CrRdd[K, V] = transform(sort.ascend.cr.timestamp)
  def descendTimestamp: CrRdd[K, V] = transform(sort.descend.cr.timestamp)
  def ascendOffset: CrRdd[K, V] = transform(sort.ascend.cr.offset)
  def descendOffset: CrRdd[K, V] = transform(sort.descend.cr.offset)

  def repartition(num: Int): CrRdd[K, V] = transform(_.repartition(num))
  def persist(f: StorageLevel.type => StorageLevel): CrRdd[K, V] =
    transform(_.persist(f(StorageLevel)))

  def diff(other: RDD[NJConsumerRecord[K, V]]): CrRdd[K, V] = transform(_.subtract(other))
  def diff(other: CrRdd[K, V]): CrRdd[K, V] = diff(other.rdd)

  def diff(other: RDD[NJConsumerRecord[K, V]], numPartitions: Int): CrRdd[K, V] = transform(
    _.subtract(other, numPartitions))
  def diff(other: CrRdd[K, V], numPartitions: Int): CrRdd[K, V] =
    diff(other.rdd, numPartitions)

  def union(other: RDD[NJConsumerRecord[K, V]]): CrRdd[K, V] = transform(_.union(other))
  def union(other: CrRdd[K, V]): CrRdd[K, V] = union(other.rdd)

  // transition

  def prRdd: PrRdd[K, V] =
    new PrRdd[K, V](rdd.map(_.toNJProducerRecord))

  def stats[F[_]]: Statistics[F] = {
    import ss.implicits.*
    new Statistics[F](ss.createDataset(rdd.map(MetaInfo(_))))
  }

  // IO

  def count[F[_]](implicit F: Sync[F]): F[Long] = F.delay(rdd.count())
  def count[F[_]: Sync](description: String): F[Long] =
    describeJob[F](rdd.sparkContext, description).surround(count[F])

  def cherryPick[F[_]](partition: Int, offset: Long)(implicit F: Sync[F]): F[Option[NJConsumerRecord[K, V]]] =
    F.delay(
      transform(_.filter(cr => cr.partition === partition && cr.offset === offset)).rdd.collect().headOption)

  def stream[F[_]: Sync](chunkSize: ChunkSize): Stream[F, NJConsumerRecord[K, V]] =
    Stream.fromBlockingIterator[F](rdd.toLocalIterator, chunkSize.value)
}
