package com.github.chenharryhua.nanjin.spark.kafka

import cats.Endo
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import fs2.Stream
import fs2.kafka.ProducerRecords
import org.apache.spark.rdd.RDD

final class PrRdd[K, V] private[kafka] (
  val rdd: RDD[NJProducerRecord[K, V]],
  codec: NJAvroCodec[NJProducerRecord[K, V]]
) extends Serializable {

  // transform
  def transform(f: Endo[RDD[NJProducerRecord[K, V]]]): PrRdd[K, V] =
    new PrRdd[K, V](f(rdd), codec)

  def filter(f: NJProducerRecord[K, V] => Boolean): PrRdd[K, V] = transform(_.filter(f))
  def partitionOf(num: Int): PrRdd[K, V]                        = filter(_.partition.exists(_ === num))

  def offsetRange(start: Long, end: Long): PrRdd[K, V] = transform(range.pr.offset(start, end))
  def timeRange(dr: DateTimeRange): PrRdd[K, V]        = transform(range.pr.timestamp(dr))

  def ascendTimestamp: PrRdd[K, V]  = transform(sort.ascend.pr.timestamp)
  def descendTimestamp: PrRdd[K, V] = transform(sort.descend.pr.timestamp)
  def ascendOffset: PrRdd[K, V]     = transform(sort.ascend.pr.offset)
  def descendOffset: PrRdd[K, V]    = transform(sort.descend.pr.offset)

  def noTimestamp: PrRdd[K, V]                          = transform(_.map(_.noTimestamp))
  def noPartition: PrRdd[K, V]                          = transform(_.map(_.noPartition))
  def noMeta: PrRdd[K, V]                               = transform(_.map(_.noMeta))
  def withTopicName(topicName: TopicName): PrRdd[K, V]  = transform(_.map(_.withTopicName(topicName)))
  def withTopicName(topicName: TopicNameL): PrRdd[K, V] = withTopicName(TopicName(topicName))
  def replicate(num: Int): PrRdd[K, V] =
    transform(rdd => (1 until num).foldLeft(rdd) { case (r, _) => r.union(rdd) })

  def normalize: PrRdd[K, V] = transform(_.map(codec.idConversion))

  // transition

  def output: RddAvroFileHoarder[NJProducerRecord[K, V]] =
    new RddAvroFileHoarder[NJProducerRecord[K, V]](rdd, codec)

  // IO

  def count[F[_]](implicit F: Sync[F]): F[Long] = F.blocking(rdd.count())

  def stream[F[_]: Sync](chunkSize: ChunkSize): Stream[F, NJProducerRecord[K, V]] =
    Stream.fromBlockingIterator[F](rdd.toLocalIterator, chunkSize.value)

  def producerRecords[F[_]](chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, ProducerRecords[K, V]] =
    Stream.fromBlockingIterator[F](rdd.toLocalIterator.map(_.toProducerRecord), chunkSize.value).chunks

}
