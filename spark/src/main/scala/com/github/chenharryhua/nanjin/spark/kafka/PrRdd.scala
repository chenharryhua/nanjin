package com.github.chenharryhua.nanjin.spark.kafka

import cats.Endo
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameC}
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import fs2.kafka.ProducerRecords
import fs2.Stream
import org.apache.spark.rdd.RDD

final class PrRdd[F[_], K, V] private[kafka] (
  val frdd: F[RDD[NJProducerRecord[K, V]]],
  codec: NJAvroCodec[NJProducerRecord[K, V]]
)(implicit F: Sync[F])
    extends Serializable {

  // transform
  def transform(f: Endo[RDD[NJProducerRecord[K, V]]]): PrRdd[F, K, V] =
    new PrRdd[F, K, V](F.flatMap(frdd)(rdd => F.blocking(f(rdd))), codec)

  def filter(f: NJProducerRecord[K, V] => Boolean): PrRdd[F, K, V] = transform(_.filter(f))
  def partitionOf(num: Int): PrRdd[F, K, V]                        = filter(_.partition.exists(_ === num))

  def offsetRange(start: Long, end: Long): PrRdd[F, K, V] = transform(range.pr.offset(start, end))
  def timeRange(dr: NJDateTimeRange): PrRdd[F, K, V]      = transform(range.pr.timestamp(dr))

  def ascendTimestamp: PrRdd[F, K, V]  = transform(sort.ascend.pr.timestamp)
  def descendTimestamp: PrRdd[F, K, V] = transform(sort.descend.pr.timestamp)
  def ascendOffset: PrRdd[F, K, V]     = transform(sort.ascend.pr.offset)
  def descendOffset: PrRdd[F, K, V]    = transform(sort.descend.pr.offset)

  def noTimestamp: PrRdd[F, K, V]                          = transform(_.map(_.noTimestamp))
  def noPartition: PrRdd[F, K, V]                          = transform(_.map(_.noPartition))
  def noMeta: PrRdd[F, K, V]                               = transform(_.map(_.noMeta))
  def withTopicName(topicName: TopicName): PrRdd[F, K, V]  = transform(_.map(_.withTopicName(topicName)))
  def withTopicName(topicName: TopicNameC): PrRdd[F, K, V] = withTopicName(TopicName(topicName))
  def replicate(num: Int): PrRdd[F, K, V] =
    transform(rdd => (1 until num).foldLeft(rdd) { case (r, _) => r.union(rdd) })

  def normalize: PrRdd[F, K, V] = transform(_.map(codec.idConversion))

  // actions

  def count: F[Long] = F.flatMap(frdd)(rdd => F.interruptible(rdd.count()))

  def output: RddAvroFileHoarder[F, NJProducerRecord[K, V]] =
    new RddAvroFileHoarder[F, NJProducerRecord[K, V]](frdd, codec.avroEncoder)

  def producerRecords(chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, ProducerRecords[K, V]] =
    Stream
      .eval(frdd)
      .flatMap(rdd =>
        Stream
          .fromBlockingIterator(rdd.toLocalIterator, chunkSize.value)
          .chunks
          .map(_.map(_.toProducerRecord)))

}
