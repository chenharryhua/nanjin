package com.github.chenharryhua.nanjin.spark.kafka

import akka.NotUsed
import akka.kafka.ProducerMessage.{multi, Envelope}
import akka.stream.scaladsl.Source
import cats.effect.kernel.Sync
import cats.syntax.all.*
import cats.Endo
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.NJProducerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.persist.{RddAvroFileHoarder, RddStreamSource}
import fs2.kafka.ProducerRecords
import org.apache.spark.rdd.RDD

final class PrRdd[F[_], K, V] private[kafka] (
  val rdd: RDD[NJProducerRecord[K, V]],
  codec: NJAvroCodec[NJProducerRecord[K, V]]
) extends Serializable {

  // transform
  def transform(f: Endo[RDD[NJProducerRecord[K, V]]]): PrRdd[F, K, V] =
    new PrRdd[F, K, V](f(rdd), codec)

  def partitionOf(num: Int): PrRdd[F, K, V] = transform(_.filter(_.partition.exists(_ === num)))

  def offsetRange(start: Long, end: Long): PrRdd[F, K, V] = transform(range.pr.offset(start, end))
  def timeRange(dr: NJDateTimeRange): PrRdd[F, K, V]      = transform(range.pr.timestamp(dr))

  def ascendTimestamp: PrRdd[F, K, V]  = transform(sort.ascend.pr.timestamp)
  def descendTimestamp: PrRdd[F, K, V] = transform(sort.descend.pr.timestamp)
  def ascendOffset: PrRdd[F, K, V]     = transform(sort.ascend.pr.offset)
  def descendOffset: PrRdd[F, K, V]    = transform(sort.descend.pr.offset)

  def noTimestamp: PrRdd[F, K, V] = transform(_.map(_.noTimestamp))
  def noPartition: PrRdd[F, K, V] = transform(_.map(_.noPartition))
  def noMeta: PrRdd[F, K, V]      = transform(_.map(_.noMeta))

  def replicate(num: Int): PrRdd[F, K, V] =
    transform(rdd => (1 until num).foldLeft(rdd) { case (r, _) => r.union(rdd) })

  def normalize: PrRdd[F, K, V] = transform(_.map(codec.idConversion))

  // actions

  def count(implicit F: Sync[F]): F[Long] = F.delay(rdd.count())

  def save: RddAvroFileHoarder[F, NJProducerRecord[K, V]] =
    new RddAvroFileHoarder[F, NJProducerRecord[K, V]](rdd, codec.avroEncoder)

  lazy val asSource: RddStreamSource[F, NJProducerRecord[K, V]] =
    new RddStreamSource[F, NJProducerRecord[K, V]](rdd)

  def producerRecords(topicName: TopicName, chunkSize: ChunkSize)(implicit
    F: Sync[F]): fs2.Stream[F, ProducerRecords[Unit, K, V]] =
    asSource.stream(chunkSize).chunks.map(ms => ProducerRecords(ms.map(_.toFs2ProducerRecord(topicName))))

  def producerMessages(topicName: TopicName, chunkSize: ChunkSize): Source[Envelope[K, V, NotUsed], NotUsed] =
    asSource.source.grouped(chunkSize.value).map(ms => multi(ms.map(_.toProducerRecord(topicName))))
}
