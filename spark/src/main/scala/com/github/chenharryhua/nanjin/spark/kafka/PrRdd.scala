package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.spark.persist.{HoarderConfig, RddAvroFileHoarder}
import com.github.chenharryhua.nanjin.terminals.NJPath
import fs2.Stream
import fs2.kafka.ProducerRecords
import org.apache.spark.rdd.RDD

final class PrRdd[F[_], K, V] private[kafka] (
  val rdd: RDD[NJProducerRecord[K, V]],
  codec: AvroCodec[NJProducerRecord[K, V]],
  cfg: SKConfig
) extends Serializable {

  // transform
  def transform(f: RDD[NJProducerRecord[K, V]] => RDD[NJProducerRecord[K, V]]): PrRdd[F, K, V] =
    new PrRdd[F, K, V](f(rdd), codec, cfg)

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

  val params: SKParams = cfg.evalConfig

  def save(path: NJPath): RddAvroFileHoarder[F, NJProducerRecord[K, V]] =
    new RddAvroFileHoarder[F, NJProducerRecord[K, V]](
      rdd,
      codec.avroEncoder,
      HoarderConfig(path).chunkSize(params.loadParams.chunkSize).byteBuffer(params.loadParams.byteBuffer))

  def stream(implicit F: Sync[F]): Stream[F, NJProducerRecord[K, V]] = rdd.stream[F](params.loadParams.chunkSize)

  def producerRecords(topicName: TopicName)(implicit F: Sync[F]): Stream[F, ProducerRecords[Unit, K, V]] =
    stream.chunks.map(ms => ProducerRecords(ms.map(_.toFs2ProducerRecord(topicName))))

}
