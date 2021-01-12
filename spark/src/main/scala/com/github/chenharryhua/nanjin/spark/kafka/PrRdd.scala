package com.github.chenharryhua.nanjin.spark.kafka

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.ProducerMessage
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink}
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.{akkaSinks, KafkaTopic}
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.persist.RddAvroFileHoarder
import frameless.cats.implicits._
import fs2.kafka.{ProducerRecords, ProducerResult}
import fs2.{Pipe, Stream}
import org.apache.spark.rdd.RDD

import scala.concurrent.duration.FiniteDuration

final class PrRdd[F[_], K, V] private[kafka] (
  val rdd: RDD[NJProducerRecord[K, V]],
  topic: KafkaTopic[F, K, V],
  cfg: SKConfig
) extends Serializable {

  val params: SKParams = cfg.evalConfig

  // config
  private def updateCfg(f: SKConfig => SKConfig): PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd, topic, f(cfg))

  def triggerEvery(ms: Long): PrRdd[F, K, V]           = updateCfg(_.withUploadInterval(ms))
  def triggerEvery(ms: FiniteDuration): PrRdd[F, K, V] = updateCfg(_.withUploadInterval(ms))
  def batchSize(num: Int): PrRdd[F, K, V]              = updateCfg(_.withUploadBatchSize(num))

  def recordsLimit(num: Long): PrRdd[F, K, V]       = updateCfg(_.withUploadRecordsLimit(num))
  def timeLimit(ms: Long): PrRdd[F, K, V]           = updateCfg(_.withUploadTimeLimit(ms))
  def timeLimit(fd: FiniteDuration): PrRdd[F, K, V] = updateCfg(_.withUploadTimeLimit(fd))

  // transform
  def transform(f: RDD[NJProducerRecord[K, V]] => RDD[NJProducerRecord[K, V]]): PrRdd[F, K, V] =
    new PrRdd[F, K, V](f(rdd), topic, cfg)

  def partitionOf(num: Int): PrRdd[F, K, V] = transform(_.filter(_.partition.exists(_ === num)))

  def offsetRange(start: Long, end: Long): PrRdd[F, K, V] = transform(range.pr.offset(start, end))
  def timeRange(dr: NJDateTimeRange): PrRdd[F, K, V]      = transform(range.pr.timestamp(dr))
  def timeRange: PrRdd[F, K, V]                           = timeRange(params.timeRange)

  def ascendTimestamp: PrRdd[F, K, V]  = transform(sort.ascend.pr.timestamp)
  def descendTimestamp: PrRdd[F, K, V] = transform(sort.descend.pr.timestamp)
  def ascendOffset: PrRdd[F, K, V]     = transform(sort.ascend.pr.offset)
  def descendOffset: PrRdd[F, K, V]    = transform(sort.descend.pr.offset)

  def noTimestamp: PrRdd[F, K, V] = transform(_.map(_.noTimestamp))
  def noPartition: PrRdd[F, K, V] = transform(_.map(_.noPartition))
  def noMeta: PrRdd[F, K, V]      = transform(_.map(_.noMeta))

  def replicate(num: Int): PrRdd[F, K, V] =
    transform(rdd => (1 until num).foldLeft(rdd) { case (r, _) => r.union(rdd) })

  def normalize: PrRdd[F, K, V] = transform(_.map(NJProducerRecord.avroCodec(topic.topicDef).idConversion))

  // maps
  def bimap[K2, V2](k: K => K2, v: V => V2)(other: KafkaTopic[F, K2, V2]): PrRdd[F, K2, V2] =
    new PrRdd[F, K2, V2](rdd.map(_.bimap(k, v)), other, cfg).normalize

  def map[K2, V2](f: NJProducerRecord[K, V] => NJProducerRecord[K2, V2])(
    other: KafkaTopic[F, K2, V2]): PrRdd[F, K2, V2] =
    new PrRdd[F, K2, V2](rdd.map(f), other, cfg).normalize

  def flatMap[K2, V2](f: NJProducerRecord[K, V] => TraversableOnce[NJProducerRecord[K2, V2]])(
    other: KafkaTopic[F, K2, V2]): PrRdd[F, K2, V2] =
    new PrRdd[F, K2, V2](rdd.flatMap(f), other, cfg).normalize

  // actions
  def upload(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] = {
    val producer: Pipe[F, ProducerRecords[K, V, Unit], ProducerResult[K, V, Unit]] =
      topic.fs2Channel(ce).producer[Unit]
    rdd
      .stream[F]
      .interruptAfter(params.uploadParams.timeLimit)
      .take(params.uploadParams.recordsLimit)
      .chunkN(params.uploadParams.batchSize)
      .map(chk => ProducerRecords(chk.map(_.toFs2ProducerRecord(topic.topicName.value))))
      .buffer(5)
      .metered(params.uploadParams.uploadInterval)
      .through(producer)
  }

  def bestEffort(akkaSystem: ActorSystem)(implicit F: ConcurrentEffect[F], cs: ContextShift[F]): F[Done] =
    F.defer {
      implicit val mat: Materializer = Materializer(akkaSystem)
      val flexiFlow: Flow[ProducerMessage.Envelope[K, V, NotUsed], ProducerMessage.Results[K, V, NotUsed], NotUsed] =
        topic.akkaChannel(akkaSystem).flexiFlow[NotUsed]
      val sink: Sink[Any, F[Done]] = akkaSinks.ignore[F]
      rdd
        .source[F]
        .take(params.uploadParams.recordsLimit)
        .takeWithin(params.uploadParams.timeLimit)
        .grouped(params.uploadParams.batchSize)
        .map(ms => ProducerMessage.multi(ms.map(_.toProducerRecord(topic.topicName.value))))
        .buffer(20, OverflowStrategy.backpressure)
        .via(flexiFlow)
        .runWith(sink)
    }

  def count(implicit F: Sync[F]): F[Long] = F.delay(rdd.count())

  def save: RddAvroFileHoarder[F, NJProducerRecord[K, V]] = {
    val ac = NJProducerRecord.avroCodec(topic.topicDef)
    new RddAvroFileHoarder[F, NJProducerRecord[K, V]](rdd, ac.avroEncoder)
  }
}
