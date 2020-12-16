package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark._
import frameless.cats.implicits._
import fs2.Stream
import fs2.kafka.ProducerResult
import org.apache.spark.rdd.RDD

import scala.concurrent.duration.FiniteDuration

final class PrRdd[F[_], K, V] private[kafka] (
  topic: KafkaTopic[F, K, V],
  rdd: RDD[NJProducerRecord[K, V]],
  cfg: SKConfig
) extends Serializable {

  val params: SKParams = cfg.evalConfig

  def withParamUpdate(f: SKConfig => SKConfig): PrRdd[F, K, V] =
    new PrRdd[F, K, V](topic, rdd, f(cfg))

  def interval(ms: Long): PrRdd[F, K, V]           = withParamUpdate(_.withUploadInterval(ms))
  def interval(ms: FiniteDuration): PrRdd[F, K, V] = withParamUpdate(_.withUploadInterval(ms))

  def batch(num: Int): PrRdd[F, K, V]         = withParamUpdate(_.withUploadBatchSize(num))
  def recordsLimit(num: Long): PrRdd[F, K, V] = withParamUpdate(_.withUploadRecordsLimit(num))

  def timeLimit(ms: Long): PrRdd[F, K, V]           = withParamUpdate(_.withUploadTimeLimit(ms))
  def timeLimit(fd: FiniteDuration): PrRdd[F, K, V] = withParamUpdate(_.withUploadTimeLimit(fd))

  def noTimestamp: PrRdd[F, K, V] =
    new PrRdd[F, K, V](topic, rdd.map(_.noTimestamp), cfg)

  def noPartition: PrRdd[F, K, V] =
    new PrRdd[F, K, V](topic, rdd.map(_.noPartition), cfg)

  def noMeta: PrRdd[F, K, V] =
    new PrRdd[F, K, V](topic, rdd.map(_.noMeta), cfg)

  // actions
  def pipeTo[K2, V2](other: KafkaTopic[F, K2, V2])(k: K => K2, v: V => V2)(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K2, V2, Unit]] =
    rdd.stream[F].map(_.bimap(k, v)).through(sk.uploader(other, params.uploadParams))

  def upload(other: KafkaTopic[F, K, V])(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    pipeTo(other)(identity, identity)

  def upload(implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    upload(topic)

  def count(implicit F: Sync[F]): F[Long] = F.delay(rdd.count())
}
