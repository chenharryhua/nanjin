package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark._
import frameless.cats.implicits._
import fs2.Stream
import fs2.kafka.ProducerResult
import org.apache.spark.rdd.RDD

final class PrRdd[F[_], K, V](
  rdd: RDD[NJProducerRecord[K, V]],
  cfg: SKConfig
) extends SparKafkaUpdateParams[PrRdd[F, K, V]] {

  override val params: SKParams = cfg.evalConfig

  override def withParamUpdate(f: SKConfig => SKConfig): PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd, f(cfg))

  def noTimestamp: PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd.map(_.noTimestamp), cfg)

  def noPartition: PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd.map(_.noPartition), cfg)

  def noMeta: PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd.map(_.noMeta), cfg)

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

  def count(implicit F: Sync[F]): F[Long] = F.delay(rdd.count())
}
