package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

final class FsmKafkaUnload[F[_], K, V](kit: KafkaTopicKit[K, V], cfg: SKConfig)(
  implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmKafkaUnload[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmKafkaUnload[F, K, V] =
    new FsmKafkaUnload[F, K, V](kit, f(cfg))

  private val p: SKParams = SKConfigF.evalParams(cfg)

  def transform[A](f: NJConsumerRecord[K, V] => A)(
    implicit
    F: Sync[F],
    encoder: TypedEncoder[A]): F[TypedDataset[A]] =
    sk.fromKafka(kit, p.timeRange, p.locationStrategy)(f)

  def consumerRecords(
    implicit
    F: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[FsmConsumerRecords[F, K, V]] =
    transform[NJConsumerRecord[K, V]](identity).map(tds =>
      new FsmConsumerRecords[F, K, V](tds.dataset, kit, cfg))

}
