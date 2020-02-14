package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategy

final class FsmKafkaUnload[F[_], K, V](kit: KafkaTopicKit[K, V], params: SKConfigParamF.ConfigParam)(
  implicit sparkSession: SparkSession)
    extends Serializable {

  def withLocationStrategy(ls: LocationStrategy): FsmKafkaUnload[F, K, V] =
    new FsmKafkaUnload[F, K, V](kit, SKConfigParamF.withLocationStrategy(ls, params))

  private val p: SKParams = SKConfigParamF.evalParams(params)

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
      new FsmConsumerRecords[F, K, V](tds.dataset, kit, params))

}
