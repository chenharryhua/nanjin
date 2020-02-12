package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}
import cats.implicits._
import monocle.macros.Lenses

@Lenses final case class KafkaUnloadParams(
  timeRange: NJDateTimeRange,
  locationStrategy: LocationStrategy,
  path: String)

object KafkaUnloadParams {

  def apply(path: String, timeRange: NJDateTimeRange): KafkaUnloadParams =
    KafkaUnloadParams(
      timeRange        = timeRange,
      locationStrategy = LocationStrategies.PreferConsistent,
      path             = path)
}

final class FsmKafkaUnload[F[_], K, V](kit: KafkaTopicKit[K, V], params: KafkaUnloadParams)(
  implicit sparkSession: SparkSession) {

  def withLocationStrategy(ls: LocationStrategy): FsmKafkaUnload[F, K, V] =
    new FsmKafkaUnload[F, K, V](kit, KafkaUnloadParams.locationStrategy.set(ls)(params))

  def transform[A](f: NJConsumerRecord[K, V] => A)(
    implicit
    F: Sync[F],
    encoder: TypedEncoder[A]): F[TypedDataset[A]] =
    sk.fromKafka(kit, params.timeRange, params.locationStrategy)(f)

  def consumerRecords(
    implicit
    F: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[FsmProcessConsumerRecords[F, K, V]] =
    transform[NJConsumerRecord[K, V]](identity).map(tds =>
      new FsmProcessConsumerRecords[F, K, V](
        tds.dataset,
        kit,
        ProcessConsumerRecordParams(params.timeRange.zoneId, params.path)))

}
