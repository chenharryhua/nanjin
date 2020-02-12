package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.NJPath
import frameless.{TypedDataset, TypedEncoder}
import monocle.macros.Lenses
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

@Lenses final case class KafkaUnloadParams(
  timeRange: NJDateTimeRange,
  locationStrategy: LocationStrategy,
  path: NJPath,
  fileFormat: NJFileFormat)

object KafkaUnloadParams {

  def apply(path: NJPath, timeRange: NJDateTimeRange, fileFormat: NJFileFormat): KafkaUnloadParams =
    KafkaUnloadParams(
      timeRange        = timeRange,
      locationStrategy = LocationStrategies.PreferConsistent,
      path             = path,
      fileFormat       = fileFormat)
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
        ProcessConsumerRecordParams(params.timeRange.zoneId, params.fileFormat, params.path)))

}
