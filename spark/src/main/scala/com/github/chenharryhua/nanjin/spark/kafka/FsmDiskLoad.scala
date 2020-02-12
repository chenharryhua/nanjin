package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.spark.NJPath
import frameless.TypedEncoder
import monocle.macros.Lenses
import org.apache.spark.sql.SparkSession

@Lenses final case class DiskLoadParams(
  timeRange: NJDateTimeRange,
  fileFormat: NJFileFormat,
  path: NJPath
)

final class FsmDiskLoad[F[_], K, V](kit: KafkaTopicKit[K, V], params: DiskLoadParams)(
  implicit sparkSession: SparkSession) {

  def consumerRecords(
    implicit
    F: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[FsmProcessConsumerRecords[F, K, V]] =
    Sync[F].delay(
      new FsmProcessConsumerRecords(
        sk.fromDisk(kit, params.timeRange, params.fileFormat, params.path).dataset,
        kit,
        ProcessConsumerRecordParams(params.timeRange.zoneId, params.fileFormat, params.path)))
}
