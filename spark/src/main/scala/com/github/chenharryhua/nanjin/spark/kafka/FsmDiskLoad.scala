package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Concurrent, ContextShift, Sync}
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.utils.Keyboard
import frameless.TypedEncoder
import fs2.Stream
import org.apache.spark.sql.SparkSession

final class FsmDiskLoad[F[_], K, V](kit: KafkaTopicKit[K, V], cfg: SKConfig)(
  implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmDiskLoad[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmDiskLoad[F, K, V] =
    new FsmDiskLoad[F, K, V](kit, f(cfg))

  override val params: SKParams = SKConfigF.evalConfig(cfg)

  def consumerRecords(
    implicit
    F: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[FsmConsumerRecords[F, K, V]] =
    Sync[F].delay(
      new FsmConsumerRecords(
        sk.fromDisk(
            kit,
            params.timeRange,
            params.fileFormat,
            params.pathBuilder.run(NJPathBuild(params.fileFormat, kit.topicName)))
          .dataset,
        kit,
        cfg))
}
