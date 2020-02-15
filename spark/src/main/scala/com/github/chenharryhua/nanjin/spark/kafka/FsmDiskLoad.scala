package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import frameless.TypedEncoder
import org.apache.spark.sql.SparkSession

final class FsmDiskLoad[F[_], K, V](kit: KafkaTopicKit[K, V], params: SKConfigF.SKConfig)(
  implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmDiskLoad[F, K, V]] {

  override def withParamUpdate(f: SKConfigF.SKConfig => SKConfigF.SKConfig): FsmDiskLoad[F, K, V] =
    new FsmDiskLoad[F, K, V](kit, f(params))

  private val p: SKParams = SKConfigF.evalParams(params)

  def consumerRecords(
    implicit
    F: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[FsmConsumerRecords[F, K, V]] =
    Sync[F].delay(
      new FsmConsumerRecords(
        sk.fromDisk(
            kit,
            p.timeRange,
            p.fileFormat,
            p.pathBuilder.run(NJPathBuild(p.fileFormat, kit.topicName)))
          .dataset,
        kit,
        params))
}
