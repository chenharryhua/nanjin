package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.jacksonFileSink
import com.github.chenharryhua.nanjin.utils.Keyboard
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.sql.SparkSession

final class FsmKafkaUnload[F[_], K, V](kit: KafkaTopicKit[K, V], cfg: SKConfig)(
  implicit sparkSession: SparkSession)
    extends SparKafkaUpdateParams[FsmKafkaUnload[F, K, V]] {

  override def withParamUpdate(f: SKConfig => SKConfig): FsmKafkaUnload[F, K, V] =
    new FsmKafkaUnload[F, K, V](kit, f(cfg))

  override val params: SKParams = SKConfigF.evalConfig(cfg)

  def transform[A](f: NJConsumerRecord[K, V] => A)(
    implicit
    F: Sync[F],
    encoder: TypedEncoder[A]): F[TypedDataset[A]] =
    sk.fromKafka(kit, params.timeRange, params.locationStrategy)(f)

  def timeRangedStream(implicit ev: Concurrent[F]): Stream[F, NJConsumerRecord[K, V]] =
    sk.timeRangedKafkaStream[F, K, V](
      kit,
      params.repartition,
      params.timeRange,
      params.locationStrategy)

  def saveJackson(path: String)(
    implicit concurrent: Concurrent[F],
    contextShift: ContextShift[F]
  ): F[Unit] = {
    import kit.topicDef.{avroKeyEncoder, avroValEncoder, schemaForKey, schemaForVal}

    val run: Stream[F, Unit] = for {
      kb <- Keyboard.signal[F]
      _ <- timeRangedStream
        .interruptWhen(kb.map(_.contains(Keyboard.Quit)))
        .through(jacksonFileSink[F, NJConsumerRecord[K, V]](path))
    } yield ()
    run.compile.drain
  }

  def consumerRecords(
    implicit
    F: Sync[F],
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[FsmConsumerRecords[F, K, V]] =
    transform[NJConsumerRecord[K, V]](identity).map(tds =>
      new FsmConsumerRecords[F, K, V](tds.dataset, kit, cfg))

}
