package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, NJProducerRecord}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

trait FsmSparKafka extends Serializable

final class SparKafkaSession[K, V](val kit: KafkaTopicKit[K, V], val params: SparKafkaParams)(
  implicit sparkSession: SparkSession)
    extends FsmSparKafka with UpdateParams[SparKafkaParams, SparKafkaSession[K, V]] {

  override def withParamUpdate(f: SparKafkaParams => SparKafkaParams): SparKafkaSession[K, V] =
    new SparKafkaSession[K, V](kit, f(params))

  def fromKafka[F[_]: Sync, A](f: NJConsumerRecord[K, V] => A)(
    implicit ev: TypedEncoder[A]): F[TypedDataset[A]] =
    sk.fromKafka(kit, params.timeRange, params.locationStrategy)(f)

  def fromKafka[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[FsmConsumerRecords[F, K, V]] =
    fromKafka(identity).map(crDataset)

  def fromDisk[F[_]](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] =
    crDataset(sk.fromDisk(kit, params.timeRange, params.fileFormat, params.getPath(kit.topicName)))

  def crDataset[F[_], A](cr: TypedDataset[NJConsumerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](cr.dataset, this)

  def prDataset[F[_], A](pr: TypedDataset[NJProducerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](pr.dataset, this)

  def replay[F[_]: ConcurrentEffect: Timer: ContextShift](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    fromDisk[F].toProducerRecords.upload.map(_ => print(".")).compile.drain

  def pipeTo[F[_]: ConcurrentEffect: Timer: ContextShift](otherKit: KafkaTopicKit[K, V])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    fromKafka[F].flatMap(_.toProducerRecords.upload(otherKit).map(_ => print(".")).compile.drain)

  def sparkStreaming[F[_]](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmSparkStreaming[F, NJConsumerRecord[K, V]] =
    new FsmSparkStreaming[F, NJConsumerRecord[K, V]](sk.streaming(kit).dataset, params)
}
