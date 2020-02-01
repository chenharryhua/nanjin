package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, NJProducerRecord}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

trait FsmSparKafka[K, V] extends Serializable with UpdateParams[KitBundle[K, V], FsmSparKafka[K, V]]

final class FsmStart[K, V](bundle: KitBundle[K, V])(implicit sparkSession: SparkSession)
    extends FsmSparKafka[K, V] {

  override def withParamUpdate(f: KitBundle[K, V] => KitBundle[K, V]): FsmStart[K, V] =
    new FsmStart[K, V](f(bundle))

  def fromKafka[F[_]: Sync, A](f: NJConsumerRecord[K, V] => A)(
    implicit ev: TypedEncoder[A]): F[TypedDataset[A]] =
    sk.fromKafka[F, K, V, A](bundle.kit, bundle.params.timeRange, bundle.params.locationStrategy)(f)

  def fromKafka[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[FsmConsumerRecords[F, K, V]] =
    fromKafka[F, NJConsumerRecord[K, V]](identity).map(crDataset)

  def fromDisk[F[_]](path: String)(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] =
    crDataset[F](sk.fromDisk(bundle.kit, bundle.params.timeRange, bundle.params.fileFormat, path))

  def fromDisk[F[_]](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] =
    fromDisk[F](bundle.getPath)

  def crDataset[F[_]](cr: TypedDataset[NJConsumerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](cr.dataset, bundle)

  def prDataset[F[_]](pr: TypedDataset[NJProducerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](pr.dataset, bundle)

  def replay[F[_]: ConcurrentEffect: Timer: ContextShift](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    fromDisk[F].toProducerRecords.upload.map(_ => print(".")).compile.drain

  def pipeTo[F[_]: ConcurrentEffect: Timer: ContextShift](otherTopic: KafkaTopicKit[K, V])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    fromKafka[F].flatMap(_.toProducerRecords.upload(otherTopic).map(_ => print(".")).compile.drain)

  def streaming[F[_]](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): SparkStreaming[F, NJConsumerRecord[K, V]] =
    new SparkStreaming[F, NJConsumerRecord[K, V]](sk.streaming(bundle.kit).dataset)
}
