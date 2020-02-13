package com.github.chenharryhua.nanjin.spark.kafka

import java.time.{LocalDateTime, ZoneId}

import cats.data.Reader
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.streaming.{SparkStreamStart, StreamParams}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.github.chenharryhua.nanjin.common.NJFileFormat

final class FsmStart[K, V](kit: KafkaTopicKit[K, V], params: ConfigParamF.ConfigParam)(
  implicit sparkSession: SparkSession)
    extends Serializable {

  // config section
  def withStartTime(dt: LocalDateTime): FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withStartTime(dt, params))

  def withEndTime(dt: LocalDateTime): FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withEndTime(dt, params))

  def withZoneId(zoneId: ZoneId): FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withZoneId(zoneId, params))

  def withJson: FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withFileFormat(NJFileFormat.Json, params))

  def withJackson: FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withFileFormat(NJFileFormat.Jackson, params))

  def withAvro: FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withFileFormat(NJFileFormat.Avro, params))

  def withParquet: FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withFileFormat(NJFileFormat.Parquet, params))

  def withPathBuilder(f: NJPathBuild => String): FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withPathBuilder(Reader(f), params))

  def withOverwrite: FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withSaveMode(SaveMode.Overwrite, params))

  def withShowRows(num: Int): FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withShowRows(num, params))

  def withShowTruncate(truncate: Boolean): FsmStart[K, V] =
    new FsmStart[K, V](kit, ConfigParamF.withShowTruncate(truncate, params))

  private val p: SKParams = ConfigParamF.evalParams(params)

  //api section
  def fromKafka[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmKafkaUnload[F, K, V] =
    new FsmKafkaUnload[F, K, V](kit, params)

  def save[F[_]: Sync](path: String)(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    fromKafka[F].consumerRecords.map(_.withSaveMode(SaveMode.Overwrite).save(path))

  def save[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    save(p.pathBuilder(NJPathBuild(p.fileFormat, kit.topicName)))

  def fromDisk[F[_]](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmDiskLoad[F, K, V] =
    new FsmDiskLoad[F, K, V](kit, params)

  def crDataset[F[_]](tds: TypedDataset[NJConsumerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]) =
    new FsmConsumerRecords[F, K, V](tds.dataset, kit, params)

  def prDataset[F[_]](tds: TypedDataset[NJProducerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]) =
    new FsmProducerRecords[F, K, V](tds.dataset, kit, params)

  def replay[F[_]: ConcurrentEffect: Timer: ContextShift](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    fromDisk[F].consumerRecords
      .flatMap(_.someValues.toProducerRecords.noMeta.upload.map(_ => print(".")).compile.drain)

  def pipeTo[F[_]: ConcurrentEffect: Timer: ContextShift](otherTopic: KafkaTopicKit[K, V])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    fromKafka[F].consumerRecords.flatMap(
      _.someValues.toProducerRecords.noMeta.upload(otherTopic).map(_ => print(".")).compile.drain)

  def streaming[F[_]: Sync, A](f: NJConsumerRecord[K, V] => A)(
    implicit
    encoder: TypedEncoder[A]): F[SparkStreamStart[F, A]] =
    sk.streaming[F, K, V, A](kit, p.timeRange)(f)
      .map(s =>
        new SparkStreamStart(
          s.dataset,
          StreamParams(s"./data/checkpoint/kafka/${kit.topicName.value}")))

  def streaming[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[SparkStreamStart[F, NJConsumerRecord[K, V]]] =
    streaming[F, NJConsumerRecord[K, V]](identity)
}
